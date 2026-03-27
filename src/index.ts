import { gzip, gunzip } from "node:zlib";
import { promisify } from "node:util";
import cbor from "cbor";
import debounce from "debounce";
import z from "zod";

const COMPRESSION_MIN_LENGTH = 1024;

export interface SqliteCacheConfiguration {
  /**
   * Database file path or `:memory:` for in-memory database.
   */
  readonly database: string;

  /**
   * Default maximum time-to-live in milliseconds. Cache entries will be evicted after this time.
   * Can be overridden by `ttlMs` option in `set` method.
   * @default undefined - no ttl
   */
  readonly defaultTtlMs?: number;

  /**
   * Maximum number of items in the cache. Cache entries with oldest access time will be evicted after this number is reached.
   * @default undefined - no limit
   */
  readonly maxItems?: number;

  /**
   * Should we compress items on `set` with gzip. Old items will remain untouched so this flag can be switched at any time.
   * @default false
   */
  readonly compress?: boolean;

  /**
   * The name of the cache table in the database
   * @default "cache"
   */
  readonly cacheTableName?: string;
}

const configurationSchema = z.object({
  database: z.string(),
  defaultTtlMs: z.number().positive().optional(),
  maxItems: z.number().positive().optional(),
  compress: z.boolean().optional().default(false),
  cacheTableName: z.string().optional().default("cache"),
});

// Define types for statement parameters and results
// Buffer is compatible with Uint8Array for SQLite bindings
type SQLValue = string | number | bigint | boolean | Uint8Array | Buffer | null;

interface GetStatementParams extends Record<string, SQLValue> {
  key: string;
  now: number;
}

interface GetStatementResult {
  value: Buffer;
  compressed: number;
}

interface SetStatementParams extends Record<string, SQLValue> {
  key: string;
  value: Buffer;
  expires: number | null;
  now: number;
  compressed: number;
}

interface DeleteStatementParams extends Record<string, SQLValue> {
  key: string;
}

interface CleanupExpiredStatementParams extends Record<string, SQLValue> {
  now: number;
}

interface CleanupLruStatementParams extends Record<string, SQLValue> {
  maxItems: number;
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

type DatabaseType = "bun" | "better-sqlite3";

async function getDatabase(): Promise<{ Database: any; type: DatabaseType }> {
  try {
    const { default: Database } = await import("bun:sqlite");
    return { Database, type: "bun" };
  } catch (error) {
    const Database = (await import("better-sqlite3")).default;
    return { Database, type: "better-sqlite3" };
  }
}

async function initSqliteCache(configuration: SqliteCacheConfiguration) {
  const { Database, type } = await getDatabase();

  // better-sqlite3 doesn't support strict mode option
  const db =
    type === "bun"
      ? new Database(configuration.database, { strict: true })
      : new Database(configuration.database);

  // Enable WAL mode for file-based databases. Without WAL, every write
  // (including cache.get() which updates lastAccess) triggers two fsyncs via
  // the rollback journal. Under concurrent load from multiple processes this
  // serialises on the write lock — each waiter blocks its Node.js event loop
  // for the full busy-timeout duration, producing 200-500 ms stalls.
  // WAL + synchronous=NORMAL writes append to the WAL file with no per-write
  // fsync, reducing individual write latency by ~10-15× and eliminating the
  // contention-cascade that causes those stalls.
  if (configuration.database !== ":memory:") {
    db.exec("PRAGMA journal_mode=WAL");
    db.exec("PRAGMA synchronous=NORMAL");
  }

  const cacheTableName = configuration.cacheTableName ?? "cache";
  const escapedTableName = escapeIdentifier(cacheTableName);

  // Create table and indexes
  db.exec(`CREATE TABLE IF NOT EXISTS ${escapedTableName} (
    key TEXT PRIMARY KEY,
    value BLOB,
    expires INT,
    lastAccess INT,
    compressed BOOLEAN
  )`);

  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS key ON ${escapedTableName} (key)`);
  db.exec(
    `CREATE INDEX IF NOT EXISTS expires ON ${escapedTableName} (expires)`
  );
  db.exec(
    `CREATE INDEX IF NOT EXISTS lastAccess ON ${escapedTableName} (lastAccess)`
  );

  return {
    db,
    dbType: type,
    getStatement: db.prepare(
      `UPDATE OR IGNORE ${escapedTableName}
      SET lastAccess = @now
      WHERE key = @key AND (expires > @now OR expires IS NULL)
      RETURNING value, compressed`
    ) as any as {
      get(params: GetStatementParams): GetStatementResult | undefined;
    },
    setStatement: db.prepare(
      `INSERT OR REPLACE INTO ${escapedTableName}
      (key, value, expires, lastAccess, compressed) VALUES (@key, @value, @expires, @now, @compressed)`
    ) as any as {
      run(params: SetStatementParams): void;
    },
    deleteStatement: db.prepare(
      `DELETE FROM ${escapedTableName} WHERE key = @key`
    ) as any as {
      run(params: DeleteStatementParams): void;
    },
    clearStatement: db.prepare(`DELETE FROM ${escapedTableName}`) as any as {
      run(params?: Record<string, never>): void;
    },
    cleanupExpiredStatement: db.prepare(
      `DELETE FROM ${escapedTableName} WHERE expires < @now`
    ) as any as {
      run(params: CleanupExpiredStatementParams): void;
    },
    cleanupLruStatement: db.prepare(
      `DELETE FROM ${escapedTableName}
      WHERE key IN (
        SELECT key FROM ${escapedTableName}
        ORDER BY lastAccess ASC
        LIMIT MAX(0, (SELECT COUNT(*) - @maxItems FROM ${escapedTableName}))
      )`
    ) as any as {
      run(params: CleanupLruStatementParams): void;
    },
  };
}

function now() {
  return Date.now();
}

// Use Node.js zlib APIs (Bun supports these natively)
const compress = promisify(gzip) as (buffer: Buffer) => Promise<Buffer>;
const decompress = promisify(gunzip) as (buffer: Buffer) => Promise<Buffer>;

export class SqliteCache<TData = unknown> {
  private readonly db: ReturnType<typeof initSqliteCache>;
  private readonly checkInterval: Timer;
  private isClosed: boolean = false;
  private pendingOperations: number = 0;
  private pendingDrainResolvers: Set<() => void> = new Set();

  constructor(private readonly configuration: SqliteCacheConfiguration) {
    const config = configurationSchema.parse(configuration);
    this.db = initSqliteCache(config);
    this.checkInterval = setInterval(this.checkForExpiredItems, 1000);
  }

  /**
   * Get a cache item by its key.
   */
  public async get<T = TData>(key: string): Promise<T | undefined> {
    return this.runTrackedOperation(async () => {
      if (this.isClosed) {
        throw new Error("Cache is closed");
      }

      const res = (await this.db).getStatement.get({
        key,
        now: now(),
      });

      if (!res) {
        return undefined;
      }

      let value: Buffer = res.value;

      if (res.compressed) {
        value = await decompress(value);
      }

      return cbor.decode(value);
    });
  }

  /**
   * Update a cache item by key, or create one if it does not exist.
   */
  public async set<T = TData>(
    key: string,
    value: T,
    opts: { ttlMs?: number; compress?: boolean } = {}
  ) {
    await this.runTrackedOperation(async () => {
      if (this.isClosed) {
        throw new Error("Cache is closed");
      }

      const ttl = opts.ttlMs ?? this.configuration.defaultTtlMs;
      const expires = ttl !== undefined ? new Date(Date.now() + ttl) : undefined;

      let compression = opts.compress ?? this.configuration.compress ?? false;

      let valueBuffer = cbor.encode(value);

      if (compression && valueBuffer.length >= COMPRESSION_MIN_LENGTH) {
        const compressed = await compress(valueBuffer);
        if (compressed.length >= valueBuffer.length) {
          compression = false;
        } else {
          valueBuffer = compressed;
        }
      } else {
        compression = false;
      }

      (await this.db).setStatement.run({
        key,
        value: valueBuffer,
        expires: expires?.getTime() ?? null,
        compressed: compression ? 1 : 0,
        now: now(),
      });
    });
    setImmediate(this.checkForExpiredItems.bind(this));
  }

  /**
   * Remove specific item from the cache.
   */
  public async delete(key: string) {
    await this.runTrackedOperation(async () => {
      if (this.isClosed) {
        throw new Error("Cache is closed");
      }

      (await this.db).deleteStatement.run({ key });
    });
  }

  /**
   * Remove all items from the cache.
   */
  public async clear() {
    await this.runTrackedOperation(async () => {
      if (this.isClosed) {
        throw new Error("Cache is closed");
      }

      (await this.db).clearStatement.run({});
    });
  }

  /**
   * Close the database and clean up resources.
   */
  public async close() {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    clearInterval(this.checkInterval);
    // Wait for in-flight reads/writes/maintenance to finish before strict close.
    await this.waitForPendingOperations();
    await this.finalizePreparedStatements();
    (await this.db).db.close();
  }

  private checkForExpiredItems = debounce(
    async () => {
      await this.runTrackedOperation(async () => {
        if (this.isClosed) {
          return;
        }

        try {
          const db = await this.db;
          db.cleanupExpiredStatement.run({ now: now() });

          if (this.configuration.maxItems) {
            db.cleanupLruStatement.run({
              maxItems: this.configuration.maxItems,
            });
          }
        } catch (ex) {
          console.error(
            "Error in cache-sqlite-lru-ttl when checking for expired items",
            ex
          );
        }
      });
    },
    100,
    { immediate: true }
  );

  private async runTrackedOperation<T>(operation: () => Promise<T>): Promise<T> {
    this.pendingOperations += 1;
    try {
      return await operation();
    } finally {
      this.pendingOperations -= 1;
      if (this.pendingOperations === 0) {
        for (const resolve of this.pendingDrainResolvers) {
          resolve();
        }
        this.pendingDrainResolvers.clear();
      }
    }
  }

  private async waitForPendingOperations(): Promise<void> {
    if (this.pendingOperations === 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      this.pendingDrainResolvers.add(resolve);
    });
  }

  private async finalizePreparedStatements(): Promise<void> {
    const dbState = await this.db;
    const statements = [
      dbState.getStatement,
      dbState.setStatement,
      dbState.deleteStatement,
      dbState.clearStatement,
      dbState.cleanupExpiredStatement,
      dbState.cleanupLruStatement,
    ];

    // Bun requires statements to be finalized before strict close(true).
    for (const statement of statements) {
      const maybeFinalize = (statement as { finalize?: () => void }).finalize;
      if (typeof maybeFinalize === "function") {
        maybeFinalize.call(statement);
      }
    }
  }
}

export default SqliteCache;
