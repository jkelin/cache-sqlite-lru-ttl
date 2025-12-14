import { randomBytes, randomUUID } from "node:crypto";
import { unlink } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { expect, test } from "vitest";
import SqliteCache from ".";

// Helper function to close multiple caches and optionally clean up database file
async function cleanupCaches(
  caches: SqliteCache[],
  dbPath?: string
): Promise<void> {
  await Promise.all(caches.map((cache) => cache.close()));
  if (dbPath) {
    await new Promise((resolve) => setTimeout(resolve, 10));
    try {
      await unlink(dbPath);
    } catch (err: any) {
      if (err.code !== "EBUSY") {
        throw err;
      }
    }
  }
}

test("memory get set", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");
  } finally {
    await cleanupCaches([cache]);
  }
});

test("missing key", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("bar")).toBeUndefined();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("deletion", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    await cache.delete("foo");
    expect(await cache.get("foo")).toBeUndefined();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("clear", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    await cache.clear();
    expect(await cache.get("foo")).toBeUndefined();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("number", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", 5);
    expect(await cache.get("foo")).toBe(5);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("bool", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", true);
    expect(await cache.get("foo")).toBe(true);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("null", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", null);
    expect(await cache.get("foo")).toBeNull();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("undefined", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", undefined);
    expect(await cache.get("foo")).toBeUndefined();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("array", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", [1, 2, 3]);
    expect(await cache.get("foo")).toEqual([1, 2, 3]);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("object", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", { bar: "baz" });
    expect(await cache.get("foo")).toEqual({ bar: "baz" });
  } finally {
    await cleanupCaches([cache]);
  }
});

test("buffer", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  const buf = Buffer.from("hello world");

  try {
    await cache.set("foo", buf);
    expect(await cache.get("foo")).toEqual(buf);

    await cache.set("foo", Buffer.from("hello world 2"));
    expect(await cache.get("foo")).not.toEqual(buf);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("complex object", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  const obj = {
    foo: "bar",
    a: {
      b: {
        buf: Buffer.from("hello world"),
      },
      x: 1,
      y: 123.456,
      bool: true,
      date: new Date(),
    },
  };

  try {
    await cache.set("foo", obj);
    expect(await cache.get("foo")).toEqual(obj);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("update key", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");

    await cache.set("foo", "baz");
    expect(await cache.get("foo")).toBe("baz");
  } finally {
    await cleanupCaches([cache]);
  }
});

test("file get set", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");
  } finally {
    await cleanupCaches([cache], dbPath);
  }
});

test("file close reopen", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  let cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");

    await cache.close();
    await new Promise((resolve) => setTimeout(resolve, 10));

    cache = new SqliteCache({
      database: dbPath,
    });

    expect(await cache.get("foo")).toBe("bar");
  } finally {
    await cleanupCaches([cache], dbPath);
  }
});

test("ttl", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    await cache.set("expires", "bar", { ttlMs: 20 });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(await cache.get("foo")).toBe("bar");
    expect(await cache.get("expires")).toBeUndefined();
  } finally {
    await cache.close();
    await new Promise((resolve) => setTimeout(resolve, 10));
    try {
      await unlink(dbPath);
    } catch (err: any) {
      if (err.code !== "EBUSY") {
        throw err;
      }
    }
  }
});

test("lru", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    maxItems: 2,
  });

  try {
    await cache.set("foo", "bar");
    await cache.set("xyz", "bar");

    for (let i = 0; i < 10; i++) {
      await cache.set("foo" + i, "bar");
      await new Promise((resolve) => setTimeout(resolve, 10));
      await cache.get("xyz");
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(await cache.get("foo")).toBeUndefined();
    expect(await cache.get("xyz")).toBe("bar");
  } finally {
    await cleanupCaches([cache]);
  }
});

test("compression", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    compress: true,
  });

  const buffer = Buffer.alloc(10000, 0);

  try {
    await cache.set("buf", buffer);
    expect(await cache.get("buf")).toEqual(buffer);

    const dbResult = await (cache as any).db;
    const con = dbResult.db;
    const result = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get() as { value: Buffer; compressed: number } | undefined;

    expect(result).not.toBeUndefined();
    expect(result!.compressed).toBe(1);
    expect(result!.value.length).toBeLessThan(buffer.length);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("compression too short", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    compress: true,
  });

  const buffer = Buffer.alloc(200, 0);

  try {
    await cache.set("buf", buffer);
    expect(await cache.get("buf")).toEqual(buffer);

    const dbResult = await (cache as any).db;
    const con = dbResult.db;
    const result = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get() as { value: Buffer; compressed: number } | undefined;

    expect(result).not.toBeUndefined();
    expect(result!.compressed).toBe(0);
    expect(result!.value.length).toBeGreaterThanOrEqual(buffer.length);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("uncompressable", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    compress: true,
  });

  const buffer = randomBytes(10000);

  try {
    await cache.set("buf", buffer);
    expect(await cache.get("buf")).toEqual(buffer);

    const dbResult = await (cache as any).db;
    const con = dbResult.db;
    const result = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get() as { value: Buffer; compressed: number } | undefined;

    expect(result).not.toBeUndefined();
    expect(result!.compressed).toBe(0);
    expect(result!.value.length).toBeGreaterThanOrEqual(buffer.length);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("use after close", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");

    await cache.close();
    await new Promise((resolve) => setTimeout(resolve, 10));

    await expect(cache.get("foo")).rejects.toThrow();
    await expect(cache.set("foo", "bar")).rejects.toThrow();
    await expect(cache.delete("foo")).rejects.toThrow();
    await expect(cache.clear()).rejects.toThrow();
  } finally {
    // Cache is already closed in the test, just clean up the file
    await cleanupCaches([cache], dbPath);
  }
});

test("cacheTableName custom", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    cacheTableName: "my_cache",
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");

    const dbResult = await (cache as any).db;
    const con = dbResult.db;
    const tables = con
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='my_cache'"
      )
      .get();

    // better-sqlite3 returns undefined, bun:sqlite returns null for no results
    expect(tables != null).toBe(true);

    // Verify default table doesn't exist
    // better-sqlite3 returns undefined, bun:sqlite returns null for no results
    const defaultTable = con
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cache'"
      )
      .get();

    expect(defaultTable == null).toBe(true);
  } finally {
    await cleanupCaches([cache]);
  }
});

test("cacheTableName multiple caches same database", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache1 = new SqliteCache({
    database: dbPath,
    cacheTableName: "cache1",
  });

  const cache2 = new SqliteCache({
    database: dbPath,
    cacheTableName: "cache2",
  });

  try {
    await cache1.set("foo", "bar1");
    await cache2.set("foo", "bar2");

    expect(await cache1.get("foo")).toBe("bar1");
    expect(await cache2.get("foo")).toBe("bar2");

    // Verify they don't interfere
    await cache1.delete("foo");
    expect(await cache1.get("foo")).toBeUndefined();
    expect(await cache2.get("foo")).toBe("bar2");
  } finally {
    await cleanupCaches([cache1, cache2], dbPath);
  }
});

test("cacheTableName with double quotes", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    cacheTableName: 'table"with"quotes',
  });

  try {
    await cache.set("foo", "bar");
    expect(await cache.get("foo")).toBe("bar");

    const dbResult = await (cache as any).db;
    const con = dbResult.db;
    const tables = con
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
      .get('table"with"quotes');

    // better-sqlite3 returns undefined, bun:sqlite returns null for no results
    expect(tables != null).toBe(true);

    // Test all operations work with quotes in table name
    await cache.set("key1", "value1");
    expect(await cache.get("key1")).toBe("value1");

    await cache.delete("key1");
    expect(await cache.get("key1")).toBeUndefined();
  } finally {
    await cleanupCaches([cache]);
  }
});

test("lru cleanup should not clear table when items less than maxItems", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
    maxItems: 5,
  });

  try {
    // Add 3 items (less than maxItems = 5)
    await cache.set("item1", "value1");
    await cache.set("item2", "value2");
    await cache.set("item3", "value3");

    // Trigger cleanup by adding another item (which calls checkForExpiredItems)
    await cache.set("item4", "value4");

    // Wait for cleanup to complete (debounce is 100ms, plus some buffer)
    await new Promise((resolve) => setTimeout(resolve, 200));

    // All items should still be present since we have less than maxItems
    expect(await cache.get("item1")).toBe("value1");
    expect(await cache.get("item2")).toBe("value2");
    expect(await cache.get("item3")).toBe("value3");
    expect(await cache.get("item4")).toBe("value4");
  } finally {
    await cleanupCaches([cache]);
  }
});
