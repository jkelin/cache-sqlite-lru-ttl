import { randomBytes, randomUUID } from "node:crypto";
import type Database from "better-sqlite3";
import { unlink } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { assert, expect, test } from "vitest";
import SqliteCache from ".";

test("memory get set", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("foo"), "bar");
  } finally {
    await cache.close();
  }
});

test("missing key", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("bar"), undefined);
  } finally {
    await cache.close();
  }
});

test("deletion", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    await cache.delete("foo");
    assert.equal(await cache.get("foo"), undefined);
  } finally {
    await cache.close();
  }
});

test("clear", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    await cache.clear();
    assert.equal(await cache.get("foo"), undefined);
  } finally {
    await cache.close();
  }
});

test("number", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", 5);
    assert.deepEqual(await cache.get("foo"), 5);
  } finally {
    await cache.close();
  }
});

test("bool", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", true);
    assert.deepEqual(await cache.get("foo"), true);
  } finally {
    await cache.close();
  }
});

test("null", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", null);
    assert.deepEqual(await cache.get("foo"), null);
  } finally {
    await cache.close();
  }
});

test("undefined", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", undefined);
    assert.deepEqual(await cache.get("foo"), undefined);
  } finally {
    await cache.close();
  }
});

test("array", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", [1, 2, 3]);
    assert.deepEqual(await cache.get("foo"), [1, 2, 3]);
  } finally {
    await cache.close();
  }
});

test("object", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", { bar: "baz" });
    assert.deepEqual(await cache.get("foo"), { bar: "baz" });
  } finally {
    await cache.close();
  }
});

test("buffer", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  const buf = Buffer.from("hello world");

  try {
    await cache.set("foo", buf);
    assert.deepEqual(await cache.get("foo"), buf);

    await cache.set("foo", Buffer.from("hello world 2"));
    assert.notDeepEqual(await cache.get("foo"), buf);
  } finally {
    await cache.close();
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
    assert.deepEqual(await cache.get("foo"), obj);
  } finally {
    await cache.close();
  }
});

test("update key", async () => {
  const cache = new SqliteCache({
    database: ":memory:",
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("foo"), "bar");

    await cache.set("foo", "baz");
    assert.equal(await cache.get("foo"), "baz");
  } finally {
    await cache.close();
  }
});

test("file get set", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("foo"), "bar");
  } finally {
    await cache.close();
    await unlink(dbPath);
  }
});

test("file close reopen", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  let cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("foo"), "bar");

    await cache.close();

    cache = new SqliteCache({
      database: dbPath,
    });

    assert.equal(await cache.get("foo"), "bar");
  } finally {
    await cache.close();
    await unlink(dbPath);
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
    assert.equal(await cache.get("foo"), "bar");
    assert.equal(await cache.get("expires"), undefined);
  } finally {
    await cache.close();
    await unlink(dbPath);
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
    assert.equal(await cache.get("foo"), undefined);
    assert.equal(await cache.get("xyz"), "bar");
  } finally {
    await cache.close();
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
    assert.deepEqual(await cache.get("buf"), buffer);

    const con: Database.Database = (await (cache as any).db).db;
    const { value, compressed } = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get();

    assert.equal(compressed, 1);
    expect(value.length).lessThan(buffer.length);
  } finally {
    await cache.close();
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
    assert.deepEqual(await cache.get("buf"), buffer);

    const con: Database.Database = (await (cache as any).db).db;
    const { value, compressed } = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get();

    assert.equal(compressed, 0);
    expect(value.length).gte(buffer.length);
  } finally {
    await cache.close();
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
    assert.deepEqual(await cache.get("buf"), buffer);

    const con: Database.Database = (await (cache as any).db).db;
    const { value, compressed } = con
      .prepare("SELECT value, compressed FROM cache LIMIT 1")
      .get();

    assert.equal(compressed, 0);
    expect(value.length).gte(buffer.length);
  } finally {
    await cache.close();
  }
});

test("use after close", async () => {
  const dbPath = join(tmpdir(), randomUUID() + ".db");
  const cache = new SqliteCache({
    database: dbPath,
  });

  try {
    await cache.set("foo", "bar");
    assert.equal(await cache.get("foo"), "bar");

    await cache.close();

    await expect(cache.get("foo")).rejects.toThrow();
    await expect(cache.set("foo", "bar")).rejects.toThrow();
    await expect(cache.delete("foo")).rejects.toThrow();
    await expect(cache.clear()).rejects.toThrow();
  } finally {
    await unlink(dbPath);
  }
});
