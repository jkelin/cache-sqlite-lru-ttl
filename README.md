# SQLite cache with LRU and TTL eviction

![coverage](https://img.shields.io/badge/coverage-97%25-green)
![NPM](https://img.shields.io/npm/l/cache-sqlite-lru-ttl)
[![npm version](https://badge.fury.io/js/cache-sqlite-lru-ttl.svg)](https://www.npmjs.com/package/cache-sqlite-lru-ttl)

SQLite is perfect for high-performance local cache. It is perfectly viable for caching strings or files. Caching files in SQLite is generally faster than storing them in a filesystem. This library tries to have sane defaults and features:

- TTL eviction mechanism which allows you to set maximum datetime to expire an item
- LRU eviction mechanism that enforces that no more than `maxItems` will be cached based on least recent `get`
- Optional value compression with gzip
- Values encoded with [CBOR](https://cbor.io/) which is like JSON, but is binary and serializes `Buffer` and `Date`
- Inspired by [node-cache-manager-sqlite](https://github.com/maxpert/node-cache-manager-sqlite) but with easier configuration, TypeScript, LRU and compression
- Make sure to call `await cache.close()` during graceful shutdown of your application to ensure SQLite is properly persisted to disk

### Basic usage

```typescript
  import SqliteCache from `cache-sqlite-lru-ttl`

  const cache = new SqliteCache({
    database: ':memory:', // or path to your database on disk
    defaultTtlMs: 1000 * 60 * 60, // optional TTL in milliseconds
    maxItems: 1000, // optional LRU
    compress: true, // use gzip for values > 1024 bytes, can be smaller, but slower
  });

  await cache.set('bar', 'baz')

  await cache.get('bar') // 'baz' 🎉
```

### More usage

```typescript
  import SqliteCache from `cache-sqlite-lru-ttl`

  const cache = new SqliteCache({
    database: './cache.db',
  });

  await cache.set('bar', 'baz', {
    ttlMs: 60 * 1000, // short LRU for this item
    compress: false, // disable compression for this item
  })

  await cache.get('bar') // 'baz' 🎉

  await cache.delete('bar') // delete 'bar'

  await cache.clear() // delete everything

  await cache.close() // close the database once you are done using it (usually during graceful shutdown of your application server)
```
