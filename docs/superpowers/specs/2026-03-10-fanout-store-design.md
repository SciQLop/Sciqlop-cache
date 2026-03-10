# FanoutStore Design

## Problem

SQLite WAL mode allows one writer at a time per database. For workloads with many concurrent writers (threads or processes), a single database becomes a bottleneck. Sharding across N databases gives up to N concurrent writers, and also keeps per-DB item counts lower for faster queries.

## Design

### Core

`FanoutStore<StoreType>` is a template wrapper that owns N shards of any `_Store` variant. Keys are routed via `std::hash<std::string>(key) % shard_count`.

```cpp
template <typename StoreType>
class FanoutStore {
    std::vector<std::unique_ptr<StoreType>> _shards;
    std::size_t _shard_count;

    std::size_t _shard_for(const std::string& key) const {
        return std::hash<std::string>{}(key) % _shard_count;
    }

public:
    explicit FanoutStore(const std::filesystem::path& path,
                         std::size_t shard_count = 8,
                         std::size_t max_size = 0);
};
```

### Type aliases

```cpp
using FanoutCache = FanoutStore<Cache>;
using FanoutIndex = FanoutStore<Index>;
```

### Directory layout

```
base_path/00/  ->  shard 0
base_path/01/  ->  shard 1
...
base_path/07/  ->  shard 7
```

Zero-padded two-digit shard directories.

### Per-key operations (dispatch to single shard)

`set`, `get`, `add`, `del`, `pop`, `exists`, `touch`, `incr`, `decr`, `set_meta`, `get_meta` -- all forward to `_shards[_shard_for(key)]`.

### Aggregated operations (iterate all shards)

- `count()` -- sum across shards
- `size()` -- sum across shards
- `keys()` -- concatenate all shard keys
- `clear()` -- call on each shard
- `evict()` -- call on each shard, sum results
- `expire()` -- call on each shard
- `evict_tag(tag)` -- call on each shard, sum results
- `stats()` -- sum hits/misses across shards
- `reset_stats()` -- call on each shard
- `check()` -- AND of all shard results

### transact()

Scoped to a single shard: `transact(key)` returns a `TransactionGuard` from the shard owning that key. No cross-shard transactions.

### max_size semantics

`max_size` is per shard. Total capacity = `max_size * shard_count`. Matches diskcache behavior.

### Python bindings

Expose `FanoutCache` and `FanoutIndex` with the same dict-like interface as `Cache`/`Index`.

### Non-goals

- Cross-shard transactions
- Rebalancing on shard count change (new count = fresh cache)
- Ordered iteration guarantees
