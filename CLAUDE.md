# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sciqlop-cache is a C++20 caching library with Python bindings. It uses SQLite for metadata and a hybrid storage strategy: values ≤ 8KB stored as BLOBs in SQLite, larger values stored as files on disk. Thread-safe via per-instance database connections and WAL mode.

## Build Commands

```bash
# Configure with tests
meson setup build -Dwith_tests=true

# Build
meson compile -C build

# Run all tests
meson test -C build

# Run a single test suite
meson test -C build sciqlop-cache:basic

# Available test suites:
#   C++ (always-on):  sciqlop-cache:basic, sciqlop-cache:basic_index,
#                     sciqlop-cache:database, sciqlop-cache:intermediate,
#                     sciqlop-cache:multithreads, sciqlop-cache:fanout,
#                     sciqlop-cache:check, sciqlop-cache:checkpoint
#   C++ (with_torture_tests=true):
#                     sciqlop-cache:torture, sciqlop-cache:concurrency_bugs
#   Python (always-on if Python wrapper enabled):
#                     sciqlop-cache:test_python_interface,
#                     sciqlop-cache:test_serializers,
#                     sciqlop-cache:test_python_multiprocess,
#                     sciqlop-cache:test_perf_vs_diskcache,
#                     sciqlop-cache:test_concurrency_bugs
#   Python (with_torture_tests=true):
#                     sciqlop-cache:test_torture, sciqlop-cache:test_hypothesis

# Build Python wheel
pip install meson-python numpy && python -m build --wheel

# Run the bulk-insert benchmark (bg checkpoint/eviction stall + WAL size
# under sustained writes; not run by `meson test` by default)
meson test -C build --benchmark bgscan
```

### Meson Options

- `with_tests` (false) — build C++ tests
- `with_benchmarks` (false) — build benchmarks
- `disable_python_wrapper` (false) — skip Python bindings
- `tracy_enable` (false) — enable Tracy profiling

## Architecture

**Core layer** (`include/sciqlop_cache/`):

- `store.hpp` — `_Store<Storage, Policies...>` template class, the main engine. Uses zero-cost policy-based design: policies are inherited as mixins, SQL and behavior composed via `if constexpr` + fold expressions. No virtual dispatch.
- `policies.hpp` — Policy structs: `WithExpiration`, `WithEviction`, `WithTags`, `WithStats`. Each contributes schema columns, WHERE clause fragments, and runtime hooks. `has_policy_v` trait for compile-time branching.
- `sciqlop_cache.hpp` — Type aliases: `Cache = _Store<DiskStorage, WithExpiration, WithEviction, WithTags, WithStats>`, `Index = _Store<DiskStorage>`, `FanoutCache = FanoutStore<Cache>`, `FanoutIndex = FanoutStore<Index>`.
- `fanout_store.hpp` — `FanoutStore<StoreType>` template. Shards keys across N independent `_Store` instances via `hash(key) % shard_count` for write concurrency. Per-key ops dispatch to one shard; cross-shard ops aggregate.
- `database.hpp` — SQLite wrapper: `Database`, `CompiledStatement`, `BindedCompiledStatement`, `Transaction`. Uses SQLITE_NOMUTEX with manual transaction control. `~Transaction()` rolls back if `commit()` was never called (RAII rollback-by-default).
- `disk_storage.hpp` — `DiskStorage` class. UUID-based two-level directory hierarchy for file storage.
- `utils/concepts.hpp` — C++20 concepts: `DurationConcept`, `TimePoint`, `Bytes`
- `utils/buffer.hpp` — Polymorphic buffer with memory-mapped file support
- `utils/time.hpp` — Epoch/TimePoint conversions for SQLite

**Python bindings** (`pysciqlop_cache/`): nanobind-based, exposes `Cache`, `Index`, `FanoutCache`, and `FanoutIndex` with dict-like interface and `Buffer` with `.memoryview()`.

**Tests** (`tests/`): Catch2 BDD-style. Seven always-on C++ suites (basic, basic_index, database, intermediate, multithreads, fanout, check) plus Python tests. Two more suites (`torture`, `concurrency_bugs`) plus the Python `test_torture` and `test_hypothesis` are gated by `-Dwith_torture_tests=true` (designed for sanitizer / long-running runs).

## Dependencies

All vendored in `subprojects/`: SQLite amalgamation, fmt, nanobind, Catch2, stduuid, cpp_utils, tracy, robin-map, hedley.

## Key Design Decisions

- **Policy-based Store** — `_Store<Storage, Policies...>` composes features at compile time. `Cache` has expiration, LRU eviction, tags, and stats. `Index` is a bare key-value store with no overhead.
- **FanoutStore** — `FanoutStore<StoreType>` shards keys across N independent stores (default 8) for write concurrency. `max_size` is per shard. `transact(key)` scoped to one shard. No cross-shard transactions.
- Per-instance `Database` + `std::recursive_mutex` for thread safety
- WAL mode + 600s busy_timeout for multi-process safety
- **`_NestedTxn` (private RAII helper in `_Store`)** — every internal write path (`_set_impl`, `del`, `pop`, `incr`, `evict_tag`) wraps in a `BEGIN EXCLUSIVE` only at the outermost level (depth-counted via `_txn_depth`). Inner levels are no-ops. Both for cross-process atomicity (read-modify-write inside one txn) and to compose cleanly inside a user `transact()`.
- **`TransactionGuard` is reentrant on the same thread** (depth-counted same as `_NestedTxn`). Nested `with cache.transact():` is supported; outer rollback discards inner work (no real SAVEPOINTs — same semantics as diskcache).
- **Counters live in the DB, not memory** — `size()`/`count()`/`volume()` are `meta` rows (`size`, `count`, `file_size`) kept exactly by incremental (`value + NEW.size`, O(1)) INSERT/UPDATE/DELETE triggers on `cache`, in the same transaction as the row change. Cross-process exact by construction, so there's no resync step and the bg thread never needs `_mtx` for counters. `count()` always reads the maintained `meta` row (O(1)) — it no longer runs a WHERE-filtered `COUNT(*)` for types with expiration, so it may briefly include an entry that's expired but not yet evicted by the background thread (same semantics as diskcache's `len()`). `volume()` is `page_count*page_size` + the `file_size` meta row (sum of `size` for rows with `path IS NOT NULL`) — O(1) instead of walking the on-disk tree, and it only counts live values (an orphaned file contributes nothing; `check()` reports those separately). `size()`/`count()`/`volume()` are single-row queries taken under the store mutex (they block behind a long `transact()`); `clear()` is O(N) because triggers disable SQLite's truncate optimisation.
- **BG checkpoint thread never takes `_mtx`** — it only runs a PASSIVE `wal_checkpoint` + eviction (`_bg_evict`) each tick. The bg connection uses a 250 ms `sqlite3_busy_timeout` (it has none by default) and only removes a value's file after its row is confirmed deleted, so a DELETE that loses to the writer's own BEGIN EXCLUSIVE never orphans the file. The WAL is bounded by `PRAGMA wal_autocheckpoint=4000` on the writer connection instead: the bg thread's PASSIVE checkpoint alone can't reset the WAL under a continuous writer (checkpoint starvation), but the writer's own autocheckpoint finishes the small remainder once the WAL exceeds ~16 MB, keeping it flat instead of growing unbounded.
- **Never open a live cache DB with a second SQLite library in the same process** (e.g. Python's `sqlite3`) — per-process `fcntl` locks let the second library's connection truncate the `-shm` file under our own mmap, causing SIGBUS. Tests must `del cache` (closing our connection) before calling `sqlite3.connect()` on the same path.
- Expiration handled at query time via SQL (`WHERE expire IS NULL OR expire > unixepoch('now')`) — only present in types with `WithExpiration`
- No-expiry default: `set()`/`add()` without expire stores NULL (never expires)
- LRU eviction: `max_size` in bytes (0 = unlimited, default). Background thread evicts using monotonic access counter — only present with `WithEviction`
- Tags: optional `tag` parameter on `set()`/`add()`, indexed for fast `evict_tag()` bulk removal — only present with `WithTags`
- C++20 concepts enforce type safety at compile time
- `requires` clauses on methods ensure policy-specific API (e.g. `touch()`, `evict()`) is only available on types that support it
