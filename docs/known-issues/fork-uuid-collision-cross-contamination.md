# Fork inherits the UUID generator state → deterministic file-path collisions (cross-contamination)

**Status:** resolved — root cause confirmed by code review (2026-09-02),
reproduced by a dedicated regression test
(`tests/fork_safety/main.cpp`: "forked children storing file-backed values
never share an on-disk path", which fails pre-fix with one key reading back
the other key's bytes), and fixed in `DiskStorage`/`_Store`:

1. Full-state seeding via `std::seed_seq` (replaces single-32-bit-word seed).
2. `DiskStorage::reseed()` called from `_Store::_fork_child()` (breaks the
   lockstep-identical-sequence root cause).
3. `getpid()` embedded in the blob filename (structural uniqueness that does
   not depend on the atfork hook firing).
4. Exclusive-create (`O_CREAT|O_EXCL`) in `store()` with a collision-healing
   path: on EEXIST, an `is_referenced` callback (SQL `IS_PATH_REFERENCED_SQL`:
   live cache row **or pending trash-table entry** owns the path → regenerate;
   orphaned leftover → overwrite and reuse). Covering trash matters: a
   displaced path spends 5 s pending deferred deletion, and classifying it as
   orphaned would let the heal reuse a file that `_drain_trash` then unlinks.
   The happy path is a single exclusive create with no DB query.

## TL;DR

The hash mechanisms are sound — keys are stored verbatim in SQLite
(`key TEXT PRIMARY KEY`, exact-match lookups; memoize keys are full 256-bit
SHA-256). The aliasing vector is the **on-disk filename generator**: after
`fork()`, every child process inherits the *identical* mt19937 state used to
generate UUID filenames, so forked workers storing file-backed (>8 KB) values
write to the *same file paths* — different keys, same file, last writer wins.

## Mechanism

1. File-backed values (>8 KB threshold) are stored under a random UUID
   filename: `DiskStorage::store()` → `generate_random_filename()`
   (`include/sciqlop_cache/disk_storage.hpp:209-217`, `:141-144`).
2. The UUID comes from `std::mt19937 gen` seeded **once** in the constructor
   via `gen(rd())` (`disk_storage.hpp:20-22`, `:102`);
   `uuids::uuid_random_generator` holds a *pointer* to that engine
   (stduuid `uuid.h:744-747`).
3. `_Store::_fork_child()` (`include/sciqlop_cache/store.hpp:470-479`)
   rebuilds the mutex, the SQLite connection and the checkpoint thread after
   fork — **but never reseeds `storage`'s PRNG**. Nothing anywhere reseeds it.
4. Classic pattern: create `Cache` in the parent, then fork a
   `multiprocessing.Pool` whose workers use the inherited cache. Every worker
   inherits the identical mt19937 state at the identical sequence position →
   every worker's 1st, 2nd, … file-backed `store()` generates the **same
   UUID** → same path `ab/cd/<uuid>`.
5. `store()` writes with in-place truncation — **no existence check, no
   retry** (`disk_storage.hpp:83`, `:209-217`).
6. Result: worker A's value for key `K1` and worker B's value for key `K2`
   land in the same file. Both DB rows (different keys) point at it; last
   writer wins → **`K1` returns `K2`'s bytes: cross-contamination.**
   Knock-on damage: eviction/expiry of one row unlinks the file under the
   other row (dangling → silent data loss via the `get()` fallback), and mmap
   readers of the shared inode see torn content mid-rewrite.

This is deterministic, not probabilistic, for any fork-based worker pool
using an inherited cache with >8 KB values — exactly the reused-pool
production pattern documented in `pool-reuse-fork-safety-gap.md`.

## Why existing tests don't catch it

- `tests/python/test_pool_reuse_fork_safety.py` constructs `Cache(TMP)`
  **inside** each worker → fresh `random_device` seed per process → distinct
  UUID sequences.
- `tests/fork_safety/main.cpp:50` only sets a tiny inline blob
  (`cache.set("k", "child")`) → never exercises `DiskStorage::store()`.

## Secondary issues (same review)

- **Weak seeding**: `gen(rd())` seeds the 19937-bit state with a single
  32-bit word; stduuid's own README seeds the full state via `seed_seq`.
- **No collision guard in `store()`**: whatever the cause of a duplicate
  UUID, the previous value's file is silently overwritten. No defense in
  depth.
- **App-level keyspace sharing** (library can't guard): `memoize` keys start
  with `func.__module__.func.__qualname__` — lambdas, factory-generated or
  redefined functions, or two apps sharing one cache dir with the same module
  path alias into the same keyspace. Args that pickle *by reference* or with
  state-insensitive `__reduce__` produce identical key bytes for different
  logical inputs.
- **`Cache` and `Index` pointed at the same directory** share
  `sciqlop-cache.db` and the same `cache` table with no store-type marker in
  `meta` — they read each other's rows. Serializer mismatch is guarded;
  store-type mismatch is not.

## What was ruled out

- Keys are never hashed at the storage layer: verbatim SQLite
  `TEXT PRIMARY KEY`, exact-match binds for get/exists/delete. No key
  aliasing possible at the DB layer.
- Memoize keys: full 256-bit SHA-256 hexdigest, no truncation (the optional
  `version_aware` code hash is 64-bit but only distinguishes code versions of
  the *same* function).
- Fanout sharding (`std::hash<std::string> % shard_count`) only selects the
  shard DB; rows are still exact-key matched — worst case is
  misses/duplicates across differing STL implementations, never wrong data.

## Recommendations (priority order)

1. **Reseed after fork**: add `DiskStorage::reseed()` (full-state `seed_seq`
   from `random_device`) and call it from `_Store::_fork_child()`. Even more
   bulletproof: mix `getpid()` into the generated filename (e.g.
   `<uuid>-<pid>`), making cross-process path reuse structurally impossible.
2. **Defense in depth in `store()`**: open with exclusive-create semantics
   (`O_CREAT|O_EXCL`, or retry while the path exists) so a duplicate UUID
   regenerates instead of overwriting.
3. **Regression test**: parent creates a `Cache`, forks two children, both
   `set()` file-backed values under different keys → assert distinct on-disk
   paths and intact values.
4. Optional cheap guard: `size` is already stored in the row — validate
   `file_size == size` on `get()` (today only `check()` does) to turn
   contamination into a detectable miss.
