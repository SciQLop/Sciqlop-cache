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
#                     sciqlop-cache:test_concurrency_bugs,
#                     sciqlop-cache:test_free_threading
#   Python (with_torture_tests=true):
#                     sciqlop-cache:test_torture, sciqlop-cache:test_hypothesis

# Build Python wheel
pip install meson-python numpy && python -m build --wheel

# Run the bulk-insert benchmark (bg checkpoint/eviction stall + WAL size
# under sustained writes; not run by `meson test` by default)
meson test -C build --benchmark bgscan
```

### Coverage

```bash
meson setup build-cov -Db_coverage=true -Dwith_tests=true && meson compile -C build-cov
meson test -C build-cov            # C++ counters (.gcda) accumulate across runs
gcovr -r . build-cov --filter include/ --filter pysciqlop_cache/ --merge-lines --txt
# Python: start coverage.py in every process with a .pth hook (see .coveragerc and
# .github/workflows/tests-with-coverage.yml), run the tests, then coverage combine/report
```

`--merge-lines` matters: without it a line counts as uncovered if any template
instantiation of it never runs. The remaining C++ gaps are defensive SQLite error paths,
the file-name collision heal (random UUIDs, not forceable from a test) and the
checkpoint-connection retry.

### Documentation

Sphinx site in `docs/` (furo, sphinx-design, mermaid, napoleon), published on Read the Docs
(`.readthedocs.yaml` builds the package from source so autodoc can import it). User docs
live there; the README is only a pitch plus links. `docs/` also holds design notes
(`plans/`, `superpowers/`, `known-issues/`, `*.md`) excluded from the site.

```bash
pip install -r docs/requirements.txt
cd docs && PYTHONPATH=../build python -m sphinx -W -b html . /tmp/html   # run from docs/:
# from the repo root the source pysciqlop_cache/ (no .so) shadows the build
PYTHONPATH=build python docs/check_examples.py   # runs every ``code-block:: python``
```

Every Python example must run (each page's blocks share one namespace, in a temp dir),
so no placeholders. Docstrings are Google style (napoleon); the C++-only methods get theirs
from the `doc::` table in `pysciqlop_cache.cpp`, and `_share_docstrings()` copies Cache's and
Index's onto the fanout classes.

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
- **Every binding that can take the store mutex must release the GIL** (`nb::call_guard<nb::gil_scoped_release>()`, or a scoped `nb::gil_scoped_release` inside a lambda that needs the GIL for its Python args/return). A thread inside `transact()` or iterating holds `_mtx` and needs the GIL to run Python code; a binding that blocks on `_mtx` with the GIL held freezes the interpreter. Only GIL builds can deadlock. Guarded by `BindingsReleaseGilWhileWaiting` in `tests/python/test_concurrency_bugs.py` — add new store methods to its `OPS` table.
- **Free-threaded Python (3.13t+)** — nanobind declares `Py_MOD_GIL_NOT_USED` automatically when built against a `Py_GIL_DISABLED` interpreter, so the module runs GIL-free. `tests/python/test_free_threading.py` asserts the GIL stays off after import and runs a shared-store thread torture (`FT_TORTURE_DURATION`, default 3 s). It is always-on so CI's 3.14t wheel job exercises it.
- WAL mode + 600s busy_timeout for multi-process safety
- **`_NestedTxn` (private RAII helper in `_Store`)** — every internal write path (`_set_impl`, `del`, `pop`, `incr`, `evict_tag`) wraps in a `BEGIN EXCLUSIVE` only at the outermost level (depth-counted via `_txn_depth`). Inner levels are no-ops. Both for cross-process atomicity (read-modify-write inside one txn) and to compose cleanly inside a user `transact()`.
- **`TransactionGuard` is reentrant on the same thread** (depth-counted same as `_NestedTxn`). Nested `with cache.transact():` is supported; outer rollback discards inner work (no real SAVEPOINTs — same semantics as diskcache).
- **Counters live in the DB, not memory** — `size()`/`count()`/`volume()` are `meta` rows (`size`, `count`, `file_size`) kept exactly by incremental (`value + NEW.size`, O(1)) INSERT/UPDATE/DELETE triggers on `cache`, in the same transaction as the row change. Cross-process exact by construction, so there's no resync step and the bg thread never needs `_mtx` for counters. `count()` always reads the maintained `meta` row (O(1)) — it no longer runs a WHERE-filtered `COUNT(*)` for types with expiration, so it may briefly include an entry that's expired but not yet evicted by the background thread (same semantics as diskcache's `len()`). `volume()` is `page_count*page_size` + the `file_size` meta row (sum of `size` for rows with `path IS NOT NULL`) — O(1) instead of walking the on-disk tree, and it only counts live values (an orphaned file contributes nothing; `check()` reports those separately). `size()`/`count()`/`volume()` are single-row queries taken under the store mutex (they block behind a long `transact()`); `clear()` is O(N) because triggers disable SQLite's truncate optimisation.
- **BG checkpoint thread never takes `_mtx`** — it only runs a PASSIVE `wal_checkpoint` + eviction (`_bg_evict`) each tick. The bg connection uses a 250 ms `sqlite3_busy_timeout` (it has none by default). Its DELETEs queue files through the `cache_trash_delete` trigger in the same statement, so a DELETE that loses to the writer's own BEGIN EXCLUSIVE changes nothing and orphans nothing. The WAL is bounded by `PRAGMA wal_autocheckpoint=4000` on the writer connection instead: the bg thread's PASSIVE checkpoint alone can't reset the WAL under a continuous writer (checkpoint starvation), but the writer's own autocheckpoint finishes the small remainder once the WAL exceeds ~16 MB, keeping it flat instead of growing unbounded.
- **Deferred file deletion (`trash` table), for every path** — a value file is never unlinked inline. Two schema triggers, `cache_trash_delete` (AFTER DELETE, which also covers REPLACE since `recursive_triggers=ON`) and `cache_trash_update` (AFTER UPDATE OF path, e.g. `incr()` rewriting a file to a blob), queue `OLD.path` into `trash` in the same transaction as the row change. So `set`/`del`/`pop`/`evict`/`expire`/`evict_tag`/bg eviction need no file code at all, and three problems share one fix: (1) a reader in another process between `SELECT path` and `open()` (pool-reuse TOCTOU, `docs/known-issues/pool-reuse-fork-safety-gap.md`); (2) a delete inside a user `transact()` that rolls back (the trash row rolls back too, so the file survives; it used to be unlinked before the outer commit, losing the value); (3) Windows refusing to delete a file a live `Buffer` maps. The bg thread drains entries older than 5 s (`_trash_grace_secs`) in one `BEGIN IMMEDIATE` batch of at most 512: a row is dropped only once its file is gone, otherwise its `ts` is bumped and it is retried after another grace period; a busy DB skips the tick. Final drain at close. `clear()` wipes the tree and queues whatever it couldn't remove. `check()` treats pending trash as known files; the collision-heal reference check (`IS_PATH_REFERENCED_SQL`) covers trash too. Disk space of deleted big values is reclaimed ~5 s later (A/B: del/pop/evict_tag of file values 20-80% faster, set -5%). Contract tests: `tests/python/test_trash_deferred_delete.py`, `tests/python/test_mapped_file_sharing.py`.
- **Write failures raise, never return `false`** — `DiskStorage::store()` throws `std::system_error` with the real `errno` (full disk, quota, permissions), which a translator in the bindings turns into Python `OSError` (or its errno subclass). Every write step in `set`/`add`/`incr` goes through `_step_write()`, which throws unless SQLite reports `SQLITE_DONE`: a failed statement followed by a successful commit would otherwise report success for a value that was never stored (issue #16). Reproducer: `tests/python/test_set_write_failure.py` (uses `RLIMIT_FSIZE`).
- **Never open a live cache DB with a second SQLite library in the same process** (e.g. Python's `sqlite3`) — per-process `fcntl` locks let the second library's connection truncate the `-shm` file under our own mmap, causing SIGBUS. Tests must `del cache` (closing our connection) before calling `sqlite3.connect()` on the same path.
- **`close()` is optional, not required** — `~_Store()` already stops the background checkpoint thread and closes the connection deterministically on destruction (Python: when the object is garbage-collected), so nothing leaks if a caller never calls it. It's exposed on `Cache`/`Index`/`FanoutCache`/`FanoutIndex` (Python `close()`) purely for diskcache-compatible cleanup idioms (`cache.close()` in a `finally:` block) — diskcache callers doing this got `AttributeError: 'Cache' object has no attribute 'close'` before (issue #11). `close()` stops the checkpoint thread (idempotent, same order the destructor uses) then closes the SQLite connection; calling it twice, or on a `FanoutCache`/`FanoutIndex` (closes every shard), is safe. **`__enter__`/`__exit__` do NOT call `close()`** — `with cache:` is a plain no-op scope, not a diskcache-style thread-local connection reset (diskcache's `close()` is cheap and lazily reconnects on next access; ours is a real one-way shutdown, so mirroring diskcache's "call close() on `__exit__`" surface behavior silently killed reuse across sequential `with` blocks on the same object — issue #12, a regression introduced by the issue-#11 fix and never actually requested there). Any operation on a closed store — `get()`, `set()`, etc. — now raises `RuntimeError` ("operation on a closed store") instead of silently misbehaving; the check lives once in `_Store::db()` (the shared accessor nearly every method goes through), while `close()`/`opened()` use the internal `_raw_db()` to stay usable on an already-closed store. Known gap: `iterkeys()` builds its `KeyCursor` from the raw `sqlite3*` handle directly (different locking discipline) and is not yet covered by this guard.
- **`PickleOOBSerializer` (`"pickle-oob"`, opt-in)** — pickle protocol 5 with array buffers ≥ 64 KiB stored out-of-band after a small header (`MAGIC | count | header size | count × (codec, stored size, raw size) | header | buffers`). Codec 0 = raw, codec 1 = a sequence of blosc2 chunks (≤ 256 MiB each, self-describing headers); ids are persisted, never reuse one (`BufferCodec` in `pysciqlop_cache/buffer_codecs.hpp`). With `compress=True` (default) each buffer ≥ 256 KiB goes through `compress_buffer()` (C++, GIL released): lz4 + shuffle, kept only if ≥ `min_ratio` (2) times smaller; buffers ≥ 128 KiB are first tested on a 64 KiB sample, so a noisy float array is rejected in ~6 µs. c-blosc2 3.3.4 is vendored like CDFpp (`subprojects/c-blosc2.wrap` + `packagefiles/c-blosc2/meson.build`, CDFpp's port minus zstd; `-Dwith_blosc2=false` builds without it, then codec-1 values raise on load). `pysciqlop_cache/blosc2_runtime.hpp`: (1) a persistent `JobPool` (hardware_concurrency − 1 workers, caller helps) installed via `blosc2_set_threads_callback` — blosc2's own shared pool is destroyed with its last context, which cost ~0.35 ms per call, and does not survive fork; decompression uses one thread per 2 MiB of output (capped at the core count): blosc2 decompression gives each thread a fixed block range, so a preempted pool worker stalls the call — under load a 0.5 MB decode took 0.7 ms with 16 threads vs 0.17 ms on the caller alone; (2) at most 2 idle compression contexts per item size (a fresh one faults in ~8 MiB of scratch, ~7 ms); (3) one mutex for both, held across fork via `pthread_atfork` — the child drops the parent's pool (leaked) and starts its own; (4) one shuffle at import, because blosc2 initialises its SIMD kernel table lazily without a barrier (TSan race, unsafe on ARM). Multithreaded blosc2 output is not byte-deterministic (blocks land in completion order); every layout decodes the same. `dumps_chunks()` returns `[prefix+header, *array views]`; the `set`/`add` bindings accept bytes or a list/tuple of buffers (`PyPayload` → `ByteChunks`), and `DiskStorage` writes the chunks with one `writev` (a second `write()` to a new file costs ~15 µs on btrfs), GIL released, no join. Values ≤ 8 KB are flattened into a blob. On load `decode_buffers(src, jobs)` (C++) validates every (codec, offset, size, dst) job with the GIL held, then fills fresh `np.empty` arrays with the GIL released. So neither `set` nor `get` of a big value holds the GIL for the array bytes (33 MB day: set 21 → 3.9 ms, GIL 15.6 → 0 ms). datetime64/timedelta64 arrays are pickled as int64 views via `reducer_override`, since numpy won't export them as `PickleBuffer`. Values without big buffers are plain pickle, and plain entries still load, so `pickle` → `pickle-oob` is the one serializer switch allowed on an existing cache (`SERIALIZER_UPGRADES`). Bench: `benchmark/pickle_oob_gil.py`.
- Expiration handled at query time via SQL (`WHERE expire IS NULL OR expire > unixepoch('now')`) — only present in types with `WithExpiration`
- No-expiry default: `set()`/`add()` without expire stores NULL (never expires)
- LRU eviction: `max_size` in bytes (0 = unlimited, default). Background thread evicts using monotonic access counter — only present with `WithEviction`
- Tags: optional `tag` parameter on `set()`/`add()`, indexed for fast `evict_tag()` bulk removal — only present with `WithTags`
- C++20 concepts enforce type safety at compile time
- `requires` clauses on methods ensure policy-specific API (e.g. `touch()`, `evict()`) is only available on types that support it
