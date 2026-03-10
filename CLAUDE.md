# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sciqlop-cache is a C++20 caching library with Python bindings. It uses SQLite for metadata and a hybrid storage strategy: values ≤ 8KB stored as BLOBs in SQLite, larger values stored as files on disk. Thread-safe via thread-local database connections and WAL mode.

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

# Available test suites: sciqlop-cache:basic, sciqlop-cache:database,
# sciqlop-cache:intermediate, sciqlop-cache:multithreads,
# sciqlop-cache:test_python_interface, sciqlop-cache:test_serializers,
# sciqlop-cache:test_python_multiprocess, sciqlop-cache:test_perf_vs_diskcache

# Build Python wheel
pip install meson-python numpy && python -m build --wheel
```

### Meson Options

- `with_tests` (false) — build C++ tests
- `with_benchmarks` (false) — build benchmarks
- `disable_python_wrapper` (false) — skip Python bindings
- `tracy_enable` (false) — enable Tracy profiling

## Architecture

**Core layer** (`include/sciqlop_cache/`):

- `sciqlop_cache.hpp` — `_Cache<Storage>` template class, main API. Pre-compiles 18 SQL statements per thread-local connection. Instantiated as `Cache` with `DiskStorage`.
- `database.hpp` — SQLite wrapper: `Database`, `CompiledStatement`, `BindedCompiledStatement`, `Transaction`. Uses SQLITE_NOMUTEX with manual transaction control.
- `disk_storage.hpp` — `DiskStorage` class. UUID-based two-level directory hierarchy for file storage.
- `utils/concepts.hpp` — C++20 concepts: `DurationConcept`, `TimePoint`, `Bytes`
- `utils/buffer.hpp` — Polymorphic buffer with memory-mapped file support
- `utils/time.hpp` — Epoch/TimePoint conversions for SQLite

**Python bindings** (`pysciqlop_cache/`): nanobind-based, exposes `Cache` with dict-like interface and `Buffer` with `.memoryview()`.

**Tests** (`tests/`): Catch2 BDD-style. Four C++ suites (basic, database, intermediate, multithreads) plus Python tests.

## Dependencies

All vendored in `subprojects/`: SQLite amalgamation, fmt, nanobind, Catch2, stduuid, cpp_utils, tracy, robin-map, hedley.

## Key Design Decisions

- Thread-local `Database` instances avoid SQLite threading issues
- WAL mode + 600s busy_timeout for multi-process safety
- `size()` computed on demand via `SUM(size)` query
- Expiration handled at query time via SQL (`WHERE expire IS NULL OR expire > unixepoch('now')`)
- No-expiry default: `set()`/`add()` without expire stores NULL (never expires)
- LRU eviction: `max_size` in bytes (0 = unlimited, default). Auto-evicts on `set()`/`add()` using monotonic access counter for ordering
- Tags: optional `tag` parameter on `set()`/`add()`, indexed for fast `evict_tag()` bulk removal. Schema auto-migrates existing databases via `ALTER TABLE ADD COLUMN` (error ignored if column exists)
- C++20 concepts enforce type safety at compile time
