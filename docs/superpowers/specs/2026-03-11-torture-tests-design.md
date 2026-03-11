# Torture / Fuzz Tests for sciqlop-cache

## Goal

Add opt-in torture tests that stress multi-process concurrency, data integrity, and crash resilience across all four store types (`Cache`, `Index`, `FanoutCache`, `FanoutIndex`).

## Meson integration

- New boolean option `with_torture_tests` (default `false`)
- Registers `test_torture` with `timeout: 120`, `is_parallel: false`
- Only built when both `with_torture_tests=true` and Python wrapper is enabled

## Test file

`tests/python/test_torture.py`

### CLI parameter

`--duration N` (default 30) — seconds per test. Passed via meson test env or command line.

### Shared infrastructure

- `random_ops(cache_path, store_type_name, duration, seed)` — core worker loop
  - Random operations: set (40%), get (30%), del (20%), pop (10%)
  - Key space: `k_0` to `k_199` (200 keys, frequent collisions)
  - Value sizes: uniform random 50B to 64KB (crosses 8KB blob/file threshold)
  - Runs for `duration` seconds, seeded PRNG for reproducibility
- Store types parametrized: `Cache`, `Index`, `FanoutCache`, `FanoutIndex`
  - `Index`/`FanoutIndex` use raw bytes (no expire/tag params)
  - `Cache`/`FanoutCache` randomly include expire and tag on some ops

### Test 1: Multi-process stress (`test_multiprocess_stress`)

1. Spawn `2 * os.cpu_count()` worker processes via `multiprocessing.Pool`
2. Each worker runs `random_ops` on the same cache directory with a unique seed
3. After all workers complete:
   - Open cache, run `check(fix=True)`, assert `ok`
   - Iterate all remaining keys, verify each `get()` returns non-None data

### Test 2: Data integrity (`test_data_integrity`)

1. Single process, sequential
2. Maintain in-memory `dict` as ground truth
3. For `duration` seconds, randomly:
   - `set(key, value)` → update dict
   - `get(key)` → compare with dict (must match or both be absent)
   - `del(key)` → remove from dict
4. After loop: iterate all cache keys, verify each matches dict
5. Random value sizes 50B to 64KB

### Test 3: Crash resilience (`test_crash_resilience`)

1. Loop until duration budget exhausted:
   a. Spawn subprocess running `random_ops` on the cache
   b. After random delay (0.1–1.0s), send `SIGKILL`
   c. Open cache, run `check(fix=True)`, assert `ok`
   d. Iterate all readable keys, verify `get()` returns valid data (not corrupt)
2. Multiple kill cycles per test run

## Success criteria

- All three tests pass on all four store types within the timeout
- No crashes, no data corruption, no hung processes
- `check(fix=True).ok` is always `True` after recovery
