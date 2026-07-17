# Fork-safety under reused worker pools

**Status:** resolved — root cause confirmed, reproduced, and fixed. The
severity is much lower than originally suspected: no corruption, no crash,
no hang. Kept as a record of the investigation and the residual limitation.

## What is actually validated

The fork-safety fix (`fe4181d` "Make Cache/Index fork-safe via pthread_atfork
quiescing", released as 0.1.3, PR [#5](https://github.com/SciQLop/Sciqlop-cache/pull/5))
makes a **one-shot** `fork → touch the cache once → exit` pattern safe:
`pthread_atfork` handlers quiesce the background checkpoint thread and reset
mutex/SQLite connection state at the fork instant.

This is exactly what both regression tests exercise:

- `tests/python/test_fork_safety.py` — helper threads hammer `_mtx` via reads,
  20 forks, each child does a single `incr()` then exits immediately.
- `tests/fork_safety/main.cpp` (added in `0cfc45b`, made portable in `d66864a`,
  PR [#6](https://github.com/SciQLop/Sciqlop-cache/pull/6)) — same idea in C++,
  needed because a Python child calling `os._exit()` skips gcov's atexit flush.

Neither test models a worker that stays alive after the fork and keeps
touching the cache repeatedly over a long lifetime. That gap is now closed by
`tests/python/test_pool_reuse_fork_safety.py` (see below).

## The gap (as originally reported)

Found downstream in Speasy PR [#315](https://github.com/SciQLop/speasy/pull/315)
(`fix/cachecall-dedup`). `tests/test_cache.py::CacheRequestsDeduplicationMultiProcess`
was changed from spawning a fresh `multiprocessing.Process()` per test step
(400 short-lived forks total) to a persistent `multiprocessing.Pool(processes=4)`
created once and reused across all 100 steps, with each worker repeatedly
touching the same cache file.

That change made CI worse, not better: `build (3.11, ubuntu-latest)` ran for
3+ hours before being force-cancelled, left orphaned processes, and the log
showed errors against the *shared* cache file:

```
Error loading file for key... Failed to open memory-mapped file... deleting entry
```

with knock-on provider-init failures in other jobs of the same run. Speasy
reverted to fresh `Process()` per step (`4ebbe3b`) rather than debug the pool
further.

## Root cause (confirmed)

`tests/python/test_pool_reuse_fork_safety.py` reproduces the *exact* log
signature deterministically: a persistent `multiprocessing.Pool` (forced to
the `fork` start method — Python 3.14 changed the multiprocessing default to
`forkserver`, which doesn't reproduce this at all since it re-imports
`__main__` per worker instead of cloning) with dedicated writer and reader
processes hammering one file-backed (>8KB) key.

The race is **not fork-specific at all** — it's a plain cross-process TOCTOU
between two independent code paths that both touch the same on-disk file
without a shared lock:

- `DiskStorage::load()` (`disk_storage.hpp`) checks `std::filesystem::exists()`
  and then opens a memory-mapped file. Nothing prevents the file from being
  removed between those two steps.
- `_Store::_set_impl()`'s REPLACE path (`store.hpp`) writes the new file,
  commits the DB row pointing at it, and *only then* removes the old file —
  a plain, non-transactional `unlink()` with no cross-process coordination.

A reader that read the *old* path just before a concurrent REPLACE commits
can lose the race: by the time it opens the file, the writer has already
unlinked it. `DiskStorage::load()` catches the failure and `get()`'s existing
fallback (T2-D) cleans up gracefully — so the observed effect is a **spurious
cache miss**, not corruption, a crash, or a deadlock. A reused pool doing
thousands of touches on the same key over a long lifetime turns this
low-probability race into something that reliably fires; a one-shot
fork-touch-exit basically never hits the window.

Confirmed severity: **this does not explain the 3+ hour CI hang.** No hang,
crash, or deadlock reproduces here even under contention far heavier than
the original CI run (a zero-delay busy loop of two writers + two readers on
one key reliably produces >10 misses in 3 seconds; a more realistic paced
workload reliably produces the same class of miss, just less often). The
hang was most likely a different confound — see the precedent already noted
below (the import-time-network-call bug that caused a similar-looking hang
in the same test class during the same investigation).

## Fix

`_Store::get()` (`store.hpp`) now retries the load, bounded to 4 attempts,
re-reading the row's current path each time and stopping as soon as the path
stops changing. This is safe because the writer's ordering guarantees a
freshly-read path's file is always complete (write file → commit row → *then*
remove old file) — a newly observed path is never a half-written one.

This closes the overwhelming majority of the race (in testing: ~0.0155% miss
rate down to 0 misses across several million ops at a realistic pace, and a
~100x reduction even under the pathological zero-delay stress case). It does
**not** provide a hard guarantee under unbounded adversarial contention on a
single key — no bounded retry count can, without reference-counted blob
files and deferred deletion, which is a materially bigger change than this
low-severity, already-gracefully-handled race justifies. This residual
limitation is intentional and documented here rather than fixed further.

## Guidance

Reusing an OS-level worker pool (`multiprocessing.Pool`,
`concurrent.futures.ProcessPoolExecutor`) whose workers repeatedly touch a
`Cache`/`Index`/`FanoutCache` backed by the same file across many tasks is
now a **supported, tested pattern** (`test_pool_reuse_fork_safety.py`).
Extremely hot single-key contention (many processes REPLACE-ing the exact
same key back-to-back with no work in between) can still produce an
occasional spurious miss — treat a `None` `get()` result as "recompute", the
same tolerance any cache client should already have.
