# Fork-safety is unvalidated for reused worker pools

**Status:** open, unconfirmed — needs a dedicated reproducer.

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
touching the cache repeatedly over a long lifetime. README's "Multi-process
safe" claim under **Concurrency** should be read with that scope: proven for
short-lived process-per-task usage, not for reused pools.

## The gap

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
further, since that pattern is the one this repo's tests actually validate.

## Why this is a theory, not a confirmed root cause

- No dedicated reproducer in *this* repo exercises "fork once, then many
  sequential/concurrent cache touches from the surviving child" — the actual
  shape a reused `Pool` produces. The corruption claim rests on one bad CI
  run's symptoms, not an isolated repro.
- The same Speasy investigation separately found and confirmed (via direct
  reproducers built in this repo) that a *different*, unrelated bug — Python
  package import-time network calls, nothing to do with caching or forking —
  caused multi-hour Windows CI hangs in the very same test class. That
  confusion is a concrete precedent for misattributing a hang to "the cache"
  when the real cause was elsewhere. The Pool-reuse theory was never put
  through the same bisection rigor before being abandoned.
- It's plausible on its face — `pthread_atfork`'s **child** handler reopens
  the SQLite connection and resets `_mtx` at the fork instant, but says
  nothing about correctness of *sustained* concurrent use afterward by
  multiple long-lived forked processes against one shared file — but "plausible"
  isn't the same as "demonstrated."

## What would close this

A reproducer analogous to `test_fork_safety.py`, but shaped like a reused
pool instead of one-shot fork:

1. Start N worker processes via `multiprocessing.Pool` (or bare `fork`) once.
2. Have each worker perform many sequential `get`/`set`/`incr` calls against
   the *same* cache file over its lifetime, concurrently with the other
   workers — not a single touch-then-exit.
3. Run this against a build with **no** other confounds (no consumer package
   import, no network calls) to isolate whether corruption is reproducible
   from this repo's code alone.

If it reproduces: the atfork quiescing needs to extend beyond the fork
instant, or the docs need a hard "one-shot fork only" caveat. If it doesn't
reproduce here in isolation: the original Speasy CI symptoms were most likely
a different confound (worth revisiting against the import-time-hang lesson
above), and the caveat can be relaxed once re-verified end-to-end.

## Guidance until this is resolved

Don't reuse an OS-level worker pool (`multiprocessing.Pool`,
`concurrent.futures.ProcessPoolExecutor`) whose workers repeatedly touch a
`Cache`/`Index`/`FanoutCache` backed by the same file across many tasks.
Prefer spawning a short-lived process per unit of work (fork/spawn → touch →
exit) — the pattern the fork-safety fix and its tests actually cover.
