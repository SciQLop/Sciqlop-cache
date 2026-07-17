"""Reproducer for docs/known-issues/pool-reuse-fork-safety-gap.md.

The 0.1.3 fork-safety fix (pthread_atfork quiescing, fe4181d) is only
validated by a one-shot ``fork -> touch once -> exit`` pattern
(test_fork_safety.py). Speasy PR #315 switched a test from spawning a fresh
Process() per step to a persistent ``multiprocessing.Pool`` reused across 100
steps, and CI got a lot worse: a 3+ hour hang plus logs showing

    Error loading file for key... Failed to open memory-mapped file... deleting entry

against the shared cache file. This models that exact shape: a Pool created
*once*, whose workers repeatedly touch the *same* key with file-backed
(>8KB) values over a sustained lifetime — not a single touch-then-exit.

Root cause (confirmed by this reproducer): DiskStorage::load() (existence
check, then mmap-open) races _set_impl's REPLACE path, which commits the
new row *before* removing the old file (store.hpp). A reader that read the
old path just before a REPLACE can lose that race and see the old file
vanish out from under it. It's benign as coded — get() falls back to a
graceful miss, no corruption or crash — but it's still wrong: the key was
never logically deleted, only replaced, so it should never spuriously miss.
Confirmed severity: NOT the cause of the 3+ hour CI hang (no hang or crash
reproduces here even under much heavier sustained contention than the CI
run saw) — most likely a different confound, per the known-issue doc's own
caution about the unrelated import-time-hang precedent in the same test
class.

Everything runs inside a subprocess so a real hang shows up as a bounded
timeout instead of wedging the test runner, and so we can inspect the
child's stderr for the exact log signature from the CI failure.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest


_POOL_WORKER_SCRIPT = '''
import json, sys, time
import multiprocessing
from pysciqlop_cache import Cache

TMP = sys.argv[1]
KEY = "shared"
DURATION_S = 3.0
N_WRITERS = 2
N_READERS = 2
# >8KB forces the file-backed DiskStorage/mmap path, where the CI error
# ("Failed to open memory-mapped file") originates.
PAYLOAD_A = b"A" * (12 * 1024)
PAYLOAD_B = b"B" * (16 * 1024)


def writer(_):
    # A tiny per-iteration delay keeps this a "sustained, demanding, reused
    # pool" workload (matching the known-issue doc's "many sequential calls
    # over its lifetime") rather than a zero-delay busy loop with no
    # analogue in real usage — a real worker does some actual work between
    # cache touches. Readers below stay unthrottled: polling a cache as
    # fast as possible (e.g. "is my dedup key ready yet") is realistic.
    cache = Cache(TMP)
    end = time.time() + DURATION_S
    errors, n = 0, 0
    last_error = ""
    i = 0
    while time.time() < end:
        try:
            cache.set(KEY, PAYLOAD_A if i % 2 == 0 else PAYLOAD_B)
        except Exception as e:  # nanobind maps C++ exceptions to RuntimeError
            errors += 1
            last_error = repr(e)
        i += 1
        n += 1
        time.sleep(0.001)
    return {"role": "writer", "errors": errors, "mismatches": 0,
            "misses": 0, "hits": n, "last_error": last_error}


def reader(_):
    # A reader races writers on the *same* key with no self-serialization:
    # unlike a worker that set()s then immediately get()s its own write,
    # this can observe another process mid-REPLACE (new file written, old
    # file removed) at any point.
    cache = Cache(TMP)
    end = time.time() + DURATION_S
    errors = mismatches = misses = hits = 0
    last_error = ""
    while time.time() < end:
        try:
            got = cache.get(KEY)
        except Exception as e:
            errors += 1
            last_error = repr(e)
            continue
        if got is None:
            misses += 1
        elif got == PAYLOAD_A or got == PAYLOAD_B:
            hits += 1
        else:
            mismatches += 1
            last_error = f"corrupted value len={len(got)}"
    return {"role": "reader", "errors": errors, "mismatches": mismatches,
            "misses": misses, "hits": hits, "last_error": last_error}


def run(role_and_arg):
    role, arg = role_and_arg
    return (writer if role == "writer" else reader)(arg)


if __name__ == "__main__":
    # Force "fork" explicitly: this reproduces the shape that actually hit
    # Speasy's CI (fork was the multiprocessing default on Linux for
    # Python < 3.14; 3.14 switched the default to "forkserver").
    ctx = multiprocessing.get_context("fork")
    cache = Cache(TMP)
    cache.set(KEY, PAYLOAD_A)  # seed so early readers see a real value
    del cache
    jobs = [("writer", i) for i in range(N_WRITERS)] + [("reader", i) for i in range(N_READERS)]
    with ctx.Pool(processes=len(jobs)) as pool:
        results = pool.map(run, jobs)
    summary = {"errors": sum(r["errors"] for r in results),
               "mismatches": sum(r["mismatches"] for r in results),
               "misses": sum(r["misses"] for r in results),
               "hits": sum(r["hits"] for r in results),
               "last_errors": [r["last_error"] for r in results if r["last_error"]]}
    print("RESULT:" + json.dumps(summary))
'''


@unittest.skipUnless(hasattr(os, "fork"), "fork() not available on this platform")
class TestPoolReuseForkSafety(unittest.TestCase):

    def test_reused_pool_survives_sustained_concurrent_touches(self):
        tmp_dir = tempfile.mkdtemp()
        env = os.environ.copy()
        env["PYTHONPATH"] = (
            os.path.join(os.path.dirname(__file__), "..", "..", "build")
            + os.pathsep + env.get("PYTHONPATH", ""))

        try:
            res = subprocess.run(
                [sys.executable, "-c", _POOL_WORKER_SCRIPT, tmp_dir],
                capture_output=True, text=True, timeout=90.0, env=env)
        except subprocess.TimeoutExpired as e:
            self.fail(
                "reused pool hung under sustained concurrent get/set on a "
                "shared key (matches the Speasy CI 3+ hour hang). "
                f"stdout so far: {(e.stdout or b'').decode(errors='replace')!r} "
                f"stderr so far: {(e.stderr or b'').decode(errors='replace')!r}")

        self.assertEqual(res.returncode, 0,
            f"worker pool crashed (returncode={res.returncode}).\n"
            f"stdout={res.stdout!r}\nstderr={res.stderr!r}")

        result_line = next(
            (line for line in res.stdout.splitlines() if line.startswith("RESULT:")),
            None)
        self.assertIsNotNone(result_line,
            f"worker script did not print a summary.\n"
            f"stdout={res.stdout!r}\nstderr={res.stderr!r}")
        summary = json.loads(result_line[len("RESULT:"):])

        if "Failed to open memory-mapped file" in res.stderr:
            print("\n[diagnostic] reproduced the CI log signature "
                  "'Failed to open memory-mapped file' under sustained "
                  "reused-pool load (see stderr).", file=sys.stderr)

        self.assertEqual(summary["mismatches"], 0,
            f"data corruption: a get() returned neither payload variant "
            f"nor None. summary={summary}")
        self.assertEqual(summary["errors"], 0,
            f"unexpected exceptions raised by get()/set() under sustained "
            f"reused-pool load. summary={summary}\nstderr={res.stderr!r}")
        # The key is only ever REPLACEd, never deleted, so it must never
        # spuriously miss. A nonzero count here is the TOCTOU race between
        # DiskStorage::load()'s exists-check/mmap-open and a concurrent
        # REPLACE's old-file removal (see module docstring).
        self.assertEqual(summary["misses"], 0,
            f"spurious cache miss(es) for a key that was only ever "
            f"REPLACEd, never deleted — TOCTOU race in DiskStorage::load() "
            f"vs. a concurrent REPLACE's old-file removal. "
            f"summary={summary}\nstderr={res.stderr!r}")


if __name__ == "__main__":
    unittest.main()
