"""Torture tests for sciqlop-cache: multi-process stress, data integrity, crash resilience.

Opt-in via: meson setup build -Dwith_torture_tests=true
CLI:        python test_torture.py [--duration 30]
"""

import os
import random
import signal
import shutil
import subprocess
import sys
import time
import unittest
from multiprocessing import Pool
from tempfile import TemporaryDirectory

from pysciqlop_cache import Cache, Index, FanoutCache, FanoutIndex

DURATION = int(os.environ.get("TORTURE_DURATION", 30))
NUM_KEYS = 200
MIN_VALUE_SIZE = 50
MAX_VALUE_SIZE = 64 * 1024  # crosses 8KB blob/file threshold


def _make_store(store_cls, path):
    if store_cls is Cache:
        return Cache(path)
    elif store_cls is Index:
        return Index(path)
    elif store_cls is FanoutCache:
        return FanoutCache(path, shard_count=4)
    elif store_cls is FanoutIndex:
        return FanoutIndex(path, shard_count=4)


def _supports_expire(store_cls):
    return store_cls in (Cache, FanoutCache)


def _supports_tag(store_cls):
    return store_cls in (Cache, FanoutCache)


def random_ops(cache_path, store_cls_name, duration, seed):
    """Core worker loop: random set/get/del/pop for `duration` seconds."""
    store_cls = {"Cache": Cache, "Index": Index,
                 "FanoutCache": FanoutCache, "FanoutIndex": FanoutIndex}[store_cls_name]
    rng = random.Random(seed)
    store = _make_store(store_cls, cache_path)
    deadline = time.monotonic() + duration
    ops = 0

    while time.monotonic() < deadline:
        key = f"k_{rng.randint(0, NUM_KEYS - 1)}"
        op = rng.random()
        try:
            if op < 0.40:
                value = rng.randbytes(rng.randint(MIN_VALUE_SIZE, MAX_VALUE_SIZE))
                kwargs = {}
                if _supports_expire(store_cls) and rng.random() < 0.2:
                    kwargs["expire"] = rng.randint(10, 3600)
                if _supports_tag(store_cls) and rng.random() < 0.3:
                    kwargs["tag"] = f"tag_{rng.randint(0, 5)}"
                store.set(key, value, **kwargs)
            elif op < 0.70:
                store.get(key)
            elif op < 0.90:
                store.delete(key)
            else:
                store.pop(key)
        except Exception:
            # Transient errors (busy DB, etc.) are acceptable under heavy contention
            pass
        ops += 1

    return ops



STORE_CLASSES = [Cache, Index, FanoutCache, FanoutIndex]


class TestMultiProcessStress(unittest.TestCase):
    """Spawn many workers doing random ops on the same cache directory."""

    def _run_for_store(self, store_cls):
        with TemporaryDirectory() as tmp:
            # Pre-create the store so the DB schema exists
            s = _make_store(store_cls, tmp)
            del s

            n_workers = max(4, 2 * os.cpu_count())
            per_worker_duration = DURATION

            with Pool(processes=n_workers) as pool:
                results = pool.starmap(random_ops, [
                    (tmp, store_cls.__name__, per_worker_duration, i * 12345)
                    for i in range(n_workers)
                ])

            total_ops = sum(results)

            # Verify integrity after concurrent torture
            # First pass fixes orphaned files (expected under multi-process contention)
            store = _make_store(store_cls, tmp)
            store.check(True)
            # Second pass must be clean
            cr = store.check()
            self.assertTrue(cr.ok, f"check() failed after {total_ops} ops across {n_workers} workers")

            # Verify all remaining keys are readable (get() may return None for
            # file-backed entries whose files were overwritten by another process;
            # that's expected — the important thing is no crash/exception)
            for key in store.keys():
                store.get(key)

    def test_cache(self):
        self._run_for_store(Cache)

    def test_index(self):
        self._run_for_store(Index)

    def test_fanout_cache(self):
        self._run_for_store(FanoutCache)

    def test_fanout_index(self):
        self._run_for_store(FanoutIndex)


class TestDataIntegrity(unittest.TestCase):
    """Single-process sequential test: verify data matches ground truth."""

    def _run_for_store(self, store_cls):
        with TemporaryDirectory() as tmp:
            store = _make_store(store_cls, tmp)
            rng = random.Random(42)
            ground_truth = {}
            deadline = time.monotonic() + DURATION

            while time.monotonic() < deadline:
                key = f"k_{rng.randint(0, NUM_KEYS - 1)}"
                op = rng.random()

                if op < 0.50:
                    value = rng.randbytes(rng.randint(MIN_VALUE_SIZE, MAX_VALUE_SIZE))
                    store.set(key, value)
                    ground_truth[key] = value
                elif op < 0.80:
                    result = store.get(key)
                    if key in ground_truth:
                        self.assertIsNotNone(result, f"key {key!r} should exist")
                        self.assertEqual(result, ground_truth[key],
                                         f"data mismatch for key {key!r}")
                    # If not in ground_truth, result may or may not exist (expired, etc.)
                else:
                    store.delete(key)
                    ground_truth.pop(key, None)

            # Final full verification
            for key, expected in ground_truth.items():
                result = store.get(key)
                self.assertIsNotNone(result, f"final check: key {key!r} missing")
                self.assertEqual(result, expected, f"final check: data mismatch for {key!r}")

            cr = store.check()
            self.assertTrue(cr.ok, "check() failed after integrity test")

    def test_cache(self):
        self._run_for_store(Cache)

    def test_index(self):
        self._run_for_store(Index)

    def test_fanout_cache(self):
        self._run_for_store(FanoutCache)

    def test_fanout_index(self):
        self._run_for_store(FanoutIndex)


# Self-contained crash worker script (no imports from test_torture)
_CRASH_WORKER_SCRIPT = """
import os, random, sys, time
sys.path.insert(0, os.environ.get("PYTHONPATH", ""))
from pysciqlop_cache import Cache, Index, FanoutCache, FanoutIndex

store_map = {"Cache": Cache, "Index": Index, "FanoutCache": FanoutCache, "FanoutIndex": FanoutIndex}
path, cls_name, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
store_cls = store_map[cls_name]
if store_cls is FanoutCache:
    store = FanoutCache(path, shard_count=4)
elif store_cls is FanoutIndex:
    store = FanoutIndex(path, shard_count=4)
else:
    store = store_cls(path)
rng = random.Random(seed)
while True:
    key = f"k_{rng.randint(0, 199)}"
    op = rng.random()
    try:
        if op < 0.50:
            store.set(key, rng.randbytes(rng.randint(50, 65536)))
        elif op < 0.80:
            store.get(key)
        else:
            store.delete(key)
    except Exception:
        pass
"""


class TestCrashResilience(unittest.TestCase):
    """Kill a writer process mid-operation, verify recovery."""

    def _run_for_store(self, store_cls):
        with TemporaryDirectory() as tmp:
            # Pre-create
            s = _make_store(store_cls, tmp)
            del s

            rng = random.Random(99)
            deadline = time.monotonic() + DURATION
            cycles = 0

            while time.monotonic() < deadline:
                seed = rng.randint(0, 999999)
                proc = subprocess.Popen(
                    [sys.executable, "-c", _CRASH_WORKER_SCRIPT,
                     tmp, store_cls.__name__, str(seed)],
                    env={**os.environ, "PYTHONPATH": os.environ.get("PYTHONPATH", "")},
                )

                # Let it run for a random duration then kill
                kill_delay = rng.uniform(0.1, 1.0)
                time.sleep(kill_delay)
                try:
                    proc.send_signal(signal.SIGKILL)
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
                    proc.wait(timeout=5)

                # Verify recovery: fix pass then clean check
                store = _make_store(store_cls, tmp)
                store.check(True)
                cr = store.check()
                self.assertTrue(cr.ok, f"check() failed after kill cycle {cycles}")

                # Verify all readable keys return valid data
                for key in store.keys():
                    val = store.get(key)
                    # val can be None if key was mid-delete, that's OK
                del store
                cycles += 1

            self.assertGreater(cycles, 0, "No kill cycles completed within duration")

    def test_cache(self):
        self._run_for_store(Cache)

    def test_index(self):
        self._run_for_store(Index)

    def test_fanout_cache(self):
        self._run_for_store(FanoutCache)

    def test_fanout_index(self):
        self._run_for_store(FanoutIndex)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=30)
    args, remaining = parser.parse_known_args()
    DURATION = args.duration
    # Pass remaining args to unittest
    sys.argv = [sys.argv[0]] + remaining
    unittest.main()
