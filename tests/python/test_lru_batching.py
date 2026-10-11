"""get() on a size-limited cache records the LRU touch in memory; the background
thread writes it out once per tick, just before it evicts.

Writing it inline made every hit a write transaction: 5x slower on tmpfs,
13x on btrfs, and a read that blocks behind another process's writer.
"""

import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

from pysciqlop_cache import Cache

HOLD_WRITE_LOCK = textwrap.dedent("""
    import sys, time
    from pysciqlop_cache import Cache
    c = Cache(sys.argv[1], max_size=1 << 30)
    with c.transact():
        c["other"] = b"x"
        print("locked", flush=True)
        time.sleep(float(sys.argv[2]))
""")

VALUE = b"x" * 1000


def _sizes_until(cache, predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.05)
    return predicate()


class GetIsNotAWrite(unittest.TestCase):
    def test_hit_does_not_wait_for_another_process_writer(self):
        with tempfile.TemporaryDirectory() as d:
            cache = Cache(d, max_size=1 << 30, timeout=0.2)
            cache["k"] = VALUE
            holder = subprocess.Popen([sys.executable, "-c", HOLD_WRITE_LOCK, d, "2"],
                                      stdout=subprocess.PIPE, text=True, cwd=d)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "locked")
                t = time.perf_counter()
                self.assertEqual(cache["k"], VALUE)
                self.assertLess(time.perf_counter() - t, 0.15)
            finally:
                holder.wait()


class RecentReadsStillCount(unittest.TestCase):
    def _fill(self, cache):
        for k in ("a", "b", "c"):
            cache[k] = VALUE

    def test_background_eviction_keeps_a_just_read_key(self):
        with tempfile.TemporaryDirectory() as d:
            cache = Cache(d, max_size=3500)
            self._fill(cache)
            cache.get("a")
            cache["d"] = VALUE
            self.assertTrue(_sizes_until(cache, lambda: "b" not in cache))
            self.assertIn("a", cache)

    def test_explicit_evict_keeps_a_just_read_key(self):
        with tempfile.TemporaryDirectory() as d:
            cache = Cache(d, max_size=1 << 30)
            self._fill(cache)
            cache.get("a")
            cache["d"] = VALUE
            cache.set_max_cache_size(3500)
            cache.evict()
            self.assertNotIn("b", cache)
            self.assertIn("a", cache)

    def test_reads_pending_at_close_are_kept(self):
        with tempfile.TemporaryDirectory() as d:
            cache = Cache(d, max_size=1 << 30)
            self._fill(cache)
            cache.get("a")
            cache.close()
            cache = Cache(d, max_size=1 << 30)
            cache["d"] = VALUE
            cache.set_max_cache_size(3500)
            cache.evict()
            self.assertNotIn("b", cache)
            self.assertIn("a", cache)


if __name__ == "__main__":
    unittest.main()
