"""get() on a size-limited cache records the LRU touch in memory; the background
thread writes it out once per tick, just before it evicts.

Writing it inline made every hit a write transaction: 5x slower on tmpfs,
13x on btrfs, and a read that blocks behind another process's writer.
"""

import shutil
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

# On one CPU the background thread rarely starts before close(), so it never
# opened its connection and couldn't write the pending reads (lost 189/200).
QUICK_CLOSE = textwrap.dedent("""
    import os, shutil, sys, tempfile
    from pysciqlop_cache import Cache
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {sorted(os.sched_getaffinity(0))[0]})
    value = b"x" * 1000
    lost = 0
    for _ in range(50):
        d = tempfile.mkdtemp(dir=sys.argv[1])
        c = Cache(d, max_size=1 << 30)
        for k in "abc":
            c[k] = value
        c.get("a")
        c.close()
        c = Cache(d, max_size=1 << 30)
        c["d"] = value
        c.set_max_cache_size(3500)
        c.evict()
        lost += "a" not in c
        c.close()
    print(lost)
""")


def _wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.05)
    return predicate()


class _Stores(unittest.TestCase):
    """Closes every store before removing the directory: Windows can't delete open files."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.stores = []

    def tearDown(self):
        for store in self.stores:
            store.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def open(self, **kwargs):
        self.stores.append(Cache(self.dir, **kwargs))
        return self.stores[-1]


class GetIsNotAWrite(_Stores):
    def test_hit_does_not_wait_for_another_process_writer(self):
        cache = self.open(max_size=1 << 30, timeout=0.2)
        cache["k"] = VALUE
        holder = subprocess.Popen([sys.executable, "-c", HOLD_WRITE_LOCK, self.dir, "2"],
                                  stdout=subprocess.PIPE, text=True, cwd=self.dir)
        try:
            self.assertEqual(holder.stdout.readline().strip(), "locked")
            t = time.perf_counter()
            self.assertEqual(cache["k"], VALUE)
            self.assertLess(time.perf_counter() - t, 0.15)
        finally:
            holder.wait()
            holder.stdout.close()


class RecentReadsStillCount(_Stores):
    def _fill(self, cache):
        for k in ("a", "b", "c"):
            cache[k] = VALUE

    def test_background_eviction_keeps_a_just_read_key(self):
        cache = self.open(max_size=3500)
        self._fill(cache)
        cache.get("a")
        cache["d"] = VALUE
        self.assertTrue(_wait_for(lambda: "b" not in cache))
        self.assertIn("a", cache)

    def test_explicit_evict_keeps_a_just_read_key(self):
        cache = self.open(max_size=1 << 30)
        self._fill(cache)
        cache.get("a")
        cache["d"] = VALUE
        cache.set_max_cache_size(3500)
        cache.evict()
        self.assertNotIn("b", cache)
        self.assertIn("a", cache)

    def test_reads_pending_at_close_are_kept(self):
        cache = self.open(max_size=1 << 30)
        self._fill(cache)
        cache.get("a")
        cache.close()
        cache = self.open(max_size=1 << 30)
        cache["d"] = VALUE
        cache.set_max_cache_size(3500)
        cache.evict()
        self.assertNotIn("b", cache)
        self.assertIn("a", cache)

    def test_reads_survive_a_close_right_after_open(self):
        out = subprocess.run([sys.executable, "-c", QUICK_CLOSE, self.dir], cwd=self.dir,
                             capture_output=True, text=True, check=True).stdout
        self.assertEqual(out.strip(), "0")


if __name__ == "__main__":
    unittest.main()
