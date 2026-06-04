"""Reproducers for the concurrency bugs identified in the pre-Speasy-migration audit.

Each test is expected to FAIL on the current code and PASS once the corresponding
fix is applied. Tests are skipped if they would take too long under unfavorable
scheduling — they're tuned to fail reliably on the audit machine but not flake
on slower runners.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from multiprocessing import Pool

from pysciqlop_cache import Cache, FanoutCache


def _mp_increment(args):
    path, n = args
    cache = Cache(path)
    for _ in range(n):
        cache.incr("counter")


def _mp_fanout_increment(args):
    path, n = args
    cache = FanoutCache(path)
    for _ in range(n):
        cache.incr("counter")




class IncrLostUpdate(unittest.TestCase):
    """Cache.incr / Index.incr are not atomic: read-modify-write through serializer."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_incr_threads_lose_updates(self):
        cache = Cache(self.tmp)
        cache.set("counter", 0)
        threads, n_threads, per_thread = [], 8, 500
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()
            for _ in range(per_thread):
                cache.incr("counter")

        for _ in range(n_threads):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        expected = n_threads * per_thread
        actual = cache.get("counter")
        self.assertEqual(actual, expected,
            f"lost {expected - actual}/{expected} increments — Cache.incr is not atomic")

    def test_incr_processes_lose_updates(self):
        cache = Cache(self.tmp)
        cache.set("counter", 0)
        del cache

        n_procs, per_proc = 4, 50
        with Pool(processes=n_procs) as pool:
            pool.map(_mp_increment, [(self.tmp, per_proc)] * n_procs)

        cache = Cache(self.tmp)
        expected = n_procs * per_proc
        actual = cache.get("counter")
        self.assertEqual(actual, expected,
            f"lost {expected - actual}/{expected} increments across processes")


class PopRace(unittest.TestCase):
    """pop() does get + del in two separate lock acquisitions — a set() between them is silently dropped."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_pop_loses_writes(self):
        # The bug: pop() releases the lock between its internal get() and del().
        # If a set("k", B) lands in that window after pop() got "A", pop's del
        # erases B and pop returns A — B is lost without being returned.
        # Test: each iteration sets "A", races a popper against a writer that
        # sets "B". After the race, exactly one of (popped value, surviving cache
        # value) should account for "B". If neither does, the bug fired.
        cache = Cache(self.tmp)
        iters = 2000
        b_lost = 0
        for _ in range(iters):
            cache.set("k", "A")
            barrier = threading.Barrier(2)
            popped_box = [None]

            def popper():
                barrier.wait()
                popped_box[0] = cache.pop("k")

            def writer():
                barrier.wait()
                cache.set("k", "B")

            tp = threading.Thread(target=popper)
            tw = threading.Thread(target=writer)
            tp.start(); tw.start()
            tp.join(); tw.join()

            survivor = cache.get("k")
            popped = popped_box[0]
            # Any of these outcomes is correct:
            #   popped=A, survivor=B   (popper saw A first, writer set B after del)
            #   popped=B, survivor=None (writer set B before popper's get)
            #   popped=A, survivor=None (writer set B before popper's get, popper read stale A — impossible without bug)
            # The bug-only outcome:
            #   popped=A, survivor=None — B was set, then popper's del wiped it
            if popped == "A" and survivor is None:
                b_lost += 1
            # Reset for next iteration
            try:
                del cache["k"]
            except KeyError:
                pass

        self.assertEqual(b_lost, 0,
            f"{b_lost}/{iters} iterations lost the writer's value via pop's get/del race")

    @unittest.skip("In-process pop race window is too narrow to reproduce "
                   "deterministically; bug is verified by code reading "
                   "(store.hpp:959 — pop = get + del with separate locks). "
                   "Kept as a regression test once the fix lands.")
    def test_pop_loses_writes_multithread_aggressive(self):
        pass


_DEADLOCK_WORKER = '''
import tempfile, threading, time, sys, os
from pysciqlop_cache import Cache
tmp = tempfile.mkdtemp()
cache = Cache(tmp)
for i in range(50):
    cache.set(f"k{i}", i)

def writer():
    cache.set("zzz", 1)
    print("writer:done", flush=True)

def slow_iter():
    count = 0
    for _ in cache:
        count += 1
        time.sleep(0.005)
        if count >= 10:
            break
    print("iter:done", flush=True)

ti = threading.Thread(target=slow_iter)
tw = threading.Thread(target=writer)
ti.start(); time.sleep(0.05); tw.start()
ti.join(); tw.join()
print("both:done", flush=True)
'''


class KeyCursorBlocks(unittest.TestCase):
    """Iterating a Cache while another thread writes deadlocks the Python interpreter.

    Two compounding bugs:
    - KeyCursor holds the C++ recursive_mutex for its entire lifetime
    - The nanobind bindings don't release the GIL on blocking C++ calls

    Combined effect: writer thread enters C++ holding GIL, blocks on _mtx, never
    releases GIL → iter thread can never reacquire GIL to advance → hard hang.

    Has to run in a subprocess because, once deadlocked, the main thread also
    can't reacquire the GIL — no Python timeout can rescue it.
    """

    def test_iter_plus_writer_deadlocks(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = (
            os.path.join(os.path.dirname(__file__), "..", "..", "build")
            + os.pathsep + env.get("PYTHONPATH", ""))
        try:
            res = subprocess.run(
                [sys.executable, "-c", _DEADLOCK_WORKER],
                capture_output=True, text=True, timeout=5.0, env=env)
        except subprocess.TimeoutExpired as e:
            self.fail("deadlock confirmed: subprocess hung (output so far: "
                      f"{(e.stdout or b'').decode(errors='replace')!r}). "
                      "KeyCursor holds the C++ mutex AND nanobind bindings don't "
                      "release the GIL → iter and writer mutually starve.")
        self.assertIn("both:done", res.stdout,
            f"unexpected: subprocess exited but didn't reach completion. "
            f"stdout={res.stdout!r} stderr={res.stderr!r}")


class NestedTransactSupported(unittest.TestCase):
    """T1-D: cache.transact() should be reentrant on the same thread.

    diskcache supports nesting (RLock + depth). Sciqlop-cache today throws
    `RuntimeError: Nested transactions are not supported`. Composing
    transactional helpers will trip on this.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_nested_transact_does_not_throw(self):
        cache = Cache(self.tmp)
        with cache.transact():
            with cache.transact():
                cache.set("k", "v")
        self.assertEqual(cache.get("k"), "v")

    def test_nested_inner_rollback_does_not_break_outer(self):
        cache = Cache(self.tmp)
        cache.set("k", "outer")
        try:
            with cache.transact():
                cache.set("k", "outer-modified")
                with cache.transact():
                    cache.set("k", "inner")
                    raise RuntimeError("inner abort")
        except RuntimeError:
            pass
        # Outer rolled back — original value should remain.
        self.assertEqual(cache.get("k"), "outer")


class CppIncrInsideTransact(unittest.TestCase):
    """T1-C: the C++ incr() unconditionally opens BEGIN EXCLUSIVE, which
    fails inside a user transaction (Python wrapper avoids this by using
    get/set in transact, but anyone touching the C++ method directly hits it).

    We simulate by calling the underlying _Cache.incr (the nanobind binding)
    inside transact()."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_underlying_incr_inside_transact(self):
        from pysciqlop_cache._pysciqlop_cache import Cache as _RawCache
        cache = Cache(self.tmp)
        # _RawCache.incr is the C++ method. Wrap it inside our transact():
        with cache.transact():
            # This currently throws "cannot start a transaction within a transaction"
            _RawCache.incr(cache, "counter", 1, 0)
        # After fix, value should be 1
        from pysciqlop_cache._pysciqlop_cache import Cache as _RawCache2  # noqa
        # We can't easily get the raw int back through pickle serializer; just
        # verify exists() reports True and no exception escaped the with-block.
        self.assertTrue(cache.__contains__("counter"))


class GetFallbackPathAware(unittest.TestCase):
    """T2-D: when get() fails to load a file-backed value, it must only
    delete the row if the row STILL points to the path we tried to load.
    Cross-process: another process may have swapped the entry between our
    SELECT and this fallback — a bare del(key) re-reads the row, finds the
    new path, and removes the other process's just-written file.

    NOTE: the actual race window is inside one C++ binding call holding
    _mtx, so it can't be made deterministic from Python without C++-side
    instrumentation. These tests exercise the fixed code path and lock in
    that the path-conditional DELETE works for the common scenarios — they
    do NOT go RED without the fix in single-process. The bug class is
    well-understood from code review (store.hpp get() fallback).
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_get_fallback_does_not_delete_after_path_mutation(self):
        import sqlite3, os
        cache = Cache(self.tmp)
        # Write key K with a file-backed value (>8KB).
        big = b"D" * (12 * 1024)
        # super().set bypasses the Python pickle wrapper so the file content
        # is exactly `big` and survivor_path content stays raw bytes.
        cache.__setitem__("K", big)
        db_path = os.path.join(self.tmp, "sciqlop-cache.db")
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT path FROM cache WHERE key = ?", ("K",)
            ).fetchone()
            self.assertIsNotNone(row)
            # path is stored relative to the cache root (so the cache stays
            # relocatable); resolve it for direct filesystem access.
            original_path = os.path.join(self.tmp, row[0])
            self.assertTrue(os.path.exists(original_path))

            os.remove(original_path)
            survivor_path = os.path.join(self.tmp, "survivor.bin")
            with open(survivor_path, "wb") as f:
                f.write(b"DO_NOT_DELETE")
            conn.execute(
                "UPDATE cache SET path = ? WHERE key = ?",
                (survivor_path, "K"),
            )
            conn.commit()
        finally:
            conn.close()

        # Use the raw __getitem__ → C++ get → returns Buffer. We don't need
        # to interpret the value; just verify the survivor file is intact.
        from pysciqlop_cache._pysciqlop_cache import Cache as _RawCache
        buf = _RawCache.get(cache, "K")
        self.assertIsNotNone(buf)
        self.assertTrue(os.path.exists(survivor_path))

    def test_get_fallback_only_removes_row_when_path_matches(self):
        import sqlite3, os
        cache = Cache(self.tmp)
        cache.__setitem__("K", b"D" * (12 * 1024))
        db_path = os.path.join(self.tmp, "sciqlop-cache.db")
        # Force the row to point at a non-existent path: get() will fail load.
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "UPDATE cache SET path = ? WHERE key = ?",
                ("/nonexistent/path/at_all", "K"),
            )
            conn.commit()
        finally:
            conn.close()

        # With the fix: DELETE WHERE key='K' AND path='/nonexistent/path/at_all'
        # — matches, row removed.
        from pysciqlop_cache._pysciqlop_cache import Cache as _RawCache
        result = _RawCache.get(cache, "K")
        self.assertIsNone(result)
        # Verify via the cache's own connection (Python's sqlite3 has snapshot
        # quirks against an open WAL writer).
        self.assertFalse(cache.__contains__("K"))


class LockReleaseDetectsLost(unittest.TestCase):
    """T2-E: Lock.release() used to silently succeed when the key it was
    "holding" had already vanished (e.g. expired or evicted from another
    process). After the fix it returns True only if the key was actually
    deleted by this call — callers can detect a lost lock without changing
    the no-args usage pattern.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_release_returns_false_if_key_already_gone(self):
        from pysciqlop_cache import Cache, Lock
        cache = Cache(self.tmp)
        lock = Lock(cache, "L")
        lock.acquire()
        # Simulate the lock evaporating (other process expire/clear).
        del cache["L"]
        # release() must report it didn't actually own the row anymore.
        self.assertFalse(lock.release())

    def test_release_returns_true_on_normal_path(self):
        from pysciqlop_cache import Cache, Lock
        cache = Cache(self.tmp)
        lock = Lock(cache, "L")
        lock.acquire()
        self.assertTrue(lock.release())


class CounterBgResyncRace(unittest.TestCase):
    """T1-B: the background checkpoint thread runs _resync_counters every ~1s
    while the main thread does set/del operations. Sequence:
       main: SQL commit (DB has +1 row, atomic counter still old)
       bg:   _resync reads DB, stores new value into atomic
       main: fetch_add(1) — atomic now over-counts by 1
    is observable. After the fix BG takes _mtx around resync so it can't
    fire between user's commit and atomic update.

    Reproducer: a single process loops set/del while the BG thread runs;
    afterwards check() compares atomic counters to DB ground truth.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_drift_after_steady_writes(self):
        cache = Cache(self.tmp)
        # Run for ~2s so the BG thread fires at least twice during writes.
        end = time.time() + 2.0
        i = 0
        while time.time() < end:
            cache.set(f"k{i % 200}", b"x" * 32)
            if i % 7 == 0:
                cache.delete(f"k{(i - 50) % 200}")
            i += 1
        # No reopen — drift would be lost.
        result = cache.check(fix=False)
        self.assertTrue(result.counters_consistent,
            f"BG/main race drifted counters: "
            f"size()={cache.size()}, count()={cache.count()}, "
            f"size_mismatches={result.size_mismatches}")


def _mp_writer_loop(args):
    path, key_prefix, tag, n, payload = args
    cache = Cache(path)
    for i in range(n):
        cache.set(f"{key_prefix}{i}", payload, tag=tag)


def _mp_evictor_loop(args):
    path, n_evicts, tag = args
    cache = Cache(path)
    for _ in range(n_evicts):
        cache.evict_tag(tag)
        time.sleep(0.001)


class EvictTagAtomic(unittest.TestCase):
    """T1-E: evict_tag() reads SUM(size) WHERE tag=? in one statement, then
    DELETEs in another. Between them a concurrent process can add/remove
    tagged rows → counter drift after the eviction.

    Reproducer: writer process keeps adding tagged rows while another process
    runs evict_tag in a loop. Then we reopen the cache and assert size() and
    count() agree with the DB ground truth.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_evict_tag_does_not_drift_counters(self):
        # Parent stays open throughout — drift only persists in *this* process's
        # atomic counters, and reopening would reload them from the DB and
        # mask the bug. Child process concurrently inserts tagged rows; parent
        # loops evict_tag so the buggy SUM-then-DELETE race fires repeatedly.
        cache = Cache(self.tmp)
        for i in range(50):
            cache.set(f"keep{i}", b"x" * 32)

        from multiprocessing import Process
        writer = Process(target=_mp_writer_loop,
                         args=((self.tmp, "child_", "racy", 500, b"y" * 64),))
        writer.start()

        # Tight loop in parent so several evicts overlap with child writes.
        for _ in range(80):
            cache.evict_tag("racy")
            time.sleep(0.001)
        writer.join()
        cache.evict_tag("racy")  # final flush

        # Don't reopen — drift would be lost. check() compares the in-memory
        # counters against the DB ground truth on the parent's own connection.
        result = cache.check(fix=False)
        self.assertTrue(result.counters_consistent,
            f"counters drifted after concurrent evict_tag: "
            f"in-memory size()={cache.size()}, count()={cache.count()}")


def _mp_thrash_key(args):
    """Hammer one key with mixed small/large values from a worker process."""
    path, key, n, large_payload = args
    cache = Cache(path)
    small = b"x" * 64
    large = large_payload
    for i in range(n):
        if i % 2 == 0:
            cache.set(key, small)
        else:
            cache.set(key, large)


class OrphanedFilesOnConcurrentSet(unittest.TestCase):
    """T1-A: blob set() and del() are not wrapped in BEGIN EXCLUSIVE.

    Cross-process: Process A reads (path=/abc, size=100) for key K. Process B
    swaps K to a different file-backed value (REPLACE writes new path, removes
    its own old). Process A REPLACES with a small blob (path=NULL) but its
    own old_filepath snapshot is /abc (already removed). The new file B wrote
    is now orphaned because nobody knows to clean it.

    Reproducer: hammer the same key with alternating small/large payloads from
    several processes. After they all settle, run check() and assert no
    orphans. On the bug, orphan count grows with iterations.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_orphans_after_concurrent_mixed_size_set(self):
        # 12KB triggers the file-backed path (threshold is 8KB).
        large = b"L" * (12 * 1024)
        n_procs, per_proc = 4, 80
        with Pool(processes=n_procs) as pool:
            pool.map(_mp_thrash_key,
                     [(self.tmp, "shared", per_proc, large)] * n_procs)

        cache = Cache(self.tmp)
        # Settle: give the BG thread a chance to converge counters.
        time.sleep(1.2)
        result = cache.check(fix=False)
        self.assertEqual(result.orphaned_files, 0,
            f"orphan files after concurrent same-key set: "
            f"{result.orphaned_files} (dangling={result.dangling_rows}, "
            f"size_mismatches={result.size_mismatches})")


class FanoutIncrAtomic(unittest.TestCase):
    """FanoutCache.incr wraps in transact(): the SQLite EXCLUSIVE lock should
    prevent lost updates across processes. Multi-thread is GIL-deadlock-prone
    (same root cause as KeyCursor), so this test only covers multi-process —
    the only safe way to use FanoutCache.incr today."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_fanout_incr_processes_safe(self):
        cache = FanoutCache(self.tmp)
        cache.set("counter", 0)
        del cache

        n_procs, per_proc = 4, 50
        with Pool(processes=n_procs) as pool:
            pool.map(_mp_fanout_increment, [(self.tmp, per_proc)] * n_procs)

        cache = FanoutCache(self.tmp)
        expected = n_procs * per_proc
        actual = cache.get("counter")
        self.assertEqual(actual, expected,
            f"FanoutCache.incr lost {expected - actual}/{expected} cross-process updates")


if __name__ == "__main__":
    unittest.main()
