"""Fork-safety reproducer.

A ``Cache`` constructed in the parent owns a native background checkpoint
thread that periodically takes the store's internal ``_mtx``. POSIX ``fork``
clones only the calling thread but inherits every mutex in its current state,
so if any thread holds ``_mtx`` at the fork instant, a child that later touches
the *inherited* cache object blocks on a lock that no surviving thread will
ever release — a permanent deadlock. This is what hangs Speasy's request
deduplication test, whose workers fork (the default start method on Linux for
Python < 3.14) and reuse the import-time global cache.

To trigger it deterministically without relying on the background thread's
timing, a pool of helper threads hammers the cache with reads, keeping ``_mtx``
held a large fraction of the time (reads only, so no write lock is held across
the fork). Each forked child must still be able to use the inherited cache and
exit promptly; with several forks the unfixed library deadlocks on at least
one of them.
"""

import os
import threading
import time
import tempfile
import unittest

from pysciqlop_cache import Cache


@unittest.skipUnless(hasattr(os, "fork"), "fork() not available on this platform")
class TestForkSafety(unittest.TestCase):

    def test_child_using_inherited_cache_does_not_deadlock(self):
        tmp_dir = tempfile.mkdtemp()
        cache = Cache(os.path.join(tmp_dir, "c"))
        cache.set("k", 0)

        stop = threading.Event()

        def hammer():
            while not stop.is_set():
                cache.get("k")

        threads = [threading.Thread(target=hammer, daemon=True) for _ in range(4)]
        for t in threads:
            t.start()

        try:
            for _ in range(20):
                pid = os.fork()
                if pid == 0:  # child
                    try:
                        cache.incr("k", 1, default=0)
                        os._exit(0)
                    except BaseException:
                        os._exit(2)

                deadline = time.monotonic() + 15
                status = None
                while time.monotonic() < deadline:
                    wpid, st = os.waitpid(pid, os.WNOHANG)
                    if wpid == pid:
                        status = st
                        break
                    time.sleep(0.02)

                if status is None:
                    os.kill(pid, 9)
                    os.waitpid(pid, 0)
                    self.fail("child deadlocked using the fork-inherited cache")
                self.assertTrue(
                    os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0,
                    f"child failed to use inherited cache (status={status})")
        finally:
            stop.set()
            for t in threads:
                t.join(5)


if __name__ == "__main__":
    unittest.main()
