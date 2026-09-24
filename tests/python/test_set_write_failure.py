"""Reproducer for issue #16: set() reported success when the value file
could not be written, silently losing the value.

A soft RLIMIT_FSIZE below the value size makes write() on the blob file fail
with EFBIG -- the same failure path as a full disk or an exceeded quota.
"""

import os
import shutil
import tempfile
import unittest

from pysciqlop_cache import Cache, FanoutCache, FanoutIndex, Index

try:
    import resource
    import signal
except ImportError:
    resource = None

LIMIT = 1 << 20
VALUE = b"X" * (2 * LIMIT)  # file-backed, and larger than the size limit


class _FileSizeLimit:
    def __enter__(self):
        self.old_limit = resource.getrlimit(resource.RLIMIT_FSIZE)
        self.old_handler = signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        resource.setrlimit(resource.RLIMIT_FSIZE, (LIMIT, self.old_limit[1]))

    def __exit__(self, *exc):
        resource.setrlimit(resource.RLIMIT_FSIZE, self.old_limit)
        signal.signal(signal.SIGXFSZ, self.old_handler)


@unittest.skipIf(resource is None, "POSIX-only: relies on RLIMIT_FSIZE")
class SetWriteFailure(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _assert_raises_and_stores_nothing(self, store, op):
        with _FileSizeLimit():
            with self.assertRaises(OSError):
                op(store)
        self.assertNotIn("k", store)

    def test_set_raises(self):
        for cls in (Cache, FanoutCache, Index, FanoutIndex):
            with self.subTest(cls=cls.__name__):
                store = cls(os.path.join(self.tmp, cls.__name__))
                self._assert_raises_and_stores_nothing(store, lambda s: s.set("k", VALUE))

    def test_add_raises_instead_of_returning_false(self):
        for cls in (Cache, FanoutCache, Index, FanoutIndex):
            with self.subTest(cls=cls.__name__):
                store = cls(os.path.join(self.tmp, cls.__name__))
                self._assert_raises_and_stores_nothing(store, lambda s: s.add("k", VALUE))

    def test_store_still_usable_after_failure(self):
        cache = Cache(os.path.join(self.tmp, "c"))
        with _FileSizeLimit():
            with self.assertRaises(OSError):
                cache.set("k", VALUE)
        cache.set("k", VALUE)
        self.assertEqual(cache.get("k"), VALUE)


if __name__ == "__main__":
    unittest.main()
