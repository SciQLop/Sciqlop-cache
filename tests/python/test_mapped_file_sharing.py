"""A live Buffer maps its value's file. That must not break other operations.

On Windows a mapped file used to be opened with share mode 0 (exclusive):
- deleting the key while a Buffer was alive failed, leaving an orphan file;
- a second store instance could not open the file at all, so get() missed.
Linux allows both, so these only fail on Windows without the fix.
"""

import os
import time
import unittest
from tempfile import TemporaryDirectory

from pysciqlop_cache._pysciqlop_cache import Cache as RawCache

BIG = os.urandom(64 * 1024)  # above the 8 KB blob threshold: stored as a file


def value_files(root):
    return [os.path.join(d, f) for d, _, files in os.walk(root) for f in files
            if not f.startswith("sciqlop-cache.db")]


class DeleteWhileMapped(unittest.TestCase):
    def test_delete_with_live_buffer_leaves_no_orphan(self):
        with TemporaryDirectory() as tmp:
            c = RawCache(tmp)
            c.set("big", BIG)
            buf = c.get("big")
            c.delete("big")
            self.assertTrue(c.check().ok, "file left behind with no row and no trash entry")
            del buf
            deadline = time.monotonic() + 15  # 5 s trash grace + 1 s bg tick, with margin
            while value_files(tmp) and time.monotonic() < deadline:
                time.sleep(0.5)
            self.assertEqual(value_files(tmp), [], "file never unlinked once the buffer was released")
            c.close()


@unittest.skipIf(os.name == "nt" or os.geteuid() == 0, "needs POSIX directory permissions, non-root")
class FailedUnlinkIsRetried(unittest.TestCase):
    """Portable stand-in for the Windows case: a read-only directory makes unlink fail."""

    def test_file_is_queued_then_removed_once_deletable(self):
        with TemporaryDirectory() as tmp:
            c = RawCache(tmp)
            c.set("big", BIG)
            (path,) = value_files(tmp)
            os.chmod(os.path.dirname(path), 0o555)
            try:
                c.delete("big")
                self.assertTrue(c.check().ok, "failed unlink orphaned the file")
            finally:
                os.chmod(os.path.dirname(path), 0o755)
            deadline = time.monotonic() + 15
            while value_files(tmp) and time.monotonic() < deadline:
                time.sleep(0.5)
            self.assertEqual(value_files(tmp), [], "queued file never retried")
            c.close()


class ConcurrentMappedReaders(unittest.TestCase):
    def test_second_store_reads_while_first_holds_buffer(self):
        with TemporaryDirectory() as tmp:
            a, b = RawCache(tmp), RawCache(tmp)
            a.set("big", BIG)
            held = a.get("big")
            other = b.get("big")
            self.assertIsNotNone(other, "second reader missed while the file was mapped")
            self.assertEqual(bytes(other.memoryview()), BIG)
            del held, other
            a.close()
            b.close()


if __name__ == "__main__":
    unittest.main()
