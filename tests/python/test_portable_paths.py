"""A cache directory copied between Windows and Linux/macOS must still resolve
its value files.

Value files live in sub-folders, and each row stores the file's path relative
to the cache root. Windows used to store it with backslashes ("ab\\cd\\file"),
which Linux and macOS read as one file name, so every file-backed value of a
cache written on Windows was missing elsewhere (diskcache #304 has the same bug).
"""

import gc
import os
import shutil
import sqlite3
import tempfile
import unittest

from pysciqlop_cache import Cache, Index

BIG = b"x" * 100_000  # above the 8 KB blob threshold: stored as a file


def _stored_paths(cache_dir):
    con = sqlite3.connect(os.path.join(cache_dir, "sciqlop-cache.db"))
    try:
        return [p for (p,) in con.execute("SELECT path FROM cache WHERE path IS NOT NULL")]
    finally:
        con.close()


def _rewrite_paths_windows_style(cache_dir):
    con = sqlite3.connect(os.path.join(cache_dir, "sciqlop-cache.db"))
    try:
        con.execute("UPDATE cache SET path = REPLACE(path, '/', '\\') WHERE path IS NOT NULL")
        con.commit()
    finally:
        con.close()


class PortablePaths(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        gc.collect()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _close(self, store):
        # Our SQLite connection must be closed before Python's sqlite3 opens
        # the same file (see CLAUDE.md: a second SQLite library in one process).
        store.close()
        del store
        gc.collect()

    def test_new_rows_use_forward_slashes(self):
        for cls in (Cache, Index):
            with self.subTest(cls=cls.__name__):
                d = os.path.join(self.tmp, cls.__name__)
                store = cls(d)
                store["k"] = BIG
                self._close(store)
                paths = _stored_paths(d)
                self.assertEqual(len(paths), 1)
                self.assertNotIn("\\", paths[0])

    def test_reads_rows_written_on_windows(self):
        for cls in (Cache, Index):
            with self.subTest(cls=cls.__name__):
                d = os.path.join(self.tmp, cls.__name__)
                store = cls(d)
                store["k"] = BIG
                self._close(store)
                _rewrite_paths_windows_style(d)

                store = cls(d)
                self.assertEqual(store["k"], BIG)
                self.assertTrue(store.check().ok)
                del store["k"]
                self.assertNotIn("k", store)


if __name__ == "__main__":
    unittest.main()
