"""Reproducer for get() deleting a valid row on a transient file-open failure.

Production incident 2026-09-01 (speasy-proxy): the mmap-fd leak in
Buffer.memoryview() exhausted file descriptors, so DiskStorage::load() started
failing to open blob files that were perfectly healthy on disk. get()'s
fallback treated every load failure as "the file is gone" and deleted the
row -- permanently losing ~61k valid cache entries whose files were still
present (and now orphaned, since the delete never unlinks them either).

The fix: only delete the row when the blob file is actually absent from
disk. A transient open failure (e.g. EACCES, EMFILE) with the file still
present must be a soft miss that leaves the row intact.

This test forces a deterministic open failure by chmod'ing a real blob file
to 0o000 (open() then fails with EACCES while the file exists), and asserts
the row survives once permissions are restored.
"""

import os
import shutil
import stat
import tempfile
import unittest

from pysciqlop_cache import Cache

VALUE = b"X" * 16_384  # >8KB => file-backed


def _find_blob_file(root):
    """The single file-backed blob under the cache dir (anything that isn't
    the SQLite index). Serialization framing means the on-disk size isn't
    exactly len(value), so match on "not the db" rather than an exact size.
    """
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.startswith("sciqlop-cache.db"):
                continue
            return os.path.join(dirpath, name)
    return None


@unittest.skipUnless(hasattr(os, "geteuid"), "POSIX-only: relies on file permission bits")
@unittest.skipIf(getattr(os, "geteuid", lambda: -1)() == 0, "root ignores file permission bits")
class GetTransientOpenFailure(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_chmod_0_then_restored_keeps_the_row(self):
        cache = Cache(self.tmp)
        cache.set("K", VALUE)
        # Reopen to defeat the DiskStorage mmap LRU and any warm state left
        # over from set(), so the next get() actually calls open().
        del cache
        cache = Cache(self.tmp)

        blob = _find_blob_file(self.tmp)
        self.assertIsNotNone(blob, "could not locate the file-backed blob on disk")

        os.chmod(blob, 0o000)
        try:
            cache.get("K")  # None (soft miss) or VALUE if still served: both ok
        finally:
            os.chmod(blob, 0o644)

        # Reopen again so the transient failure above can't be papered over
        # by a cached handle from the failed attempt.
        del cache
        cache = Cache(self.tmp)

        self.assertEqual(
            cache.get("K"), VALUE,
            "row was deleted by a transient open failure even though the "
            "file was present on disk the whole time",
        )


if __name__ == "__main__":
    unittest.main()
