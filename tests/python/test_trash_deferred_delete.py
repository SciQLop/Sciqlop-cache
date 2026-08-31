"""Reproducers for the cross-process REPLACE-vs-reader TOCTOU on blob files.

A writer that REPLACEs a file-backed value used to unlink the displaced file
immediately after commit, while a reader in another process could still be
between "SELECT path" and "open()" — turning a live key into a spurious miss
(docs/known-issues/pool-reuse-fork-safety-gap.md). The fix defers deletion:
displaced files go into a `trash` table (same transaction as the row change)
and the background thread unlinks them only after a grace period.

These tests pin the new file-lifecycle contract; the lifecycle tests are
deterministic RED before the fix and GREEN after.
"""

import multiprocessing
import os
import shutil
import tempfile
import time
import unittest

from pysciqlop_cache import Cache

BIG_A = b"A" * 100_000
BIG_B = b"B" * 100_000
GRACE_DEADLINE = 20  # trash grace (5s) + bg tick (1s) + slow-runner margin


def _data_files(root):
    """All regular files under the cache dir except the SQLite db files."""
    found = set()
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.startswith("sciqlop-cache.db"):
                continue
            found.add(os.path.join(dirpath, name))
    return found


def _wait_until(predicate, deadline_secs):
    deadline = time.monotonic() + deadline_secs
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return predicate()


def _hammer_writer(args):
    path, stop_at = args
    cache = Cache(path)
    while time.monotonic() < stop_at:
        cache.set("K", BIG_A)
        cache.set("K", BIG_B)


def _hammer_reader(args):
    path, stop_at = args
    cache = Cache(path)
    misses = 0
    while time.monotonic() < stop_at:
        if cache.get("K") is None:
            misses += 1
    return misses


class ReplaceDeferredDeletion(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_replace_defers_old_file_deletion_past_grace(self):
        """The displaced file must survive the REPLACE (grace window), then be
        removed by the background drain — never unlinked inline."""
        cache = Cache(self.tmp)
        cache.set("K", BIG_A)
        old_files = _data_files(self.tmp)
        self.assertEqual(len(old_files), 1)

        cache.set("K", BIG_B)
        new_files = _data_files(self.tmp) - old_files
        self.assertEqual(len(new_files), 1, "REPLACE must write a fresh file")
        self.assertTrue(
            old_files <= _data_files(self.tmp),
            "displaced file was unlinked inline — reader TOCTOU window is open",
        )
        self.assertEqual(cache.get("K"), BIG_B)

        self.assertTrue(
            _wait_until(lambda: _data_files(self.tmp) == new_files, GRACE_DEADLINE),
            f"trash not drained: {_data_files(self.tmp) - new_files} still on disk",
        )
        self.assertEqual(cache.get("K"), BIG_B)

    def test_shrink_to_blob_defers_old_file_deletion(self):
        """file -> blob REPLACE displaces a file too; same deferred contract."""
        cache = Cache(self.tmp)
        cache.set("K", BIG_A)
        old_files = _data_files(self.tmp)
        self.assertEqual(len(old_files), 1)

        cache.set("K", b"small")
        self.assertEqual(
            _data_files(self.tmp), old_files,
            "displaced file was unlinked inline — reader TOCTOU window is open",
        )
        self.assertEqual(cache.get("K"), b"small")

        self.assertTrue(
            _wait_until(lambda: not _data_files(self.tmp), GRACE_DEADLINE),
            "trash not drained after shrink-to-blob",
        )

    def test_check_treats_pending_trash_as_known(self):
        """A trashed-but-not-yet-drained file is not an orphan, and
        check(fix=True) must not unlink it early."""
        cache = Cache(self.tmp)
        cache.set("K", BIG_A)
        old_files = _data_files(self.tmp)
        cache.set("K", BIG_B)

        result = cache.check(fix=True)
        self.assertEqual(result.orphaned_files, 0)
        self.assertTrue(result.ok)
        self.assertTrue(
            old_files <= _data_files(self.tmp),
            "check(fix=True) unlinked a trash file inside its grace window",
        )

    def test_cpp_incr_on_file_backed_key_leaks_no_file(self):
        """C++ incr() rewrites a file-backed row to a blob (path = NULL); the
        displaced file must be trashed and drained, not leaked forever."""
        from pysciqlop_cache._pysciqlop_cache import Cache as _RawCache

        cache = Cache(self.tmp)
        _RawCache.set(cache, "K", BIG_A)
        self.assertEqual(len(_data_files(self.tmp)), 1)

        _RawCache.incr(cache, "K", 1, 0)
        self.assertTrue(
            _wait_until(lambda: not _data_files(self.tmp), GRACE_DEADLINE),
            "file displaced by incr() was never removed (orphan leak)",
        )
        self.assertEqual(cache.check().orphaned_files, 0)

    def test_no_spurious_miss_under_replace_hammer(self):
        """Regression guard: dedicated writer + reader processes hammering one
        file-backed key must never observe a miss for a key that always
        exists."""
        cache = Cache(self.tmp)
        cache.set("K", BIG_A)
        del cache

        ctx = multiprocessing.get_context("fork")
        stop_at = time.monotonic() + 3.0
        with ctx.Pool(processes=3) as pool:
            writers = pool.map_async(_hammer_writer, [(self.tmp, stop_at)] * 2)
            misses = pool.apply(_hammer_reader, ((self.tmp, stop_at),))
            writers.get(timeout=60)
        self.assertEqual(misses, 0, f"{misses} spurious misses on a live key")


if __name__ == "__main__":
    unittest.main()
