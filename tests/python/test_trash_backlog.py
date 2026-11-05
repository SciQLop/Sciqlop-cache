"""Issue #31: sustained overwrites of file values must not grow the trash forever.

Every overwrite of a file-backed value queues the old file for deletion after a
5 s grace period. The background thread used to delete them inside a write
transaction, at most 512 per 1 s tick, and lost the write lock to a busy
writer, so a tight set() loop outran it: the files piled up on disk without
bound. It now unlinks them with no lock held, and only takes the lock briefly
to drop their trash rows.
"""

import multiprocessing
import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from pysciqlop_cache import Cache

VALUE = os.urandom(9 * 1024)  # above the 8 KB blob threshold: stored as a file
# Well above what a 512-files-per-tick drain manages, but throttled so the
# unfixed run stays around 150 MB of files.
WRITES_PER_SEC = 1500
RUN_SECS = 12
# Grace (5 s) + bg tick (1 s) + timestamp rounding: a file written longer ago
# than this should be gone.
WINDOW_SECS = 8
DRAIN_DEADLINE = 20  # grace + tick + slow-runner margin

SHARED_KEYS = [f"k{i}" for i in range(20)]


def _value_paths(root):
    return [
        os.path.join(d, name)
        for d, _, names in os.walk(root)
        for name in names
        if not name.startswith("sciqlop-cache.db")
    ]


def _value_files(root):
    return len(_value_paths(root))


def _wait_until(predicate, deadline_secs):
    deadline = time.monotonic() + deadline_secs
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return predicate()


def _trash_rows(path):
    # Only after the cache is closed: a second SQLite library on a live DB
    # can truncate our -shm mapping (see CLAUDE.md).
    with sqlite3.connect(os.path.join(path, "sciqlop-cache.db")) as db:
        return db.execute("SELECT count(*) FROM trash").fetchone()[0]


def _hold_write_lock(path, locked, release):
    cache = Cache(path)
    with cache.transact():
        locked.set()
        release.wait(60)
    cache.close()


def _payload(key):
    return key.encode() + b"|" + os.urandom(9 * 1024)


def _overwrite_and_verify(path, stop_at):
    """Overwrite shared keys and read them back; returns the number of bad reads.

    Every key is set before the workers start and never deleted, so a miss or
    a value for another key means a live file was unlinked.
    """
    cache = Cache(path)
    bad = 0
    i = 0
    while time.monotonic() < stop_at:
        key = SHARED_KEYS[i % len(SHARED_KEYS)]
        cache.set(key, _payload(key))
        other = SHARED_KEYS[(i * 7 + 3) % len(SHARED_KEYS)]
        value = cache.get(other)
        if value is None or not bytes(value).startswith(other.encode() + b"|"):
            bad += 1
        i += 1
    cache.close()
    return bad


class TrashBacklog(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_disk_files_track_recent_writes_not_total_writes(self):
        cache = Cache(self.path)
        writes_per_second = []
        start = time.monotonic()
        for second in range(1, RUN_SECS + 1):
            writes = 0
            while writes < WRITES_PER_SEC and time.monotonic() - start < second:
                cache.set("k", VALUE)
                writes += 1
            time.sleep(max(0.0, second - (time.monotonic() - start)))
            writes_per_second.append(writes)
        files = _value_files(self.path)
        cache.close()

        recent = sum(writes_per_second[-WINDOW_SECS:])
        total = sum(writes_per_second)
        self.assertLessEqual(
            files, recent + 256,
            f"{files} files on disk for 1 live value; {total} writes in "
            f"{RUN_SECS} s, {recent} in the last {WINDOW_SECS} s",
        )

    def test_files_are_reclaimed_while_another_process_holds_the_write_lock(self):
        cache = Cache(self.path)
        for _ in range(300):
            cache.set("k", VALUE)

        ctx = multiprocessing.get_context("spawn")
        locked, release = ctx.Event(), ctx.Event()
        holder = ctx.Process(target=_hold_write_lock, args=(self.path, locked, release))
        holder.start()
        try:
            self.assertTrue(locked.wait(30), "lock holder never started")
            reclaimed = _wait_until(lambda: _value_files(self.path) == 1, DRAIN_DEADLINE)
            still_locked = holder.is_alive() and not release.is_set()
        finally:
            release.set()
            holder.join(30)
        self.assertTrue(still_locked)
        self.assertTrue(reclaimed, f"{_value_files(self.path)} files left, 1 live value")

        self.assertEqual(bytes(cache.get("k")), VALUE)
        cache.close()  # final drain drops the rows the busy passes left behind
        self.assertEqual(_trash_rows(self.path), 0)

    def test_concurrent_drains_never_unlink_a_live_file(self):
        cache = Cache(self.path)
        for key in SHARED_KEYS:
            cache.set(key, _payload(key))

        ctx = multiprocessing.get_context("spawn")
        stop_at = time.monotonic() + 10  # > grace, so drains run during the writes
        with ctx.Pool(3) as pool:
            bad_reads = pool.starmap(_overwrite_and_verify, [(self.path, stop_at)] * 3)
        self.assertEqual(bad_reads, [0, 0, 0])

        for key in SHARED_KEYS:
            self.assertTrue(bytes(cache.get(key)).startswith(key.encode() + b"|"), key)
        self.assertTrue(
            _wait_until(lambda: _value_files(self.path) == len(SHARED_KEYS), DRAIN_DEADLINE),
            f"{_value_files(self.path)} files left, {len(SHARED_KEYS)} live values",
        )
        report = cache.check()
        self.assertTrue(report.ok, report)
        self.assertEqual(report.dangling_rows, 0)
        self.assertEqual(report.orphaned_files, 0)
        cache.close()


class DrainSafety(unittest.TestCase):
    """Each test pins one rule the lock-free drain relies on (mutation-checked)."""

    def setUp(self):
        self.path = tempfile.mkdtemp()
        self.cache = Cache(self.path)
        self.cache.set("k", VALUE)
        (self.file,) = _value_paths(self.path)

    def tearDown(self):
        self.cache.close()
        os.chmod(os.path.dirname(self.file), 0o755)
        shutil.rmtree(self.path, ignore_errors=True)

    def test_a_deleted_file_outlives_the_grace_period(self):
        self.cache.delete("k")
        time.sleep(2.5)  # > 2 bg ticks, < the 4 s minimum grace (whole-second timestamps)
        self.assertTrue(os.path.exists(self.file), "file unlinked before the grace period")
        self.assertTrue(_wait_until(lambda: not os.path.exists(self.file), DRAIN_DEADLINE))

    @unittest.skipIf(os.name == "nt" or os.geteuid() == 0, "needs POSIX directory permissions, non-root")
    def test_a_file_the_drain_cannot_delete_stays_queued(self):
        os.chmod(os.path.dirname(self.file), 0o555)
        self.cache.delete("k")
        time.sleep(WINDOW_SECS)  # the drain has tried, and failed, to unlink it
        report = self.cache.check()
        self.assertEqual(report.orphaned_files, 0, "queue entry dropped for a file still on disk")
        os.chmod(os.path.dirname(self.file), 0o755)
        self.assertTrue(
            _wait_until(lambda: not os.path.exists(self.file), DRAIN_DEADLINE),
            "file never retried once deletable",
        )

    def test_a_file_another_process_deleted_leaves_the_queue(self):
        self.cache.delete("k")
        os.remove(self.file)  # another process's drain won the race
        time.sleep(WINDOW_SECS)
        self.cache.close()
        self.assertEqual(_trash_rows(self.path), 0, "queue entry kept for a file already gone")


if __name__ == "__main__":
    unittest.main()
