"""Stores created before v0.4.2 have unguarded update counter triggers.

Opening one replaces them, once, with triggers that skip an UPDATE whose counter
would not change (an overwrite with a value of the same size): every overwrite
used to run two no-op UPDATEs on meta. The counters must stay exact.
"""

import os
import shutil
import sqlite3
import tempfile
import unittest

from pysciqlop_cache import Cache, Index

UNGUARDED = [
    "DROP TRIGGER cache_size_update",
    "DROP TRIGGER cache_fsize_update",
    "CREATE TRIGGER cache_size_update AFTER UPDATE OF size ON cache BEGIN"
    "   UPDATE meta SET value = value + NEW.size - OLD.size WHERE key = 'size'; END",
    "CREATE TRIGGER cache_fsize_update AFTER UPDATE OF size, path ON cache BEGIN"
    "   UPDATE meta SET value = value"
    "     - (CASE WHEN OLD.path IS NOT NULL THEN OLD.size ELSE 0 END)"
    "     + (CASE WHEN NEW.path IS NOT NULL THEN NEW.size ELSE 0 END)"
    "   WHERE key = 'file_size'; END",
]


def _db(path):
    return os.path.join(path, "sciqlop-cache.db")


GUARDS = {"cache_size_update": "WHEN NEW.size <> OLD.size",
          "cache_fsize_update": "<> (CASE WHEN NEW.path"}


def _guarded(path):
    """Which update triggers carry their WHEN guard, by name."""
    return {name: GUARDS[name] in sql for name, sql in _trigger_sql(path).items()}


def _trigger_sql(path):
    # Only on a closed store: a second SQLite library on a live DB can truncate
    # our -shm mapping (see CLAUDE.md).
    with sqlite3.connect(_db(path)) as db:
        return dict(db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name IN ('cache_size_update', 'cache_fsize_update')"))


class OldStoresGetGuardedUpdateTriggers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _old_store(self, cls):
        path = os.path.join(self.tmp, cls.__name__)
        store = cls(path)
        store.set("blob", b"s" * 100)
        store.set("file", b"f" * 32 * 1024)
        store.close()
        del store
        with sqlite3.connect(_db(path)) as db:
            for sql in UNGUARDED:
                db.execute(sql)
        return path

    def test_reopening_replaces_the_triggers(self):
        for cls in (Cache, Index):
            with self.subTest(cls.__name__):
                path = self._old_store(cls)
                unguarded = {"cache_size_update": False, "cache_fsize_update": False}
                self.assertEqual(_guarded(path), unguarded)
                cls(path).close()
                self.assertEqual(_guarded(path), {name: True for name in unguarded})

    def test_counters_stay_exact_through_every_overwrite_kind(self):
        for cls in (Cache, Index):
            with self.subTest(cls.__name__):
                store = cls(self._old_store(cls))
                for key, value in [("blob", b"s" * 100), ("blob", b"t" * 100), ("blob", b"u" * 50),
                                   ("blob", b"F" * 32 * 1024), ("file", b"g" * 32 * 1024),
                                   ("file", b"h" * 40 * 1024), ("file", b"x" * 10)]:
                    store.set(key, value)
                    self.assertTrue(store.check().ok, (key, len(value)))
                self.assertEqual(len(store), 2)
                store.close()


if __name__ == "__main__":
    unittest.main()
