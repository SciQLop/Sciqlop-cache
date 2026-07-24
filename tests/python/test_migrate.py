"""Tests for pysciqlop_cache.migrate -- in particular its resilience to legacy
entries that fail to deserialize (e.g. pickled by an incompatible library version,
most commonly a different numpy major version)."""
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

import diskcache

from pysciqlop_cache.migrate import (
    InsufficientDiskSpaceError,
    _iter_diskcache_entries,
    _UNREADABLE,
    migrate,
)


class IterDiskcacheEntries(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="migrate_test_")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def _corrupt_value(self, key):
        db = os.path.join(self.root, "cache.db")
        con = sqlite3.connect(db)
        con.execute("UPDATE Cache SET value=? WHERE key=?", (b"not a valid pickle blob!!", key))
        con.commit()
        con.close()

    def test_yields_every_readable_entry(self):
        cache = diskcache.Cache(self.root)
        cache["a"] = 1
        cache["b"] = 2
        cache.close()

        entries = list(_iter_diskcache_entries(diskcache.Cache(self.root)))
        by_key = {key: value for key, value, ttl, tag in entries}
        self.assertEqual(by_key, {"a": 1, "b": 2})

    def test_skips_an_entry_that_cannot_be_deserialized_instead_of_raising(self):
        """One unreadable entry must not abort iteration of every entry after it --
        this reproduces the exact real-world case: a legacy cache holding a value
        pickled under one numpy major version, read back under another."""
        cache = diskcache.Cache(self.root)
        cache["good_before"] = 1
        cache["poisoned"] = {"a": 1}
        cache["good_after"] = 2
        cache.close()

        self._corrupt_value("poisoned")

        entries = list(_iter_diskcache_entries(diskcache.Cache(self.root)))
        by_key = {key: value for key, value, ttl, tag in entries}

        self.assertEqual(set(by_key), {"good_before", "poisoned", "good_after"})
        self.assertEqual(by_key["good_before"], 1)
        self.assertEqual(by_key["good_after"], 2)
        self.assertIs(by_key["poisoned"], _UNREADABLE)


class MigrateSkipsUnreadableEntries(unittest.TestCase):
    """End-to-end: migrate() itself must complete and count the unreadable entry as
    'skipped', not abort the whole migration. Requires the compiled pysciqlop_cache
    extension (unlike IterDiskcacheEntries above, which only needs diskcache)."""

    def setUp(self):
        self.src_root = tempfile.mkdtemp(prefix="migrate_src_")
        self.dst_root = tempfile.mkdtemp(prefix="migrate_dst_")
        self.addCleanup(shutil.rmtree, self.src_root, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.dst_root, ignore_errors=True)

    def test_migration_completes_and_reports_the_unreadable_entry_as_skipped(self):
        cache = diskcache.Cache(self.src_root)
        cache["good_before"] = 1
        cache["poisoned"] = {"a": 1}
        cache["good_after"] = 2
        cache.close()

        db = os.path.join(self.src_root, "cache.db")
        con = sqlite3.connect(db)
        con.execute("UPDATE Cache SET value=? WHERE key=?", (b"not a valid pickle blob!!", "poisoned"))
        con.commit()
        con.close()

        result = migrate(self.src_root, self.dst_root)

        self.assertEqual(result["migrated"], 2)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["errors"], 0)

        from pysciqlop_cache import Cache
        dst = Cache(str(self.dst_root))
        self.assertEqual(dst.get("good_before"), 1)
        self.assertEqual(dst.get("good_after"), 2)
        self.assertIsNone(dst.get("poisoned"))


class MigratePreflightDiskSpaceCheck(unittest.TestCase):
    """migrate() must predict whether it can fit before writing anything, rather than
    getting abandoned half-written partway through when the disk fills up mid-copy."""

    def setUp(self):
        self.src_root = tempfile.mkdtemp(prefix="migrate_src_")
        self.dst_root = tempfile.mkdtemp(prefix="migrate_dst_")
        self.addCleanup(shutil.rmtree, self.src_root, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.dst_root, ignore_errors=True)

    def test_raises_before_writing_anything_when_disk_space_is_insufficient(self):
        cache = diskcache.Cache(self.src_root)
        cache["a"] = "x" * 10_000
        cache.close()

        with mock.patch("pysciqlop_cache.migrate.shutil.disk_usage",
                        return_value=mock.Mock(free=1)):
            with self.assertRaises(InsufficientDiskSpaceError):
                migrate(self.src_root, self.dst_root)

        self.assertEqual(os.listdir(self.dst_root), [],
                         "nothing should be written to the destination when the "
                         "preflight check fails")

    def test_proceeds_normally_when_disk_space_is_sufficient(self):
        cache = diskcache.Cache(self.src_root)
        cache["a"] = 1
        cache.close()

        result = migrate(self.src_root, self.dst_root)  # real disk_usage: plenty of room
        self.assertEqual(result["migrated"], 1)


if __name__ == "__main__":
    unittest.main()
