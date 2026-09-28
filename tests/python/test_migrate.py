"""Tests for pysciqlop_cache.migrate -- in particular its resilience to legacy
entries that fail to deserialize (e.g. pickled by an incompatible library version,
most commonly a different numpy major version)."""
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

try:
    import diskcache
except ImportError:  # migration needs diskcache, an optional dependency
    diskcache = None


def setUpModule():
    if diskcache is None:
        raise unittest.SkipTest("diskcache not installed")

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


class MigrateMoveMode(unittest.TestCase):
    """drop=True (move instead of copy) deletes each source entry as soon as it's
    migrated, so the preflight estimate should scale with the largest single entry
    rather than the whole source, and the source should end up emptied out."""

    def setUp(self):
        self.src_root = tempfile.mkdtemp(prefix="migrate_src_")
        self.dst_root = tempfile.mkdtemp(prefix="migrate_dst_")
        self.addCleanup(shutil.rmtree, self.src_root, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.dst_root, ignore_errors=True)

    def test_preflight_estimate_is_smaller_in_move_mode(self):
        from pysciqlop_cache.migrate import _ensure_enough_disk_space

        # Two ~200KB values each get externalized by diskcache into their own file,
        # so the whole source is ~400KB+ while the largest single entry is ~200KB --
        # a large enough gap to tell the two estimates apart.
        cache = diskcache.Cache(self.src_root)
        cache["big1"] = "x" * 200_000
        cache["big2"] = "y" * 200_000
        cache.close()

        with mock.patch("pysciqlop_cache.migrate.shutil.disk_usage",
                        return_value=mock.Mock(free=300_000)):
            with self.assertRaises(InsufficientDiskSpaceError):
                _ensure_enough_disk_space(self.src_root, self.dst_root, move=False)
            _ensure_enough_disk_space(self.src_root, self.dst_root, move=True)  # must not raise

    def test_drop_true_empties_the_source_as_entries_migrate(self):
        cache = diskcache.Cache(self.src_root)
        cache["a"] = 1
        cache["b"] = 2
        cache.close()

        result = migrate(self.src_root, self.dst_root, drop=True)

        self.assertEqual(result["migrated"], 2)
        self.assertEqual(list(diskcache.Cache(self.src_root)), [],
                         "source entries must be deleted once successfully migrated")

        from pysciqlop_cache import Cache
        dst = Cache(str(self.dst_root))
        self.assertEqual(dst.get("a"), 1)
        self.assertEqual(dst.get("b"), 2)


class MigrateSourcesAndMetadata(unittest.TestCase):
    """Every source type, and everything an entry carries, arrives intact."""

    def setUp(self):
        self.src = tempfile.mkdtemp(prefix="migrate_src_")
        self.dst = tempfile.mkdtemp(prefix="migrate_dst_")
        self.addCleanup(shutil.rmtree, self.src, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.dst, ignore_errors=True)

    def test_keys_keep_their_type(self):
        cache = diskcache.Cache(self.src)
        cache[42] = "int key"
        cache[("sensor", 7)] = "tuple key"
        cache["text"] = "str key"
        cache.close()

        migrate(self.src, self.dst)

        from pysciqlop_cache import Cache
        dst = Cache(self.dst)
        self.assertEqual(dst[42], "int key")
        self.assertEqual(dst[("sensor", 7)], "tuple key")
        self.assertEqual(dst["text"], "str key")
        self.assertNotIn("42", dst)

    def test_drop_removes_non_str_keys_from_the_source(self):
        cache = diskcache.Cache(self.src)
        cache[42] = "int key"
        cache.close()

        migrate(self.src, self.dst, drop=True)

        self.assertEqual(list(diskcache.Cache(self.src)), [])

    def test_none_values_are_migrated(self):
        cache = diskcache.Cache(self.src)
        cache["nothing"] = None
        cache.close()

        result = migrate(self.src, self.dst)

        from pysciqlop_cache import Cache
        self.assertEqual(result["migrated"], 1)
        self.assertIn("nothing", Cache(self.dst))

    def test_expire_and_tag_are_kept_and_expired_entries_skipped(self):
        cache = diskcache.Cache(self.src)
        cache.set("live", 1, expire=3600, tag="t")
        cache.set("expired", 2, expire=0.01)
        cache.close()
        import time
        time.sleep(0.05)

        result = migrate(self.src, self.dst)

        from pysciqlop_cache import Cache
        dst = Cache(self.dst)
        self.assertEqual(result["migrated"], 1)
        value, expire_time, tag = dst.get("live", expire_time=True, tag=True)
        self.assertEqual((value, tag), (1, "t"))
        self.assertAlmostEqual(expire_time, time.time() + 3600, delta=60)
        self.assertNotIn("expired", dst)

    def test_fanout_cache_source_keeps_its_shard_count(self):
        fanout = diskcache.FanoutCache(self.src, shards=4)
        for i in range(20):
            fanout[f"k{i}"] = i
        fanout.close()

        result = migrate(self.src, self.dst)

        from pysciqlop_cache import FanoutCache
        dst = FanoutCache(self.dst, shard_count=4)
        self.assertEqual(result["migrated"], 20)
        self.assertEqual(dst.shard_count(), 4)
        self.assertEqual(sorted(dst[f"k{i}"] for i in range(20)), list(range(20)))

    def test_the_source_is_left_intact_by_default(self):
        cache = diskcache.Cache(self.src)
        cache["a"] = 1
        cache.close()

        migrate(self.src, self.dst)

        self.assertEqual(diskcache.Cache(self.src)["a"], 1)

    def test_single_shard_fanout_source(self):
        fanout = diskcache.FanoutCache(self.src, shards=1)
        fanout["k"] = "v"
        fanout.close()

        result = migrate(self.src, self.dst)

        from pysciqlop_cache import FanoutCache
        self.assertEqual(result["migrated"], 1)
        self.assertEqual(FanoutCache(self.dst, shard_count=1)["k"], "v")

    def test_drop_empties_an_index_source(self):
        index = diskcache.Index(self.src)
        index["a"] = 1
        index["b"] = 2

        migrate(self.src, self.dst, store_type="index", drop=True)

        self.assertEqual(len(diskcache.Index(self.src)), 0)

    def test_an_entry_that_fails_to_write_is_counted_as_an_error(self):
        from pysciqlop_cache import Cache
        cache = diskcache.Cache(self.src)
        cache["good"] = 1
        cache["bad"] = 2
        cache.close()
        real_set = Cache.set

        def failing_set(store, key, value, **kwargs):
            if key == "bad":
                raise OSError("disk full")
            return real_set(store, key, value, **kwargs)

        with mock.patch.object(Cache, "set", failing_set), mock.patch("sys.stderr"):
            result = migrate(self.src, self.dst)

        self.assertEqual((result["migrated"], result["errors"]), (1, 1))

    def test_index_source(self):
        index = diskcache.Index(self.src)
        index["a"] = [1, 2]
        index["b"] = {"x": 1}

        result = migrate(self.src, self.dst, store_type="index")

        from pysciqlop_cache import Index
        dst = Index(self.dst)
        self.assertEqual(result["migrated"], 2)
        self.assertEqual(dst["a"], [1, 2])
        self.assertEqual(dst["b"], {"x": 1})


class MigratePreflightBoundary(unittest.TestCase):
    def setUp(self):
        self.src = tempfile.mkdtemp(prefix="migrate_src_")
        self.addCleanup(shutil.rmtree, self.src, ignore_errors=True)
        cache = diskcache.Cache(self.src)
        cache["big"] = "x" * 100_000
        cache.close()

    def test_exactly_enough_space_passes_and_one_byte_less_fails(self):
        from pysciqlop_cache.migrate import _dir_size, _ensure_enough_disk_space
        needed = _dir_size(self.src) * 1.2
        dst = os.path.join(self.src, "not", "created", "yet")  # checked on its closest existing parent
        with mock.patch("pysciqlop_cache.migrate.shutil.disk_usage",
                        return_value=mock.Mock(free=needed)):
            _ensure_enough_disk_space(self.src, dst)
        with mock.patch("pysciqlop_cache.migrate.shutil.disk_usage",
                        return_value=mock.Mock(free=needed - 1)):
            with self.assertRaises(InsufficientDiskSpaceError):
                _ensure_enough_disk_space(self.src, dst)

    def test_size_helpers_on_a_missing_path(self):
        from pysciqlop_cache.migrate import _dir_size, _largest_file_size
        missing = os.path.join(self.src, "missing")
        self.assertEqual((_dir_size(missing), _largest_file_size(missing)), (0, 0))
        self.assertGreater(_largest_file_size(self.src), 0)


class MigrateHelpers(unittest.TestCase):
    def test_remaining_ttl(self):
        import time
        from pysciqlop_cache.migrate import _remaining_ttl
        self.assertIsNone(_remaining_ttl(None))
        self.assertEqual(_remaining_ttl(time.time() - 10), 0)
        self.assertAlmostEqual(_remaining_ttl(time.time() + 100), 100, delta=1)

    def test_existing_ancestor_and_empty_directories(self):
        from pysciqlop_cache.migrate import _existing_ancestor, _largest_file_size
        root = tempfile.mkdtemp(prefix="migrate_anc_")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        from pathlib import Path
        self.assertEqual(Path(_existing_ancestor(os.path.join(root, "a", "b"))), Path(root))
        self.assertEqual(_largest_file_size(root), 0)


class MigrateCommandLine(unittest.TestCase):
    def setUp(self):
        self.src = tempfile.mkdtemp(prefix="migrate_src_")
        self.dst = tempfile.mkdtemp(prefix="migrate_dst_")
        self.addCleanup(shutil.rmtree, self.src, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.dst, ignore_errors=True)

    def test_main_migrates_and_prints_a_summary(self):
        from pysciqlop_cache import migrate as migrate_module
        cache = diskcache.Cache(self.src)
        cache["a"] = 1
        cache.close()

        with mock.patch("sys.argv", ["migrate", "--drop", self.src, self.dst]), \
                mock.patch("sys.stdout") as out:
            migrate_module.main()

        printed = "".join(call.args[0] for call in out.write.call_args_list)
        self.assertIn("1 migrated", printed)
        from pysciqlop_cache import Cache
        self.assertEqual(Cache(self.dst)["a"], 1)

    def test_main_exits_with_1_when_an_entry_fails(self):
        from pysciqlop_cache import migrate as migrate_module
        failed = {"migrated": 0, "skipped": 0, "errors": 1, "elapsed_secs": 0.0}
        with mock.patch("sys.argv", ["migrate", self.src, self.dst]), \
                mock.patch.object(migrate_module, "migrate", return_value=failed), \
                mock.patch("sys.stdout"):
            with self.assertRaises(SystemExit) as exited:
                migrate_module.main()
        self.assertEqual(exited.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
