import os
import shutil
import unittest
from pysciqlop_cache import  Cache
from  tempfile import TemporaryDirectory
import time

class TestCache(unittest.TestCase):

    def setUp(self):
        """
        Set up the test environment.
        """
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)

    def tearDown(self):
        """
        Clean up the test environment.
        """
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_simple_set_get(self):
        """
        Test the Cache functionality.
        """

        key = "test_key"
        value = "test_value"

        # Test setting a value
        self.cache.set(key, value)
        assert self.cache.get(key) == value


    def test_expiration(self):
        # Test expiration
        key = "test_expire_key"
        value = "test_expire_value"
        self.cache.set(key, value, expire=1)
        time.sleep(2)
        assert self.cache.get(key) is None, "Cache should return None after expiration"




    def test_incr_new_key(self):
        result = self.cache.incr("counter")
        self.assertEqual(result, 1)

    def test_incr_existing_key(self):
        self.cache.set("counter", 10)
        result = self.cache.incr("counter", 5)
        self.assertEqual(result, 15)

    def test_incr_with_default(self):
        result = self.cache.incr("counter", 1, 100)
        self.assertEqual(result, 101)

    def test_decr(self):
        self.cache.set("counter", 10)
        result = self.cache.decr("counter", 3)
        self.assertEqual(result, 7)

    def test_decr_new_key(self):
        result = self.cache.decr("counter")
        self.assertEqual(result, -1)

    def test_incr_multiple(self):
        self.cache.incr("counter")
        self.cache.incr("counter")
        result = self.cache.incr("counter")
        self.assertEqual(result, 3)

    def test_incr_preserves_as_gettable(self):
        self.cache.incr("counter", 42)
        value = self.cache.get("counter")
        self.assertEqual(value, 42)


    def test_memoize_caches_result(self):
        call_count = 0

        @self.cache.memoize()
        def add(a, b):
            nonlocal call_count
            call_count += 1
            return a + b

        self.assertEqual(add(1, 2), 3)
        self.assertEqual(call_count, 1)
        self.assertEqual(add(1, 2), 3)
        self.assertEqual(call_count, 1)  # not called again

    def test_memoize_different_args(self):
        call_count = 0

        @self.cache.memoize()
        def multiply(a, b):
            nonlocal call_count
            call_count += 1
            return a * b

        self.assertEqual(multiply(2, 3), 6)
        self.assertEqual(multiply(3, 4), 12)
        self.assertEqual(call_count, 2)

    def test_memoize_with_kwargs(self):
        @self.cache.memoize()
        def greet(name, greeting="hello"):
            return f"{greeting} {name}"

        self.assertEqual(greet("world"), "hello world")
        self.assertEqual(greet("world", greeting="hi"), "hi world")
        self.assertEqual(greet("world"), "hello world")  # from cache

    def test_memoize_typed(self):
        call_count = 0

        @self.cache.memoize(typed=True)
        def identity(x):
            nonlocal call_count
            call_count += 1
            return x

        identity(1)
        identity(1.0)
        self.assertEqual(call_count, 2)  # different types = different keys

    def test_memoize_not_typed(self):
        call_count = 0

        @self.cache.memoize(typed=False)
        def identity(x):
            nonlocal call_count
            call_count += 1
            return x

        identity(1)
        identity(1.0)
        self.assertEqual(call_count, 2)  # still different because repr differs

    def test_memoize_with_tag(self):
        @self.cache.memoize(tag="math")
        def square(x):
            return x * x

        self.assertEqual(square(5), 25)
        self.cache.evict_tag("math")
        # After eviction, should recompute
        call_count = 0
        original_fn = square.__wrapped__

        @self.cache.memoize(tag="math")
        def square2(x):
            nonlocal call_count
            call_count += 1
            return x * x

        square2(5)
        self.assertEqual(call_count, 1)  # recomputed after eviction

    def test_memoize_with_expiry(self):
        call_count = 0

        @self.cache.memoize(expire=1)
        def compute(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        self.assertEqual(compute(3), 6)
        self.assertEqual(call_count, 1)
        self.assertEqual(compute(3), 6)
        self.assertEqual(call_count, 1)
        time.sleep(2)
        self.assertEqual(compute(3), 6)
        self.assertEqual(call_count, 2)  # expired, recomputed

    def test_memoize_caches_none(self):
        call_count = 0

        @self.cache.memoize()
        def returns_none(x):
            nonlocal call_count
            call_count += 1
            return None

        self.assertIsNone(returns_none(1))
        self.assertEqual(call_count, 1)
        self.assertIsNone(returns_none(1))
        self.assertEqual(call_count, 1)  # None was cached

    def test_memoize_cache_key(self):
        @self.cache.memoize()
        def func(x):
            return x

        key = func.__cache_key__(42)
        self.assertIsInstance(key, str)
        self.assertIn("func", key)


    def test_del(self):
        self.cache.set("to_delete", "value")
        self.assertTrue(self.cache.exists("to_delete"))
        self.cache.delete("to_delete")
        self.assertFalse(self.cache.exists("to_delete"))

    def test_del_nonexistent(self):
        self.assertFalse(self.cache.delete("no_such_key"))

    def test_pop(self):
        self.cache.set("pop_key", [1, 2, 3])
        result = self.cache.pop("pop_key")
        self.assertEqual(result, [1, 2, 3])
        self.assertIsNone(self.cache.get("pop_key"))

    def test_pop_nonexistent(self):
        result = self.cache.pop("no_such_key")
        self.assertIsNone(result)

    def test_pop_with_default(self):
        result = self.cache.pop("no_such_key", default="fallback")
        self.assertEqual(result, "fallback")

    def test_getitem_setitem(self):
        self.cache["dictkey"] = "dictval"
        self.assertEqual(self.cache["dictkey"], "dictval")

    def test_getitem_missing(self):
        self.assertIsNone(self.cache["missing"])

    def test_delitem(self):
        self.cache.set("delme", 42)
        del self.cache["delme"]
        self.assertIsNone(self.cache.get("delme"))

    def test_len(self):
        self.assertEqual(len(self.cache), 0)
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.assertEqual(len(self.cache), 2)

    def test_keys(self):
        self.cache.set("x", 1)
        self.cache.set("y", 2)
        self.cache.set("z", 3)
        keys = sorted(self.cache.keys())
        self.assertEqual(keys, ["x", "y", "z"])

    def test_keys_excludes_expired(self):
        self.cache.set("alive", 1)
        self.cache.set("dead", 2, expire=0)
        time.sleep(0.1)
        keys = self.cache.keys()
        self.assertIn("alive", keys)
        self.assertNotIn("dead", keys)

    def test_exists(self):
        self.cache.set("present", "yes")
        self.assertTrue(self.cache.exists("present"))
        self.assertFalse(self.cache.exists("absent"))

    def test_exists_expired(self):
        self.cache.set("temp", "val", expire=0)
        time.sleep(0.1)
        self.assertFalse(self.cache.exists("temp"))

    def test_add(self):
        self.assertTrue(self.cache.add("newkey", "newval"))
        self.assertEqual(self.cache.get("newkey"), "newval")

    def test_add_existing_key_fails(self):
        self.cache.set("occupied", "original")
        self.assertFalse(self.cache.add("occupied", "replacement"))
        self.assertEqual(self.cache.get("occupied"), "original")

    def test_add_with_expire(self):
        self.cache.add("expiring", "val", expire=1)
        self.assertEqual(self.cache.get("expiring"), "val")
        time.sleep(2)
        self.assertIsNone(self.cache.get("expiring"))

    def test_add_with_tag(self):
        self.cache.add("tagged", "val", tag="mytag")
        self.assertEqual(self.cache.get("tagged"), "val")
        self.cache.evict_tag("mytag")
        self.assertIsNone(self.cache.get("tagged"))

    def test_set_with_tag(self):
        self.cache.set("t1", "v1", tag="grp")
        self.cache.set("t2", "v2", tag="grp")
        self.cache.set("t3", "v3")
        self.cache.evict_tag("grp")
        self.assertIsNone(self.cache.get("t1"))
        self.assertIsNone(self.cache.get("t2"))
        self.assertEqual(self.cache.get("t3"), "v3")

    def test_set_with_expire_and_tag(self):
        self.cache.set("combo", "val", expire=3600, tag="combo_tag")
        self.assertEqual(self.cache.get("combo"), "val")
        self.cache.evict_tag("combo_tag")
        self.assertIsNone(self.cache.get("combo"))

    def test_evict_tag_nonexistent(self):
        evicted = self.cache.evict_tag("no_such_tag")
        self.assertEqual(evicted, 0)

    def test_clear(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.cache.clear()
        self.assertEqual(len(self.cache), 0)
        self.assertIsNone(self.cache.get("a"))

    def test_check(self):
        self.assertTrue(self.cache.check().ok)

    def test_get_default(self):
        self.assertEqual(self.cache.get("missing", "fallback"), "fallback")

    def test_get_default_none(self):
        self.assertIsNone(self.cache.get("missing"))

    def test_touch(self):
        from datetime import timedelta
        self.cache.set("touchme", "val", expire=3600)
        self.cache.touch("touchme", timedelta(seconds=0))
        self.cache.expire()
        time.sleep(0.1)
        self.assertIsNone(self.cache.get("touchme"))

    def test_expire(self):
        self.cache.set("short", "val", expire=0)
        self.cache.set("long", "val")
        time.sleep(0.1)
        self.cache.expire()
        self.assertIsNone(self.cache.get("short"))
        self.assertEqual(self.cache.get("long"), "val")

    def test_big_value_roundtrip(self):
        big = b"x" * (1024 * 1024)
        self.cache.set("bigkey", big)
        result = self.cache.get("bigkey")
        self.assertEqual(result, big)

    def test_big_value_clear(self):
        big = b"x" * (1024 * 1024)
        self.cache.set("big1", big)
        self.cache.set("big2", big)
        self.cache.clear()
        self.assertEqual(len(self.cache), 0)

    def test_max_size_constructor(self):
        with TemporaryDirectory() as td:
            cache = Cache(td, max_size=500)
            data = b"a" * 100
            for i in range(10):
                cache.set(f"k{i}", data)
            cache.evict()
            self.assertLessEqual(len(cache), 5)

    def test_various_python_types(self):
        self.cache.set("int", 42)
        self.assertEqual(self.cache.get("int"), 42)
        self.cache.set("float", 3.14)
        self.assertAlmostEqual(self.cache.get("float"), 3.14)
        self.cache.set("list", [1, "two", 3.0])
        self.assertEqual(self.cache.get("list"), [1, "two", 3.0])
        self.cache.set("dict", {"a": 1})
        self.assertEqual(self.cache.get("dict"), {"a": 1})
        self.cache.set("none", None)
        # None values need sentinel to distinguish from missing
        self.cache.set("bool", True)
        self.assertEqual(self.cache.get("bool"), True)

    def test_set_overwrite(self):
        self.cache.set("key", "first")
        self.cache.set("key", "second")
        self.assertEqual(self.cache.get("key"), "second")


class TestIndex(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        from pysciqlop_cache import Index
        self.index = Index(self.tmp_dir.name)

    def tearDown(self):
        if hasattr(self, 'index'):
            del self.index
        shutil.rmtree(self.tmp_dir.name)

    def test_set_get(self):
        self.index.set("key", "hello")
        self.assertEqual(self.index.get("key"), "hello")

    def test_get_missing(self):
        self.assertIsNone(self.index.get("nope"))
        self.assertEqual(self.index.get("nope", "default"), "default")

    def test_add(self):
        self.assertTrue(self.index.add("k", 42))
        self.assertFalse(self.index.add("k", 99))
        self.assertEqual(self.index.get("k"), 42)

    def test_delete(self):
        self.index.set("k", "val")
        self.index.delete("k")
        self.assertIsNone(self.index.get("k"))

    def test_pop(self):
        self.index.set("k", [1, 2, 3])
        self.assertEqual(self.index.pop("k"), [1, 2, 3])
        self.assertIsNone(self.index.get("k"))

    def test_pop_missing(self):
        self.assertIsNone(self.index.pop("nope"))
        self.assertEqual(self.index.pop("nope", "fallback"), "fallback")

    def test_keys_count(self):
        self.index.set("a", 1)
        self.index.set("b", 2)
        self.assertEqual(len(self.index), 2)
        self.assertEqual(sorted(self.index.keys()), ["a", "b"])

    def test_contains(self):
        self.index.set("k", "v")
        self.assertTrue("k" in self.index)
        self.assertFalse("nope" in self.index)

    def test_iter(self):
        self.index.set("x", 1)
        self.index.set("y", 2)
        self.assertEqual(sorted(self.index), ["x", "y"])

    def test_dict_interface(self):
        self.index["key"] = "value"
        self.assertEqual(self.index["key"], "value")
        del self.index["key"]
        self.assertIsNone(self.index.get("key"))

    def test_incr_decr(self):
        self.assertEqual(self.index.incr("c"), 1)
        self.assertEqual(self.index.incr("c"), 2)
        self.assertEqual(self.index.decr("c"), 1)

    def test_clear(self):
        self.index.set("a", 1)
        self.index.set("b", 2)
        self.index.clear()
        self.assertEqual(len(self.index), 0)

    def test_big_value(self):
        big = b"x" * (1024 * 1024)
        self.index.set("bigkey", big)
        result = self.index.get("bigkey")
        self.assertEqual(result, big)

    def test_context_manager(self):
        from pysciqlop_cache import Index
        with Index(self.tmp_dir.name) as idx:
            idx.set("k", "v")
            self.assertEqual(idx.get("k"), "v")

    def test_repr(self):
        self.assertIn("Index(", repr(self.index))

    def test_entries_never_expire(self):
        self.index.set("permanent", "data")
        import time
        time.sleep(0.1)
        self.assertEqual(self.index.get("permanent"), "data")


class TestTransact(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)

    def tearDown(self):
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_transact_commits_on_clean_exit(self):
        with self.cache.transact():
            self.cache.set("a", 1)
            self.cache.set("b", 2)
        self.assertEqual(self.cache.get("a"), 1)
        self.assertEqual(self.cache.get("b"), 2)

    def test_transact_rolls_back_on_exception(self):
        self.cache.set("pre", "existing")
        with self.assertRaises(ValueError):
            with self.cache.transact():
                self.cache.set("x", 10)
                raise ValueError("boom")
        self.assertIsNone(self.cache.get("x"))
        self.assertEqual(self.cache.get("pre"), "existing")

    def test_transact_as_txn_syntax(self):
        with self.cache.transact() as txn:
            txn.set("k", "v")
        self.assertEqual(self.cache.get("k"), "v")

    def test_transact_returns_self(self):
        with self.cache.transact() as txn:
            self.assertIs(txn, self.cache)

    def test_nested_transact_raises(self):
        with self.cache.transact():
            with self.assertRaises(RuntimeError):
                with self.cache.transact():
                    pass

    def test_transact_on_index(self):
        from pysciqlop_cache import Index
        with TemporaryDirectory() as td:
            idx = Index(td)
            with idx.transact() as txn:
                txn.set("a", 1)
            self.assertEqual(idx.get("a"), 1)

    def test_transact_index_rollback(self):
        from pysciqlop_cache import Index
        with TemporaryDirectory() as td:
            idx = Index(td)
            with self.assertRaises(ValueError):
                with idx.transact():
                    idx.set("x", 42)
                    raise ValueError("rollback")
            self.assertIsNone(idx.get("x"))


class TestFanoutCache(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        from pysciqlop_cache import FanoutCache
        self.cache = FanoutCache(self.tmp_dir.name, shard_count=4)

    def tearDown(self):
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_set_get(self):
        self.cache.set("key1", "value1")
        self.assertEqual(self.cache.get("key1"), "value1")

    def test_count_and_size(self):
        self.cache.set("k1", "aaa")
        self.cache.set("k2", "bbbbb")
        self.assertEqual(self.cache.count(), 2)

    def test_keys(self):
        self.cache.set("k1", "v1")
        self.cache.set("k2", "v2")
        self.assertEqual(sorted(self.cache.keys()), ["k1", "k2"])

    def test_delete(self):
        self.cache.set("k1", "v1")
        self.cache.delete("k1")
        self.assertIsNone(self.cache.get("k1"))

    def test_clear(self):
        for i in range(10):
            self.cache.set(f"key{i}", "val")
        self.cache.clear()
        self.assertEqual(self.cache.count(), 0)

    def test_shard_count(self):
        self.assertEqual(self.cache.shard_count(), 4)

    def test_stats(self):
        self.cache.set("k1", "v1")
        self.cache.get("k1")
        self.cache.get("missing")
        s = self.cache.stats()
        self.assertEqual(s["hits"], 1)
        self.assertEqual(s["misses"], 1)

    def test_evict_tag(self):
        self.cache.set("t1", "v1", tag="group")
        self.cache.set("t2", "v2", tag="group")
        self.cache.set("t3", "v3")
        evicted = self.cache.evict_tag("group")
        self.assertEqual(evicted, 2)
        self.assertEqual(self.cache.count(), 1)

    def test_dict_interface(self):
        self.cache["dictkey"] = "dictval"
        self.assertEqual(self.cache["dictkey"], "dictval")

    def test_contains(self):
        self.cache.set("k", "v")
        self.assertTrue("k" in self.cache)
        self.assertFalse("nope" in self.cache)

    def test_iter(self):
        self.cache.set("x", 1)
        self.cache.set("y", 2)
        self.assertEqual(sorted(self.cache), ["x", "y"])

    def test_repr(self):
        self.assertIn("FanoutCache(", repr(self.cache))

    def test_various_types(self):
        self.cache.set("int", 42)
        self.assertEqual(self.cache.get("int"), 42)
        self.cache.set("list", [1, 2, 3])
        self.assertEqual(self.cache.get("list"), [1, 2, 3])

    def test_expiration(self):
        self.cache.set("exp", "val", expire=1)
        time.sleep(2)
        self.assertIsNone(self.cache.get("exp"))

    def test_incr_decr(self):
        self.assertEqual(self.cache.incr("counter"), 1)
        self.assertEqual(self.cache.incr("counter"), 2)
        self.assertEqual(self.cache.decr("counter"), 1)

    def test_pop(self):
        self.cache.set("k", "v")
        self.assertEqual(self.cache.pop("k"), "v")
        self.assertIsNone(self.cache.get("k"))

    def test_add(self):
        self.assertTrue(self.cache.add("newkey", "newval"))
        self.assertFalse(self.cache.add("newkey", "other"))
        self.assertEqual(self.cache.get("newkey"), "newval")


    def test_transact_commits(self):
        with self.cache.transact("k") as txn:
            txn.set("k", "v")
        self.assertEqual(self.cache.get("k"), "v")

    def test_transact_rollback(self):
        self.cache.set("pre", "existing")
        with self.assertRaises(ValueError):
            with self.cache.transact("x"):
                self.cache.set("x", 10)
                raise ValueError("boom")
        self.assertIsNone(self.cache.get("x"))
        self.assertEqual(self.cache.get("pre"), "existing")


class TestFanoutIndex(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        from pysciqlop_cache import FanoutIndex
        self.index = FanoutIndex(self.tmp_dir.name, shard_count=4)

    def tearDown(self):
        if hasattr(self, 'index'):
            del self.index
        shutil.rmtree(self.tmp_dir.name)

    def test_set_get(self):
        self.index.set("key1", "value1")
        self.assertEqual(self.index.get("key1"), "value1")

    def test_count(self):
        self.index.set("k1", "aaa")
        self.index.set("k2", "bbbbb")
        self.assertEqual(self.index.count(), 2)

    def test_keys(self):
        self.index.set("k1", "v1")
        self.index.set("k2", "v2")
        self.assertEqual(sorted(self.index.keys()), ["k1", "k2"])

    def test_delete(self):
        self.index.set("k1", "v1")
        self.index.delete("k1")
        self.assertIsNone(self.index.get("k1"))

    def test_clear(self):
        self.index.set("a", 1)
        self.index.set("b", 2)
        self.index.clear()
        self.assertEqual(len(self.index), 0)

    def test_dict_interface(self):
        self.index["key"] = "value"
        self.assertEqual(self.index["key"], "value")

    def test_repr(self):
        self.assertIn("FanoutIndex(", repr(self.index))

    def test_transact_commits(self):
        with self.index.transact("k") as txn:
            txn.set("k", "v")
        self.assertEqual(self.index.get("k"), "v")

    def test_transact_rollback(self):
        self.index.set("pre", "existing")
        with self.assertRaises(ValueError):
            with self.index.transact("x"):
                self.index.set("x", 42)
                raise ValueError("rollback")
        self.assertIsNone(self.index.get("x"))
        self.assertEqual(self.index.get("pre"), "existing")


class TestCheckResult(unittest.TestCase):

    def test_check_returns_result_object(self):
        with TemporaryDirectory() as d:
            c = Cache(os.path.join(d, "check_test"))
            c["key"] = b"value"
            result = c.check()
            self.assertTrue(result.ok)
            self.assertEqual(result.orphaned_files, 0)
            self.assertEqual(result.dangling_rows, 0)
            self.assertEqual(result.size_mismatches, 0)
            self.assertTrue(result.counters_consistent)
            self.assertTrue(result.sqlite_integrity_ok)

    def test_check_fix(self):
        with TemporaryDirectory() as d:
            c = Cache(os.path.join(d, "check_fix"))
            c["key"] = b"value"
            result = c.check(fix=True)
            self.assertTrue(result.ok)

    def test_fanout_check_returns_result_object(self):
        from pysciqlop_cache import FanoutCache
        with TemporaryDirectory() as d:
            fc = FanoutCache(os.path.join(d, "fanout_check"), shard_count=4)
            fc["key1"] = b"value1"
            fc["key2"] = b"value2"
            result = fc.check()
            self.assertTrue(result.ok)
            self.assertEqual(result.orphaned_files, 0)
            self.assertEqual(result.dangling_rows, 0)

    def test_fanout_check_fix(self):
        from pysciqlop_cache import FanoutCache
        with TemporaryDirectory() as d:
            fc = FanoutCache(os.path.join(d, "fanout_check_fix"), shard_count=4)
            fc["key"] = b"data"
            result = fc.check(fix=True)
            self.assertTrue(result.ok)

    def test_fanout_index_check_returns_result_object(self):
        from pysciqlop_cache import FanoutIndex
        with TemporaryDirectory() as d:
            fi = FanoutIndex(os.path.join(d, "fanout_idx_check"), shard_count=4)
            fi["key1"] = b"value1"
            result = fi.check()
            self.assertTrue(result.ok)
            self.assertEqual(result.orphaned_files, 0)
            self.assertEqual(result.dangling_rows, 0)


class TestMmapCache(unittest.TestCase):
    """Tests for the mmap handle cache in DiskStorage.

    Values >8KB are stored as files and served via mmap. The cache keeps
    recently-used mmap handles alive to avoid repeated open/mmap/munmap/close.
    """

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)
        # 16KB values — above the 8KB file threshold
        self.large_value = b"x" * (16 * 1024)

    def tearDown(self):
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_no_key_mixup(self):
        for i in range(20):
            self.cache.set(f"key{i}", bytes([i]) * (16 * 1024))
        for i in range(20):
            val = self.cache.get(f"key{i}")
            self.assertEqual(val, bytes([i]) * (16 * 1024))

    def test_repeated_get_same_value(self):
        self.cache.set("big", self.large_value)
        for _ in range(100):
            self.assertEqual(self.cache.get("big"), self.large_value)

    def test_overwrite_invalidates_cache(self):
        self.cache.set("key", b"A" * (16 * 1024))
        self.assertEqual(self.cache.get("key"), b"A" * (16 * 1024))
        self.cache.set("key", b"B" * (16 * 1024))
        self.assertEqual(self.cache.get("key"), b"B" * (16 * 1024))

    def test_delete_invalidates_cache(self):
        self.cache.set("key", self.large_value)
        self.cache.get("key")  # populate mmap cache
        del self.cache["key"]
        self.assertIsNone(self.cache.get("key"))

    def test_eviction_beyond_capacity(self):
        # Default capacity is 128; write 200 large values to force LRU eviction
        for i in range(200):
            self.cache.set(f"k{i}", bytes([i % 256]) * (16 * 1024))
        # All values should still be readable (eviction only drops the mmap
        # handle, not the file — re-reading just re-opens the file)
        for i in range(200):
            val = self.cache.get(f"k{i}")
            self.assertEqual(val, bytes([i % 256]) * (16 * 1024))

    def test_mixed_small_and_large(self):
        self.cache.set("small", "hello")
        self.cache.set("large", self.large_value)
        self.assertEqual(self.cache.get("small"), "hello")
        self.assertEqual(self.cache.get("large"), self.large_value)
        # Repeat to exercise cache hit path
        self.assertEqual(self.cache.get("large"), self.large_value)
        self.assertEqual(self.cache.get("small"), "hello")


class TestIterkeys(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)

    def tearDown(self):
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_iterkeys_empty(self):
        self.assertEqual(list(self.cache.iterkeys()), [])

    def test_iterkeys_matches_keys(self):
        for i in range(20):
            self.cache.set(f"k{i}", f"v{i}")
        self.assertEqual(sorted(self.cache.iterkeys()), sorted(self.cache.keys()))

    def test_iterkeys_skips_expired(self):
        self.cache.set("alive", "v")
        self.cache.set("dead", "v", expire=0)
        time.sleep(0.1)
        keys = list(self.cache.iterkeys())
        self.assertIn("alive", keys)
        self.assertNotIn("dead", keys)

    def test_iter_uses_iterkeys(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.assertEqual(sorted(self.cache), sorted(self.cache.keys()))

    def test_iterkeys_index(self):
        from pysciqlop_cache import Index
        with TemporaryDirectory() as td:
            idx = Index(td)
            idx.set("x", 1)
            idx.set("y", 2)
            self.assertEqual(sorted(idx.iterkeys()), ["x", "y"])

    def test_iterkeys_fanout(self):
        from pysciqlop_cache import FanoutCache
        with TemporaryDirectory() as td:
            fc = FanoutCache(td, shard_count=4)
            for i in range(10):
                fc.set(f"k{i}", f"v{i}")
            self.assertEqual(sorted(fc.iterkeys()), sorted(fc.keys()))


class TestErrorHandling(unittest.TestCase):

    def test_bad_path_raises(self):
        with self.assertRaises(RuntimeError):
            Cache("/dev/null/impossible/path")


class TestLock(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = TemporaryDirectory(delete=False)
        self.cache = Cache(self.tmp_dir.name)

    def tearDown(self):
        if hasattr(self, 'cache'):
            del self.cache
        shutil.rmtree(self.tmp_dir.name)

    def test_lock_context_manager(self):
        with self.cache.lock("mylock"):
            self.assertIn("mylock", self.cache)
        self.assertNotIn("mylock", self.cache)

    def test_lock_acquire_release(self):
        from pysciqlop_cache import Lock
        lock = Lock(self.cache, "mylock")
        lock.acquire()
        self.assertTrue(lock.locked())
        lock.release()
        self.assertFalse(lock.locked())

    def test_lock_released_on_exception(self):
        with self.assertRaises(ValueError):
            with self.cache.lock("errlock"):
                raise ValueError("boom")
        self.assertNotIn("errlock", self.cache)

    def test_lock_with_expire(self):
        lock = self.cache.lock("explock", expire=1)
        lock.acquire()
        self.assertTrue(lock.locked())
        time.sleep(2)
        self.assertFalse(lock.locked())

    def test_lock_blocks_second_acquire(self):
        import threading
        results = []
        lock = self.cache.lock("contested")
        lock.acquire()

        def try_acquire():
            with self.cache.lock("contested"):
                results.append("acquired")

        t = threading.Thread(target=try_acquire)
        t.start()
        time.sleep(0.05)
        self.assertEqual(results, [])
        lock.release()
        t.join(timeout=5)
        self.assertEqual(results, ["acquired"])

    def test_fanout_lock(self):
        from pysciqlop_cache import FanoutCache
        with TemporaryDirectory() as td:
            fc = FanoutCache(td, shard_count=4)
            with fc.lock("fanout_lock"):
                self.assertIn("fanout_lock", fc)
            self.assertNotIn("fanout_lock", fc)


if __name__ == "__main__":
    unittest.main()
