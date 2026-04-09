"""Property-based stateful tests for sciqlop-cache using Hypothesis.

Drives random operation sequences against each store type with a Python dict
as ground-truth oracle.  Hypothesis shrinks failing cases to minimal reproducers.

Run:  python -m pytest tests/python/test_hypothesis.py -x -v
"""

import shutil
from tempfile import mkdtemp

from hypothesis import settings, given, HealthCheck
from hypothesis.stateful import (
    RuleBasedStateMachine,
    rule,
    invariant,
    initialize,
    precondition,
)
from hypothesis import strategies as st

from pysciqlop_cache import Cache, Index, FanoutCache, FanoutIndex

# -- Strategies ---------------------------------------------------------------

keys = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789/_-",
    min_size=1,
    max_size=30,
)
# Values that cross the 8KB blob/file threshold
small_values = st.binary(min_size=1, max_size=100)
large_values = st.binary(min_size=8192, max_size=16384)
values = st.one_of(small_values, large_values)
tags = st.sampled_from(["alpha", "beta", "gamma", "delta", "epsilon"])
expire_secs = st.integers(min_value=3600, max_value=86400)


# -- Base state machine -------------------------------------------------------

class StoreStateMachine(RuleBasedStateMachine):
    """Base class: oracle-checked random ops on a single store instance."""

    store_factory = None  # override in subclasses
    supports_expire = False
    supports_tags = False

    def __init__(self):
        super().__init__()
        self.tmpdir = mkdtemp()
        self.oracle: dict[str, bytes] = {}

    def make_store(self, path):
        raise NotImplementedError

    @initialize()
    def init_store(self):
        self.store = self.make_store(self.tmpdir)

    def teardown(self):
        del self.store
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # -- rules -----------------------------------------------------------------

    @rule(key=keys, value=values)
    def set_item(self, key, value):
        self.store.set(key, value)
        self.oracle[key] = value

    @rule(key=keys, value=values)
    def add_item(self, key, value):
        result = self.store.add(key, value)
        if key not in self.oracle:
            assert result, f"add({key!r}) should succeed for new key"
            self.oracle[key] = value
        else:
            assert not result, f"add({key!r}) should fail for existing key"

    @rule(key=keys)
    def get_item(self, key):
        result = self.store.get(key)
        if key in self.oracle:
            assert result is not None, f"get({key!r}) returned None, expected data"
            assert bytes(result) == self.oracle[key], f"data mismatch for {key!r}"
        else:
            assert result is None, f"get({key!r}) should be None for missing key"

    @rule(key=keys)
    def delete_item(self, key):
        self.store.delete(key)
        self.oracle.pop(key, None)

    @rule(key=keys)
    def pop_item(self, key):
        result = self.store.pop(key)
        expected = self.oracle.pop(key, None)
        if expected is not None:
            assert result is not None, f"pop({key!r}) returned None, expected data"
            assert bytes(result) == expected
        else:
            assert result is None

    @rule(key=keys)
    def exists_item(self, key):
        result = self.store.exists(key)
        assert result == (key in self.oracle), (
            f"exists({key!r}) = {result}, oracle says {key in self.oracle}"
        )

    @rule(key=keys, value=values)
    def overwrite_item(self, key, value):
        """Set twice with different values, verify last write wins."""
        self.store.set(key, value)
        self.oracle[key] = value

    @rule()
    def list_keys(self):
        store_keys = set(self.store.keys())
        oracle_keys = set(self.oracle.keys())
        assert store_keys == oracle_keys, (
            f"keys mismatch: extra={store_keys - oracle_keys}, "
            f"missing={oracle_keys - store_keys}"
        )

    @rule()
    def check_count(self):
        assert self.store.count() == len(self.oracle), (
            f"count={self.store.count()}, oracle={len(self.oracle)}"
        )

    @rule()
    def clear_store(self):
        self.store.clear()
        self.oracle.clear()

    # -- incr/decr (stored as 8-byte int, not tracked in oracle) ---------------

    @rule(key=keys, delta=st.integers(min_value=1, max_value=100))
    def incr_decr_roundtrip(self, key, delta):
        if key in self.oracle:
            return  # skip keys with blob data
        v1 = self.store.incr(key, delta, 0)
        v2 = self.store.decr(key, delta)
        assert v2 == v1 - delta == 0
        self.store.delete(key)

    # -- structural integrity --------------------------------------------------

    @invariant()
    def size_non_negative(self):
        assert self.store.size() >= 0

    @invariant()
    def check_passes(self):
        cr = self.store.check()
        assert cr.ok, (
            f"check failed: orphaned={cr.orphaned_files}, "
            f"dangling={cr.dangling_rows}, size_mm={cr.size_mismatches}, "
            f"counters={cr.counters_consistent}, sqlite={cr.sqlite_integrity_ok}"
        )


# -- Cache (with expiration + tags) --------------------------------------------

class CacheStateMachine(StoreStateMachine):
    supports_expire = True
    supports_tags = True

    def make_store(self, path):
        return Cache(path)

    @rule(key=keys, value=values, expire=expire_secs)
    def set_with_expire(self, key, value, expire):
        self.store.set(key, value, expire=expire)
        self.oracle[key] = value

    @rule(key=keys, value=values, tag=tags)
    def set_with_tag(self, key, value, tag):
        self.store.set(key, value, tag=tag)
        self.oracle[key] = value

    @rule(key=keys, value=values, expire=expire_secs, tag=tags)
    def set_with_expire_and_tag(self, key, value, expire, tag):
        self.store.set(key, value, expire=expire, tag=tag)
        self.oracle[key] = value


# -- Index (bare key-value) ---------------------------------------------------

class IndexStateMachine(StoreStateMachine):
    def make_store(self, path):
        return Index(path)


# -- FanoutCache --------------------------------------------------------------

class FanoutCacheStateMachine(StoreStateMachine):
    supports_expire = True
    supports_tags = True

    def make_store(self, path):
        return FanoutCache(path, shard_count=4)

    @rule(key=keys, value=values, expire=expire_secs)
    def set_with_expire(self, key, value, expire):
        self.store.set(key, value, expire=expire)
        self.oracle[key] = value

    @rule(key=keys, value=values, tag=tags)
    def set_with_tag(self, key, value, tag):
        self.store.set(key, value, tag=tag)
        self.oracle[key] = value

    @rule(key=keys, value=values, expire=expire_secs, tag=tags)
    def set_with_expire_and_tag(self, key, value, expire, tag):
        self.store.set(key, value, expire=expire, tag=tag)
        self.oracle[key] = value


# -- FanoutIndex ---------------------------------------------------------------

class FanoutIndexStateMachine(StoreStateMachine):
    def make_store(self, path):
        return FanoutIndex(path, shard_count=4)


# -- Test entry points (Hypothesis discovers these) ----------------------------

hyp_settings = settings(
    max_examples=200,
    stateful_step_count=50,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
)

TestCache = CacheStateMachine.TestCase
TestCache.settings = hyp_settings

TestIndex = IndexStateMachine.TestCase
TestIndex.settings = hyp_settings

TestFanoutCache = FanoutCacheStateMachine.TestCase
TestFanoutCache.settings = hyp_settings

TestFanoutIndex = FanoutIndexStateMachine.TestCase
TestFanoutIndex.settings = hyp_settings
