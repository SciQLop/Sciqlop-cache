"""
_pysciqlop_cache
----------------
"""

from collections.abc import Callable, Sequence
import datetime


BLOSC2_AVAILABLE: bool = True

def decode_buffers(src: object, jobs: Sequence, alloc: Callable) -> list:
    """
    Decode each (codec, offset, stored_size, raw_size) job of src into alloc(raw_size).
    """

def compress_buffer(src: object, typesize: int, min_ratio: float) -> object:
    """
    blosc2 (lz4 + shuffle) bytes of src as a Buffer, or None if not worth it.
    """

class CacheKeyCursor:
    def __iter__(self) -> object: ...

    def __next__(self) -> str: ...

class IndexKeyCursor:
    def __iter__(self) -> object: ...

    def __next__(self) -> str: ...

class FanoutCacheKeyCursor:
    def __iter__(self) -> object: ...

    def __next__(self) -> str: ...

class FanoutIndexKeyCursor:
    def __iter__(self) -> object: ...

    def __next__(self) -> str: ...

class Buffer:
    def __buffer__(self, flags, /):
        """
        Return a buffer object that exposes the underlying memory of the object.
        """

    def memoryview(self) -> object: ...

class CacheCheckResult:
    @property
    def ok(self) -> bool: ...

    @property
    def orphaned_files(self) -> int: ...

    @property
    def dangling_rows(self) -> int: ...

    @property
    def size_mismatches(self) -> int: ...

    @property
    def counters_consistent(self) -> bool: ...

    @property
    def sqlite_integrity_ok(self) -> bool: ...

class IndexCheckResult:
    @property
    def ok(self) -> bool: ...

    @property
    def orphaned_files(self) -> int: ...

    @property
    def dangling_rows(self) -> int: ...

    @property
    def size_mismatches(self) -> int: ...

    @property
    def counters_consistent(self) -> bool: ...

    @property
    def sqlite_integrity_ok(self) -> bool: ...

class CacheTransactionGuard:
    def commit(self) -> bool: ...

    def rollback(self) -> bool: ...

class IndexTransactionGuard:
    def commit(self) -> bool: ...

    def rollback(self) -> bool: ...

class Timeout(RuntimeError):
    pass

class Cache:
    def __init__(self, cache_path: str = '.cache/', max_size: int = 0, timeout: float = 600.0) -> None: ...

    def count(self) -> int:
        """Number of entries (O(1): a counter kept in the database)."""

    def __len__(self) -> int: ...

    def set(self, key: str, value: object, expire: datetime.timedelta | float | None = None, tag: str | None = None) -> None: ...

    def __setitem__(self, key: str, value: object) -> None: ...

    def max_blob_size(self) -> int:
        """
        Values up to this many bytes are stored in the database, bigger ones as
        files. Set with the ``max_blob_size`` constructor argument (default 8192).
        """

    def get(self, key: str) -> Buffer | None: ...

    def __getitem__(self, key: str) -> Buffer | None: ...

    def keys(self) -> list[str]: ...

    def iterkeys(self) -> CacheKeyCursor: ...

    def exists(self, key: str) -> bool:
        """True if ``key`` is stored (and, for a Cache, not expired)."""

    def add(self, key: str, value: object, expire: datetime.timedelta | float | None = None, tag: str | None = None) -> bool: ...

    def delete(self, key: str) -> bool: ...

    def pop(self, key: str) -> Buffer | None: ...

    def touch(self, key: str, expire: datetime.timedelta | float | None = None) -> bool: ...

    def expire_and_tag(self, key: str) -> tuple[float | None, str | None] | None:
        """The (expire_time, tag) of ``key``, or None if it is missing."""

    def expire(self) -> None: ...

    def evict(self) -> int: ...

    def evict_tag(self, tag: str) -> int:
        """Remove every entry stored with ``tag``. Returns how many were removed."""

    def incr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def decr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def clear(self) -> None:
        """Remove every entry."""

    def check(self, fix: bool = False) -> CacheCheckResult:
        """
        Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.
        """

    def set_meta(self, key: str, value: str) -> None:
        """Store a string in the store's own metadata table (not a cache entry)."""

    def get_meta(self, key: str) -> str | None:
        """
        A value from the store's own metadata table (not a cache entry), or None.
        """

    def size(self) -> int:
        """Total size of the stored values, in bytes."""

    def volume(self) -> int:
        """Bytes used on disk: the database plus the value files."""

    def set_max_cache_size(self, value: int) -> None:
        """
        Set the LRU size limit in bytes (0 = unlimited). For a FanoutCache
        the limit applies to each shard.
        """

    def path(self) -> str:
        """The directory of the store."""

    def stats(self) -> dict:
        """Hit and miss counters, as ``{"hits": ..., "misses": ...}``."""

    def reset_stats(self) -> None:
        """Reset the hit and miss counters to zero."""

    def close(self) -> bool:
        """
        Close the cache's SQLite connection and stop its background thread.

        Not required: this already happens automatically when the object is
        garbage-collected. Provided for diskcache-compatible cleanup code
        (an explicit close() in a finally: block). `with cache:` does NOT
        call this. Safe to call more than once. Any operation on the cache
        after calling this raises RuntimeError.
        """

    def begin_user_transaction(self) -> CacheTransactionGuard: ...

class Index:
    def __init__(self, path: str = '.index/') -> None: ...

    def count(self) -> int:
        """Number of entries (O(1): a counter kept in the database)."""

    def __len__(self) -> int: ...

    def set(self, key: str, value: object) -> None: ...

    def __setitem__(self, key: str, value: object) -> None: ...

    def max_blob_size(self) -> int:
        """
        Values up to this many bytes are stored in the database, bigger ones as
        files. Set with the ``max_blob_size`` constructor argument (default 8192).
        """

    def get(self, key: str) -> Buffer | None: ...

    def __getitem__(self, key: str) -> Buffer | None: ...

    def keys(self) -> list[str]: ...

    def iterkeys(self) -> IndexKeyCursor: ...

    def exists(self, key: str) -> bool:
        """True if ``key`` is stored (and, for a Cache, not expired)."""

    def add(self, key: str, value: object) -> bool: ...

    def delete(self, key: str) -> bool: ...

    def pop(self, key: str) -> Buffer | None: ...

    def incr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def decr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def clear(self) -> None:
        """Remove every entry."""

    def check(self, fix: bool = False) -> IndexCheckResult:
        """
        Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.
        """

    def size(self) -> int:
        """Total size of the stored values, in bytes."""

    def volume(self) -> int:
        """Bytes used on disk: the database plus the value files."""

    def set_meta(self, key: str, value: str) -> None:
        """Store a string in the store's own metadata table (not a cache entry)."""

    def get_meta(self, key: str) -> str | None:
        """
        A value from the store's own metadata table (not a cache entry), or None.
        """

    def path(self) -> str:
        """The directory of the store."""

    def close(self) -> bool:
        """
        Close the cache's SQLite connection and stop its background thread.

        Not required: this already happens automatically when the object is
        garbage-collected. Provided for diskcache-compatible cleanup code
        (an explicit close() in a finally: block). `with cache:` does NOT
        call this. Safe to call more than once. Any operation on the cache
        after calling this raises RuntimeError.
        """

    def begin_user_transaction(self) -> IndexTransactionGuard: ...

class FanoutCache:
    def __init__(self, cache_path: str = '.cache/', shard_count: int = 8, max_size: int = 0, timeout: float = 600.0) -> None: ...

    def count(self) -> int:
        """Number of entries (O(1): a counter kept in the database)."""

    def __len__(self) -> int: ...

    def set(self, key: str, value: object, expire: datetime.timedelta | float | None = None, tag: str | None = None) -> None: ...

    def __setitem__(self, key: str, value: object) -> None: ...

    def max_blob_size(self) -> int:
        """
        Values up to this many bytes are stored in the database, bigger ones as
        files. Set with the ``max_blob_size`` constructor argument (default 8192).
        """

    def get(self, key: str) -> Buffer | None: ...

    def __getitem__(self, key: str) -> Buffer | None: ...

    def keys(self) -> list[str]: ...

    def iterkeys(self) -> FanoutCacheKeyCursor: ...

    def exists(self, key: str) -> bool:
        """True if ``key`` is stored (and, for a Cache, not expired)."""

    def add(self, key: str, value: object, expire: datetime.timedelta | float | None = None, tag: str | None = None) -> bool: ...

    def delete(self, key: str) -> bool: ...

    def pop(self, key: str) -> Buffer | None: ...

    def touch(self, key: str, expire: datetime.timedelta | float | None = None) -> bool: ...

    def expire_and_tag(self, key: str) -> tuple[float | None, str | None] | None:
        """The (expire_time, tag) of ``key``, or None if it is missing."""

    def expire(self) -> None: ...

    def evict(self) -> int: ...

    def evict_tag(self, tag: str) -> int:
        """Remove every entry stored with ``tag``. Returns how many were removed."""

    def incr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def decr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def clear(self) -> None:
        """Remove every entry."""

    def check(self, fix: bool = False) -> CacheCheckResult:
        """
        Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.
        """

    def set_meta(self, key: str, value: str) -> None:
        """Store a string in the store's own metadata table (not a cache entry)."""

    def get_meta(self, key: str) -> str | None:
        """
        A value from the store's own metadata table (not a cache entry), or None.
        """

    def size(self) -> int:
        """Total size of the stored values, in bytes."""

    def volume(self) -> int:
        """Bytes used on disk: the database plus the value files."""

    def shard_count(self) -> int:
        """Number of shards."""

    def set_max_cache_size(self, value: int) -> None:
        """
        Set the LRU size limit in bytes (0 = unlimited). For a FanoutCache
        the limit applies to each shard.
        """

    def path(self) -> str:
        """The directory of the store."""

    def stats(self) -> dict:
        """Hit and miss counters, as ``{"hits": ..., "misses": ...}``."""

    def reset_stats(self) -> None:
        """Reset the hit and miss counters to zero."""

    def close(self) -> bool:
        """
        Close the cache's SQLite connection and stop its background thread.

        Not required: this already happens automatically when the object is
        garbage-collected. Provided for diskcache-compatible cleanup code
        (an explicit close() in a finally: block). `with cache:` does NOT
        call this. Safe to call more than once. Any operation on the cache
        after calling this raises RuntimeError.
        """

    def begin_user_transaction(self, key: str) -> CacheTransactionGuard: ...

class FanoutIndex:
    def __init__(self, path: str = '.index/', shard_count: int = 8) -> None: ...

    def count(self) -> int:
        """Number of entries (O(1): a counter kept in the database)."""

    def __len__(self) -> int: ...

    def set(self, key: str, value: object) -> None: ...

    def __setitem__(self, key: str, value: object) -> None: ...

    def max_blob_size(self) -> int:
        """
        Values up to this many bytes are stored in the database, bigger ones as
        files. Set with the ``max_blob_size`` constructor argument (default 8192).
        """

    def get(self, key: str) -> Buffer | None: ...

    def __getitem__(self, key: str) -> Buffer | None: ...

    def keys(self) -> list[str]: ...

    def iterkeys(self) -> FanoutIndexKeyCursor: ...

    def exists(self, key: str) -> bool:
        """True if ``key`` is stored (and, for a Cache, not expired)."""

    def add(self, key: str, value: object) -> bool: ...

    def delete(self, key: str) -> bool: ...

    def pop(self, key: str) -> Buffer | None: ...

    def incr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def decr(self, key: str, delta: int = 1, default_value: int = 0) -> int: ...

    def clear(self) -> None:
        """Remove every entry."""

    def check(self, fix: bool = False) -> IndexCheckResult:
        """
        Verify the store: SQLite integrity, dangling rows, orphaned files, sizes
        and counters. With ``fix=True``, also repair what it can.
        """

    def size(self) -> int:
        """Total size of the stored values, in bytes."""

    def volume(self) -> int:
        """Bytes used on disk: the database plus the value files."""

    def shard_count(self) -> int:
        """Number of shards."""

    def set_meta(self, key: str, value: str) -> None:
        """Store a string in the store's own metadata table (not a cache entry)."""

    def get_meta(self, key: str) -> str | None:
        """
        A value from the store's own metadata table (not a cache entry), or None.
        """

    def path(self) -> str:
        """The directory of the store."""

    def close(self) -> bool:
        """
        Close the cache's SQLite connection and stop its background thread.

        Not required: this already happens automatically when the object is
        garbage-collected. Provided for diskcache-compatible cleanup code
        (an explicit close() in a finally: block). `with cache:` does NOT
        call this. Safe to call more than once. Any operation on the cache
        after calling this raises RuntimeError.
        """

    def begin_user_transaction(self, key: str) -> IndexTransactionGuard: ...
