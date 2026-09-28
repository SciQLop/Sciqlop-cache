=====================
Coming from diskcache
=====================

SciQLop Cache follows the `diskcache <https://grantjenks.com/docs/diskcache/>`_ API.
Most code written for ``diskcache.Cache``, ``FanoutCache`` and ``Index`` runs unchanged
after swapping the import.

Migrating a cache
=================

Existing data moves over with one command. Keys keep their type, and expiration
times and tags are kept:

.. code-block:: console

    $ python -m pysciqlop_cache.migrate /path/to/diskcache /path/to/sciqlop-cache

    # move instead of copy: entries leave the source as they are copied
    $ python -m pysciqlop_cache.migrate --drop /old/diskcache /new/sciqlop-cache

    # an Index
    $ python -m pysciqlop_cache.migrate --type index /old/index /new/index

Or from Python:

.. code-block:: python

    import diskcache
    from pysciqlop_cache import Cache
    from pysciqlop_cache.migrate import migrate

    old = diskcache.Cache("old-cache")
    old.set("a", 1, expire=3600, tag="demo")
    old["b"] = [1, 2, 3]
    old.close()

    result = migrate("old-cache", "new-cache")
    print(result["migrated"], result["errors"])
    # 2 0
    print(Cache("new-cache").get("a", tag=True))
    # (1, 'demo')

A FanoutCache source becomes a FanoutCache with the same number of shards. Entries that
can't be unpickled (for instance, written with an incompatible numpy version) are
skipped with a warning instead of stopping the migration. The migration checks the free
disk space first.

What works the same
===================

Keys
    Any hashable key works: ``str``, ``int``, ``float``, ``bytes``, ``tuple``,
    ``frozenset``, ... ``bool`` keys are stored as ``int`` (``True`` and ``1`` are the same
    key), like diskcache. A ``str`` key starting with ``"\x00"`` is reserved.

``get()`` / ``pop()`` with metadata
    ``expire_time=True`` and ``tag=True`` return the same tuples as diskcache:

    .. code-block:: python

        cache = Cache("compat-cache")
        cache.set("k", "v", expire=60, tag="t")
        value, expire_time, tag = cache.get("k", expire_time=True, tag=True)
        print(value, tag)
        # v t

Constructors
    ``directory``, ``size_limit``, ``shards`` and ``timeout`` are accepted as aliases,
    and ``Index("path", {"a": 1}, b=2)`` seeds the index like diskcache. diskcache's tuning
    arguments (``statistics``, ``tag_index``, ``eviction_policy``, ``cull_limit``,
    ``sqlite_*``, ``disk_*``) are accepted and ignored.

``retry=``
    Accepted on every method and ignored: calls already wait up to ``timeout`` on a busy
    database, so there is no fail-and-retry mode to opt into.

``Index`` and ``FanoutIndex``
    They implement :class:`collections.abc.MutableMapping`: ``items()``, ``values()``,
    ``update()``, ``setdefault()``, ``popitem()`` and ``peekitem()`` all work.

``KeyError``
    ``cache["missing"]`` and ``del cache["missing"]`` raise :class:`KeyError`.
    ``Index.pop("missing")`` raises unless you give a default. ``Cache.pop("missing")``
    returns ``None``, like diskcache.

``Timeout``
    :class:`pysciqlop_cache.Timeout` (a :class:`RuntimeError`) is raised when the database
    stays locked longer than ``timeout``.

What differs
============

- ``Cache()`` without a directory uses ``./.cache/``, not a temporary directory.
- There is no default size limit (diskcache's is 1 GiB).
- ``close()`` is a real, final shutdown, and ``with cache:`` does not call it. Using a
  closed cache raises :class:`RuntimeError`. You never need to call it.
- ``evict()`` is the size-limit cleanup. diskcache's ``evict(tag)`` is ``evict_tag(tag)``.
- ``FanoutCache.transact()`` takes a key: transactions are per shard.
- ``stats()`` returns a dict.
- ``peekitem()`` / ``popitem()`` follow key order, not insertion order (for a ``FanoutIndex``:
  key order within each shard, shard after shard).
- ``disk=`` is rejected: use ``serializer=`` (see :doc:`serializers`).
- Not implemented: ``Deque``, ``RLock``, throttling, barriers, ``push``/``pull``/``peek``
  and ``read=True`` file handles (which raise :class:`NotImplementedError`).
