=================
Choosing a store
=================

There are four store types. They share the same storage engine and most of the API.

.. list-table::
    :header-rows: 1
    :widths: 20 45 35

    * - Store
      - What it adds
      - Use it for
    * - :class:`~pysciqlop_cache.Cache`
      - Expiration, tags, LRU size limit, hit and miss statistics, memoization, locks.
      - The default: caching results and downloads.
    * - :class:`~pysciqlop_cache.Index`
      - Nothing: a persistent ``dict`` (it implements
        :class:`collections.abc.MutableMapping`).
      - Persistent lookup tables, catalogs, state. Slightly less work per write.
    * - :class:`~pysciqlop_cache.FanoutCache`
      - A ``Cache`` split into N independent shards.
      - Many processes writing heavily at the same time.
    * - :class:`~pysciqlop_cache.FanoutIndex`
      - An ``Index`` split into N shards.
      - Same, for an ``Index``.

Index
=====

.. code-block:: python

    from pysciqlop_cache import Index

    catalog = Index("catalog", {"dataset/v1": {"rows": 10}}, dataset_v2={"rows": 20})
    catalog["dataset/v3"] = {"rows": 30}

    print(sorted(catalog.keys()))
    print(catalog.peekitem())          # last item, in key order
    print(catalog.pop("dataset/v1"))

``pop()`` on a missing key raises :class:`KeyError` unless you give a default, like a
dict. ``peekitem()`` and ``popitem()`` follow the key order, not the insertion order. On a
``FanoutIndex`` that order is per shard: the last item of the last shard, not the largest key.

Why shard: FanoutCache
======================

Each store is one SQLite database, and SQLite has one writer at a time. Readers never
wait, but writers take turns. When many processes write heavily, spread the keys over
several independent databases:

.. code-block:: python

    from pysciqlop_cache import FanoutCache

    sharded = FanoutCache("sharded", shard_count=8, max_size=1_000_000_000)
    sharded["key"] = "value"                  # stored in shard hash(key) % 8

    with sharded.transact("key"):             # a transaction on that key's shard
        sharded["key"] = sharded["key"].upper()
    print(sharded["key"])

Things to know:

- ``max_size`` applies to each shard.
- ``transact(key)`` and ``lock(key)`` work on one shard. There are no cross-shard
  transactions.
- Whole-store operations (``len``, ``clear``, ``check``, iteration) visit every shard.

For a single process, or a few writers, a plain ``Cache`` is simpler.

Where values go: ``max_blob_size``
==================================

Values up to ``max_blob_size`` bytes (8 KB by default) are stored inside the database;
bigger ones get their own file. Every store type takes it:

.. code-block:: python

    from pysciqlop_cache import Cache

    few_files = Cache("few-files", max_blob_size=256 * 1024)
    few_files["day"] = b"x" * 100_000          # stays in the database
    print(few_files.max_blob_size())
    # 262144

The value is recorded in the store: reopening it without ``max_blob_size`` keeps it,
passing a new one replaces it. Changing it never moves existing values; each one stays
where it was written, and reads don't care.

Raising it trades files for database size:

- fewer files and directory entries, which matters on filesystems with a file quota or
  few inodes, and one write instead of a file creation per value;
- but every value up to the threshold goes through SQLite's write-ahead log and is
  copied on each read, where a file is memory-mapped, and the database file grows by
  the whole value.

``max_blob_size=0`` stores every non-empty value as a file. diskcache's
``disk_min_file_size`` is accepted too (the smallest value stored as a file, so one byte
more than ``max_blob_size``). :doc:`performance` measures the trade-off.
