=============
API reference
=============

.. currentmodule:: pysciqlop_cache

This page lists everything ``pysciqlop_cache`` offers. For explanations and examples,
see :doc:`quickstart`, :doc:`stores` and :doc:`serializers`.

Stores
======

.. autoclass:: Cache
    :members: set, get, pop, add, delete, exists, touch, incr, decr, memoize, transact,
              lock, keys, iterkeys, expire, evict, evict_tag, expire_and_tag, clear,
              check, count, size, volume, stats, reset_stats, set_max_cache_size,
              serializer, path, get_meta, set_meta, close

.. autoclass:: FanoutCache
    :members: shard_count, transact

    Same methods as :class:`Cache`, each routed to the shard of its key. Whole-store
    methods (``count``, ``clear``, ``check``, iteration, ...) visit every shard.

.. autoclass:: Index
    :members: set, get, pop, add, delete, exists, incr, decr, transact, keys, iterkeys,
              items, values, peekitem, popitem, clear, check, count, size, volume,
              serializer, path, get_meta, set_meta, close

    Also a :class:`collections.abc.MutableMapping`: ``update()``, ``setdefault()``,
    ``in``, ``len()`` and iteration work like on a dict.

.. autoclass:: FanoutIndex
    :members: shard_count, transact

    Same methods as :class:`Index`, each routed to the shard of its key.

Locks and errors
================

.. autoclass:: Lock
    :members: acquire, release, locked

.. autoexception:: Timeout

Serializers
===========

.. autoclass:: Serializer
    :members: dumps, loads

.. autoclass:: PickleSerializer

.. autoclass:: PickleOOBSerializer
    :members: dumps_chunks

.. autoclass:: MsgspecSerializer

Migration
=========

.. autofunction:: pysciqlop_cache.migrate.migrate
