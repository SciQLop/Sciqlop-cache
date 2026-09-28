==========
Quickstart
==========

A cache is a directory. Open it, and use it like a dict:

.. code-block:: python

    from pysciqlop_cache import Cache

    cache = Cache("my-cache")

    cache["answer"] = 42                 # set
    print(cache["answer"])               # get, raises KeyError if missing
    print(cache.get("missing"))          # get, None if missing
    print("answer" in cache, len(cache))
    del cache["answer"]                  # delete, raises KeyError if missing

Values can be any picklable Python object. Keys can be strings, numbers, bytes or
tuples of those:

.. code-block:: python

    cache[("sensor", 7)] = {"unit": "degC", "values": [21.3, 21.5]}
    print(list(cache))

Open the same directory again, from this process or another one, and the data is there:

.. code-block:: python

    same = Cache("my-cache")
    print(same[("sensor", 7)])

Expiration and tags
===================

.. code-block:: python

    cache.set("session/abc", "token", expire=3600)      # gone in one hour
    cache.set("sensor/temp", 21.3, tag="sensor")        # tagged
    cache.set("sensor/hum", 45.0, expire=600, tag="sensor")

    cache.touch("session/abc", expire=7200)             # new lifetime
    cache.touch("session/abc")                          # never expires anymore

    print(cache.evict_tag("sensor"))                    # removes both sensor entries
    # 2

``expire`` takes seconds (int or float) or a :class:`datetime.timedelta`. Expired
entries are invisible right away, and a background thread deletes them.

Bounded caches
==============

.. code-block:: python

    bounded = Cache("bounded-cache", max_size=1_000_000_000)   # 1 GB

When the stored values exceed ``max_size`` (in bytes), a background thread evicts the
least-recently-used entries. The limit is recorded in the cache: reopening it without
``max_size`` keeps it.

Memoization
===========

Cache the results of a function in one line:

.. code-block:: python

    calls = []

    @cache.memoize(expire=300, tag="compute")
    def slow_square(x):
        calls.append(x)
        return x * x

    print(slow_square(4), slow_square(4), calls)
    # 16 16 [4]

The key is made of the function's module and name plus a hash of its arguments.
``typed=True`` stores ``f(1)`` and ``f(1.0)`` separately. ``version_aware=True`` adds a
hash of the function's bytecode, so changing the function invalidates its old results.

Counters and transactions
=========================

.. code-block:: python

    print(cache.incr("page_views"))            # 1
    print(cache.incr("page_views", delta=5))   # 6
    print(cache.decr("page_views"))            # 5

    cache["balance"] = 100
    with cache.transact():
        cache["balance"] = cache["balance"] - 30
        cache["log"] = "withdrew 30"
    print(cache["balance"])
    # 70

A ``transact()`` block commits when it ends and rolls back if it raises. Other threads
and processes never see half of it. See :doc:`concurrency` for locks and for what runs
in parallel.

Keeping an eye on it
====================

.. code-block:: python

    print(cache.stats())      # hit and miss counters
    print(cache.volume())     # bytes on disk, database and value files
    print(cache.size())       # bytes of stored values only

    result = cache.check()    # verify the structure
    print(result.ok)
    # True

``check(fix=True)`` also repairs what it finds: orphaned files, rows pointing at missing
files, drifted counters.

Closing
=======

You don't need to close a cache: it closes itself when it is garbage-collected.
``close()`` exists for code written for diskcache. Closing is final: using the cache
afterwards raises :class:`RuntimeError`, and ``with cache:`` does not close it.

.. code-block:: python

    same.close()
