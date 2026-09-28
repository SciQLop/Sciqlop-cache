=====================================
Threads, processes and transactions
=====================================

A cache can be used by many threads and many processes at once, with no setup and no
lock files. This page explains what is safe, what runs in parallel, and how to make
several operations atomic.

Threads
=======

Share one ``Cache`` object between your threads:

.. code-block:: python

    from concurrent.futures import ThreadPoolExecutor
    from pysciqlop_cache import Cache

    cache = Cache("shared-cache")

    def work(i):
        cache[f"result/{i}"] = i * i
        return cache[f"result/{i}"]

    with ThreadPoolExecutor(8) as pool:
        print(sum(pool.map(work, range(100))))
    # 328350

One store object runs its database operations one at a time: each call holds an
internal lock while it talks to SQLite. Those steps are short. Turning values into
bytes and back happens outside that lock, and every call releases the GIL while it
waits or works in C++. So a thread busy in the cache never blocks your other Python
threads, like a GUI's event loop. For big numpy arrays,
:class:`~pysciqlop_cache.PickleOOBSerializer` also moves the array bytes without the GIL
(see :doc:`serializers`).

On a free-threaded Python (3.14t), the extension runs without the GIL at all: the
serialization work of your threads runs truly in parallel.

Processes
=========

Several processes can open the same directory. The database runs in SQLite's WAL
mode: a process reading never waits for a process writing. Writers take turns, one at
a time per database. Each ``set``, ``delete`` or ``pop`` is atomic, even across
processes.

.. code-block:: python

    import multiprocessing

    def child():
        Cache("shared-cache")["from-child"] = "hello"

    process = multiprocessing.get_context("fork").Process(target=child)
    process.start()
    process.join()
    print(cache["from-child"])
    # hello

Forked processes work too, including long-lived worker pools that inherit an open cache:
each store detects the fork and reopens its database connection in the child. A cache
opened before ``fork()`` is safe to use on both sides.

If a writer holds the database longer than ``timeout`` (600 s by default), the waiting
call raises :class:`pysciqlop_cache.Timeout`.

Transactions
============

``transact()`` groups operations: they commit together when the block ends, or roll back
together if it raises. Other threads and processes never see a half-done block.

.. code-block:: python

    cache["balance"] = 100

    def withdraw(amount):
        with cache.transact():
            balance = cache["balance"]
            if balance < amount:
                raise ValueError("insufficient funds")
            cache["balance"] = balance - amount

    withdraw(30)
    try:
        withdraw(500)
    except ValueError:
        pass
    print(cache["balance"])
    # 70

Transactions are reentrant: a ``transact()`` block inside another one joins it. So a
helper that uses a transaction can be called from inside another one. A rollback of the
outer block undoes the inner one too.

Keep transactions short: while one is open, other writers wait.

Locks
=====

``lock()`` is a mutual exclusion that works across threads and processes, stored in the
cache itself:

.. code-block:: python

    with cache.lock("nightly-report", expire=30):
        cache["report"] = "generated"
    print(cache["report"])

``expire`` (seconds) bounds how long a crashed holder can keep the lock.

What runs in parallel
=====================

.. list-table::
    :header-rows: 1

    * - Between
      - What happens
    * - Threads sharing one store object
      - Database steps take turns, and a ``transact()`` block holds the store until it
        ends. Serialization, and the array copies of ``PickleOOBSerializer``, run in
        parallel.
    * - Processes (or separate store objects) on the same directory
      - Reads run in parallel with everything. Writes take turns; a ``transact()`` block
        makes other writers wait until it ends.

To let writers run in parallel, split the keys over several stores with a
:class:`~pysciqlop_cache.FanoutCache` (see :doc:`stores`).
