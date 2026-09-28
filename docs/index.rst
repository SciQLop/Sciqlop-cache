=============
SciQLop Cache
=============

**A fast, persistent, process-safe key-value cache for Python, with a C++20 core.**

Think of it as a tiny local database for things that are expensive to compute or to
download. You store Python objects under keys, and any thread or process on the machine
can read them back, safely, even at the same time. Data survives restarts. The cache can
be bounded in size, entries can expire, and nothing gets corrupted when processes crash,
fork, or race each other.

.. code-block:: python

    from pysciqlop_cache import Cache

    cache = Cache("my-cache")
    cache["sensor/temperature"] = {"ts": 1710000000, "values": [21.3, 21.5, 21.4]}

    print(cache["sensor/temperature"])
    # {'ts': 1710000000, 'values': [21.3, 21.5, 21.4]}

.. code-block:: console

    $ pip install pysciqlop-cache

It follows the `diskcache <https://grantjenks.com/docs/diskcache/>`_ API, so most
diskcache code runs unchanged, and it is 2x faster on writes and up to 6x faster in
batched transactions.

What do you want to do?
=======================

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: ⚡ Cache results in a script
        :link: quickstart
        :link-type: doc

        Store and fetch values like in a dict, with expiration, size limits and
        one-line memoization of your functions.

    .. grid-item-card:: 🔀 Share a cache between threads and processes
        :link: concurrency
        :link-type: doc

        Worker pools, forked processes, GUI threads: transactions, locks, and what
        is safe to do at the same time.

    .. grid-item-card:: 📈 Store big numpy arrays
        :link: serializers
        :link-type: doc

        Measurement series, images, spectrograms: write them without blocking your
        other threads, and let the cache compress what compresses well.

    .. grid-item-card:: 🗂️ Pick the right store
        :link: stores
        :link-type: doc

        ``Cache``, ``Index``, ``FanoutCache`` or ``FanoutIndex``: what each one is
        for.

    .. grid-item-card:: 🔁 Move from diskcache
        :link: diskcache
        :link-type: doc

        Migrate an existing cache in one command, and see where the two APIs still
        differ.

    .. grid-item-card:: 🧱 Use it from C++
        :link: cpp
        :link-type: doc

        The same engine, header-only, from C++20.

.. toctree::
    :maxdepth: 1
    :caption: Getting started

    installation
    quickstart

.. toctree::
    :maxdepth: 1
    :caption: Guides

    stores
    concurrency
    serializers
    diskcache
    cpp

.. toctree::
    :maxdepth: 1
    :caption: Reference

    api
    performance
    internals
    faq
