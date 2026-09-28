=========
C++ guide
=========

The Python package is a thin layer over a C++20 library. You can use that library
directly. It is header-only on the sciqlop-cache side; its dependencies (SQLite, fmt,
stduuid, ...) come with it as meson subprojects.

.. code-block:: cpp

    #include "sciqlop_cache/sciqlop_cache.hpp"
    using namespace std::chrono_literals;

    Cache cache(".cache/", /*max_size=*/1'000'000'000);

    std::vector<char> data(1024, 'x');
    cache.set("key", data);                  // any contiguous byte range
    cache.set("key", data, 60s);             // with an expiration
    cache.set("key", data, "mytag");         // with a tag

    std::optional<Buffer> value = cache.get("key");
    if (value)
        use(value->data(), value->size());   // memory-mapped for big values

    cache.del("key");
    cache.pop("key");                        // get + delete
    cache.add("key", data);                  // only if absent
    cache.touch("key", 120s);
    cache.touch("key");                      // drop the expiration
    cache.evict_tag("mytag");
    cache.incr("counter", /*delta=*/1, /*default_value=*/0);

The store types
===============

They are the same four as in Python (see :doc:`stores`):

.. code-block:: cpp

    Cache cache(".cache/");                              // expiration, LRU, tags, stats
    Index index(".index/");                              // bare key-value store
    FanoutCache fanout(".fanout/", /*shard_count=*/8, /*max_size=*/0);
    FanoutIndex fanout_index(".fanout-index/", /*shard_count=*/8);

``Cache`` and ``Index`` are the same class template, ``_Store<DiskStorage,
Policies...>``, with different policies. ``Index`` has none, so it pays nothing for
expiration, eviction or tags. Methods that need a policy, like ``touch()`` or
``evict_tag()``, only exist on the types that have it: calling them on an ``Index`` is a
compile error, not a runtime one.

Every constructor also takes ``busy_timeout_ms`` (600 000 by default): how long a call
waits for a locked database before throwing.

Errors
======

- A value that can't be written (full disk, quota, permissions) throws
  ``std::system_error`` with the real ``errno``.
- A database locked longer than the busy timeout throws ``busy_error``.
- Any call on a closed store throws ``std::runtime_error``.

Adding it to your project
=========================

With meson, add a wrap file, ``subprojects/sciqlop-cache.wrap``:

.. code-block:: ini

    [wrap-git]
    url = https://github.com/SciQLop/Sciqlop-cache.git
    revision = main
    depth = 1

and use it from your ``meson.build``:

.. code-block:: meson

    sciqlop_cache_dep = subproject('sciqlop-cache',
        default_options: ['disable_python_wrapper=true']
    ).get_variable('sciqlop_cache_dep')

    executable('app', 'main.cpp', dependencies: [sciqlop_cache_dep])

Pin ``revision`` to a tag or a commit for reproducible builds.
