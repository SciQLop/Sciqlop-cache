============
Installation
============

From PyPI
=========

.. code-block:: console

    $ pip install pysciqlop-cache

Wheels are published for Linux (x86-64 and ARM64, glibc and musl), macOS (Intel and
Apple Silicon) and Windows, for Python 3.10 to 3.14, including the free-threaded 3.14t
build. They have no runtime dependency. They bundle SQLite and the
`blosc2 <https://www.blosc.org/>`_ compression library.

Optional packages:

- ``numpy``, for storing arrays (see :doc:`serializers`);
- ``msgspec``, for the :class:`~pysciqlop_cache.MsgspecSerializer`.

From source
===========

You need a C++20 compiler and
`meson <https://mesonbuild.com/>`_. All other dependencies are vendored.

.. code-block:: console

    $ pip install .

For development, build and test with meson directly:

.. code-block:: console

    $ meson setup build -Dwith_tests=true
    $ meson compile -C build
    $ meson test -C build                    # full suite (includes stress tests)
    $ meson test -C build --no-suite slow    # the quick loop

Build options
-------------

``-Dwith_blosc2=false``
    Build without blosc2. Arrays are then always stored uncompressed, and values
    written compressed by another build raise an error on load.

``-Dwith_torture_tests=true``
    Adds the long stress and fuzz suites (meant for sanitizer builds).

``-Ddisable_python_wrapper=true``
    Only build the C++ library.
