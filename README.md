[![GitHub License](https://img.shields.io/github/license/SciQLop/Sciqlop-cache)](https://mit-license.org/)
[![CPP20](https://img.shields.io/badge/Language-C++20-blue.svg)]()
[![PyPi](https://img.shields.io/pypi/v/Sciqlop-cache.svg)](https://pypi.python.org/pypi/Sciqlop-cache)
[![Coverage](https://codecov.io/gh/SciQLop/Sciqlop-cache/coverage.svg?branch=main)](https://codecov.io/gh/SciQLop/Sciqlop-cache/branch/main)
# SciQLop Cache

SciQLop Cache is a C++/Python library for fast, persistent, and concurrent caching of binary and text data. It is designed for scientific and general-purpose applications that require efficient storage and retrieval of key-value pairs, with support for expiration, eviction, and multi-process access.

## Main Features

- **Persistent cache**: Stores data on disk using SQLite (small values as BLOBs, large values as files).
- **Binary and text support**: Handles arbitrary byte buffers and strings.
- **No-expiry default**: Entries persist until explicitly deleted or evicted. Optional time-based expiration.
- **LRU eviction**: Set `max_size` in bytes to automatically evict least-recently-used entries.
- **Tags**: Group cache entries with tags for bulk eviction.
- **Multi-process safe**: WAL mode with busy timeout for concurrent access.
- **Python bindings**: Easy integration with Python via `pysciqlop_cache`.

## User API (C++)

All user-facing functions are provided by the `Cache` class in `include/sciqlop_cache/sciqlop_cache.hpp`. Below are the most useful methods:

### Construction

```cpp
Cache(const std::filesystem::path &cache_path = ".cache/", size_t max_size = 0);
```

Creates a cache at the given path. `max_size` is in bytes (0 = unlimited, default). When set, LRU eviction runs automatically in the background.

Basic Operations

* Set a value
```cpp
cache.set(key, value);                      // never expires
cache.set(key, value, expire_duration);     // with expiration
cache.set(key, value, "mytag");             // with tag
cache.set(key, value, expire_duration, "mytag"); // both
```

* Get a value
```cpp
auto result = cache.get(key); // returns std::optional<Buffer>
```

* Add a value only if not present
```cpp
cache.add(key, value);
cache.add(key, value, expire_duration);
cache.add(key, value, "mytag");
cache.add(key, value, expire_duration, "mytag");
```

* Delete a value
```cpp
cache.del(key);
```

* Pop a value (get and delete)
```cpp
auto result = cache.pop(key);
```

* Check if a key exists
```cpp
cache.exists(key);
```

* List all keys
```cpp
auto keys = cache.keys(); // returns std::vector<std::string>
```

* Count items
```cpp
size_t n = cache.count();
```
Expiration and Eviction

* Touch (update expiration)
```cpp
cache.touch(key, expire_duration);
```

* Expire (remove expired items)
```cpp
cache.expire();
```

* Evict (LRU removal when over max_size)
```cpp
cache.evict();
```

* Evict by tag (remove all entries with a given tag)
```cpp
cache.evict_tag("mytag"); // returns number of entries removed
```

* Clear all items
```cpp
cache.clear();
```
Validation

* Check cache validity
```cpp
cache.check();
```

## Python API

The Python API is provided by the pysciqlop_cache module. The main class is Cache, which mirrors the C++ API:

```py
from pysciqlop_cache import Cache

cache = Cache("path/to/cache", max_size=1_000_000_000)  # 1GB limit, 0 = unlimited
cache.set("key", "value")                       # stores any picklable object
cache.set("key", "value", expire=3600)           # expire in 1 hour
cache.set("key", "value", tag="group1")          # with tag
cache.set("key", "value", expire=3600, tag="group1")  # both
value = cache.get("key")
cache.delete("key")
cache.evict_tag("group1")                        # bulk remove by tag
cache.keys()
cache.count()
cache.expire()
cache.clear()
```

## Example Usage

C++
```cpp
Cache cache(".cache/");
cache.set("mykey", std::vector<char>{'a', 'b', 'c'});
auto value = cache.get("mykey");
if (value) {
    // use *value
}
```

Python
```py
from pysciqlop_cache import Cache

cache = Cache(".cache/")
cache.set("mykey", [1, 2, 3])              # any picklable object
cache.set("sensor/temp", data, tag="sensor")
value = cache.get("mykey")
if value is not None:
    # use value
cache.evict_tag("sensor")                  # remove all sensor data
```


## Scaling

Latency stays nearly flat from 100 to 1M cache entries (256-byte values):

![Scaling benchmark](benchmark/scaling_chart.png)

Reproduce with:

```bash
PYTHONPATH=build python benchmark/scaling.py --max-entries 1000000 > results.csv
python benchmark/plot_scaling.py results.csv -o benchmark/scaling_chart.png
```

MIT License

## Contact
For questions or contributions, please open an issue or pull request on GitHub.
