# SciQLop Cache

SciQLop Cache is a C++/Python library for fast, persistent, and concurrent caching of binary and text data. It is designed for scientific and general-purpose applications that require efficient storage and retrieval of key-value pairs, with support for expiration, eviction, and multi-process access.

## Main Features

- **Persistent cache**: Stores data on disk using SQLite and files.
- **Binary and text support**: Handles arbitrary byte buffers and strings.
- **Expiration and eviction**: Supports time-based expiration and manual eviction.
- **Multi-process safe**: Can be used from multiple processes.
- **Python bindings**: Easy integration with Python via `pysciqlop_cache`.

## User API (C++)

All user-facing functions are provided by the `Cache` class in `include/sciqlop_cache/sciqlop_cache.hpp`. Below are the most useful methods:

### Construction

```cpp
Cache(const std::filesystem::path &cache_path = ".cache/", size_t max_size = 1000);
```

Creates a cache at the given path, with an optional maximum size.

Basic Operations

* Set a value
```cpp
cache.set(key, value); // value can be std::string, std::vector<char>, etc.
cache.set(key, value, expire_duration); // set with expiration
```

* Get a value
```cpp
auto result = cache.get(key); // returns std::optional<std::vector<char>>
```

* Add a value only if not present
```cpp
cache.add(key, value);
cache.add(key, value, expire_duration);
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

* Evict (manual removal, policy not implemented)
```cpp
cache.evict();
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

cache = Cache("path/to/cache", max_size=1000)
cache.set("key", b"value")
value = cache.get("key")
cache.delete("key")
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
cache.set("mykey", b"abc")
value = cache.get("mykey")
if value is not None:
    # use value
```

<vscode_annotation details='%5B%7B%22title%22%3A%22hardcoded-credentials%22%2C%22description%22%3A%22Embedding%20credentials%20in%20source%20code%20risks%20unauthorized%20access%22%7D%5D'>##</vscode_annotation> License

MIT License

## Contact
For questions or contributions, please open an issue or pull request on GitHub.
