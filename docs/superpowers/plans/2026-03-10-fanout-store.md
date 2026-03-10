# FanoutStore Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `FanoutStore<StoreType>` — a sharding wrapper that distributes keys across N independent `_Store` instances for write concurrency and smaller per-DB item counts.

**Architecture:** Template class owning `vector<unique_ptr<StoreType>>`. Keys routed via `hash(key) % shard_count`. Per-key ops dispatch to one shard; cross-shard ops aggregate. Directory layout: `base_path/00/` through `base_path/07/`.

**Tech Stack:** C++20, Catch2 BDD tests, nanobind Python bindings, Meson build system.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `include/sciqlop_cache/fanout_store.hpp` | Create | `FanoutStore<StoreType>` template |
| `include/sciqlop_cache/sciqlop_cache.hpp` | Modify | Add `FanoutCache`/`FanoutIndex` aliases |
| `tests/fanout/main.cpp` | Create | C++ tests for FanoutCache and FanoutIndex |
| `meson.build` | Modify | Add `fanout` test target |
| `pysciqlop_cache/pysciqlop_cache.cpp` | Modify | Add `FanoutCache`/`FanoutIndex` bindings |
| `tests/python/test_python_interface.py` | Modify | Add Python tests for FanoutCache |

---

## Task 1: FanoutStore header with constructor and per-key ops

**Files:**
- Create: `include/sciqlop_cache/fanout_store.hpp`
- Create: `tests/fanout/main.cpp`
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp`
- Modify: `meson.build`

- [ ] **Step 1: Create fanout_store.hpp with constructor, set, get, del, exists**

Create `include/sciqlop_cache/fanout_store.hpp`:

```cpp
#pragma once

#include <cstddef>
#include <filesystem>
#include <fmt/format.h>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

template <typename StoreType>
class FanoutStore
{
    std::vector<std::unique_ptr<StoreType>> _shards;
    std::size_t _shard_count;

    std::size_t _shard_for(const std::string& key) const
    {
        return std::hash<std::string> {}(key) % _shard_count;
    }

    StoreType& _shard(const std::string& key) { return *_shards[_shard_for(key)]; }

    template <typename F>
    void _for_each_shard(F&& f)
    {
        for (auto& s : _shards)
            f(*s);
    }

public:
    explicit FanoutStore(const std::filesystem::path& path,
                         std::size_t shard_count = 8,
                         std::size_t max_size = 0)
        : _shard_count(shard_count)
    {
        _shards.reserve(_shard_count);
        for (std::size_t i = 0; i < _shard_count; ++i)
        {
            auto shard_path = path / fmt::format("{:02d}", i);
            std::filesystem::create_directories(shard_path);
            _shards.push_back(std::make_unique<StoreType>(shard_path, max_size));
        }
    }

    [[nodiscard]] std::size_t shard_count() const { return _shard_count; }

    [[nodiscard]] bool exists(const std::string& key) { return _shard(key).exists(key); }

    inline bool del(const std::string& key) { return _shard(key).del(key); }

    [[nodiscard]] inline std::optional<Buffer> get(const std::string& key)
    {
        return _shard(key).get(key);
    }

    [[nodiscard]] inline std::optional<Buffer> pop(const std::string& key)
    {
        return _shard(key).pop(key);
    }
};
```

Note: The constructor takes `max_size` but `Index` (aka `_Store<DiskStorage>`) doesn't accept it. We need to handle this. Use `if constexpr` or a helper to detect whether `StoreType` accepts `max_size`. The simplest approach: `StoreType` constructor always takes `(path, max_size)` — `_Store` already does (it's just ignored when there's no `WithEviction`). Check `_Store` constructor signature — it takes `(path, max_size=0)` already, so this works as-is.

- [ ] **Step 2: Add type aliases in sciqlop_cache.hpp**

Add to `include/sciqlop_cache/sciqlop_cache.hpp` after the existing aliases:

```cpp
#include "fanout_store.hpp"

using FanoutCache = FanoutStore<Cache>;
using FanoutIndex = FanoutStore<Index>;
```

- [ ] **Step 3: Write failing test — basic CRUD**

Create `tests/fanout/main.cpp`:

```cpp
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

using namespace std::chrono_literals;

SCENARIO("FanoutCache basic CRUD", "[fanout]")
{
    AutoCleanDirectory db_path { "FanoutTest01" };
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');

    GIVEN("a new FanoutCache with 4 shards")
    {
        FanoutCache fc(db_path.path(), 4);
        REQUIRE(fc.shard_count() == 4);

        WHEN("we set and get a key")
        {
            fc.set("hello", v1);
            auto result = fc.get("hello");
            THEN("we retrieve the same value")
            {
                REQUIRE(result.has_value());
                REQUIRE(result->to_vector() == v1);
            }
        }

        WHEN("we check exists")
        {
            fc.set("hello", v1);
            THEN("exists returns true for present key, false for absent")
            {
                REQUIRE(fc.exists("hello"));
                REQUIRE_FALSE(fc.exists("missing"));
            }
        }

        WHEN("we delete a key")
        {
            fc.set("hello", v1);
            REQUIRE(fc.del("hello"));
            THEN("it is gone") { REQUIRE_FALSE(fc.get("hello").has_value()); }
        }

        WHEN("we pop a key")
        {
            fc.set("hello", v1);
            auto result = fc.pop("hello");
            THEN("we get the value and it is removed")
            {
                REQUIRE(result.has_value());
                REQUIRE(result->to_vector() == v1);
                REQUIRE_FALSE(fc.exists("hello"));
            }
        }
    }
}
```

- [ ] **Step 4: Register test in meson.build**

In `meson.build`, add `'fanout'` to the test loop:

```
foreach test_name:['database', 'basic', 'basic_index', 'intermediate', 'multithreads', 'fanout']
```

- [ ] **Step 5: Build and run test to verify it passes**

Run: `meson compile -C build && meson test -C build sciqlop-cache:fanout`

- [ ] **Step 6: Commit**

```bash
git add include/sciqlop_cache/fanout_store.hpp include/sciqlop_cache/sciqlop_cache.hpp \
       tests/fanout/main.cpp meson.build
git commit -m "feat: add FanoutStore with constructor and per-key ops"
```

---

## Task 2: Add set() overloads with expiration and tags

**Files:**
- Modify: `include/sciqlop_cache/fanout_store.hpp`
- Modify: `tests/fanout/main.cpp`

The `set()` method needs to forward all overloads. We use `if constexpr` with the same `has_policy_v` traits to conditionally expose them, or we can use a simpler forwarding approach since `FanoutStore` wraps a concrete `StoreType`.

- [ ] **Step 1: Add set/add methods to FanoutStore**

Add to `FanoutStore` class (public section):

```cpp
    // --- set() overloads ---

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return _shard(key).set(key, value);
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires requires(StoreType& s) { s.set(key, value, expire); }
    {
        return _shard(key).set(key, value, expire);
    }

    inline bool set(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires requires(StoreType& s) { s.set(key, value, tag); }
    {
        return _shard(key).set(key, value, tag);
    }

    inline bool set(const std::string& key, const Bytes auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s) { s.set(key, value, expire, tag); }
    {
        return _shard(key).set(key, value, expire, tag);
    }

    // --- add() overloads ---

    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return _shard(key).add(key, value);
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires requires(StoreType& s) { s.add(key, value, expire); }
    {
        return _shard(key).add(key, value, expire);
    }

    inline bool add(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires requires(StoreType& s) { s.add(key, value, tag); }
    {
        return _shard(key).add(key, value, tag);
    }

    inline bool add(const std::string& key, const Bytes auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s) { s.add(key, value, expire, tag); }
    {
        return _shard(key).add(key, value, expire, tag);
    }
```

- [ ] **Step 2: Write test for set with expiration and tags**

Add to `tests/fanout/main.cpp`:

```cpp
SCENARIO("FanoutCache set with expiration", "[fanout][expire]")
{
    AutoCleanDirectory db_path { "FanoutTestExpire" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we set a key with expiration")
        {
            fc.set("expkey", v1, 1s);
            REQUIRE(fc.get("expkey").has_value());

            THEN("it expires after the timeout")
            {
                std::this_thread::sleep_for(2s);
                REQUIRE_FALSE(fc.get("expkey").has_value());
            }
        }

        WHEN("we set a key with a tag")
        {
            fc.set("tagged", v1, "mytag");
            REQUIRE(fc.get("tagged").has_value());
        }
    }
}
```

- [ ] **Step 3: Build and run tests**

Run: `meson compile -C build && meson test -C build sciqlop-cache:fanout`

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(fanout): add set/add overloads with expiration and tags"
```

---

## Task 3: Add aggregated operations (count, size, keys, clear)

**Files:**
- Modify: `include/sciqlop_cache/fanout_store.hpp`
- Modify: `tests/fanout/main.cpp`

- [ ] **Step 1: Add aggregated methods**

Add to `FanoutStore`:

```cpp
    [[nodiscard]] std::size_t count()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.count(); });
        return total;
    }

    [[nodiscard]] std::size_t size()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.size(); });
        return total;
    }

    [[nodiscard]] std::vector<std::string> keys()
    {
        std::vector<std::string> all;
        _for_each_shard([&](auto& s) {
            auto k = s.keys();
            all.insert(all.end(), std::make_move_iterator(k.begin()),
                       std::make_move_iterator(k.end()));
        });
        return all;
    }

    void clear()
    {
        _for_each_shard([](auto& s) { s.clear(); });
    }

    bool check()
    {
        bool ok = true;
        _for_each_shard([&](auto& s) { ok &= s.check(); });
        return ok;
    }

    [[nodiscard]] std::filesystem::path path() const
    {
        // Return the parent of shard 0's path
        return _shards[0]->path().parent_path();
    }
```

- [ ] **Step 2: Write test for aggregated ops**

Add to `tests/fanout/main.cpp`:

```cpp
SCENARIO("FanoutCache aggregated operations", "[fanout][aggregate]")
{
    AutoCleanDirectory db_path { "FanoutTestAgg" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with several entries spread across shards")
    {
        FanoutCache fc(db_path.path(), 4);
        for (int i = 0; i < 20; ++i)
            fc.set("key" + std::to_string(i), v1);

        THEN("count returns total across all shards")
        {
            REQUIRE(fc.count() == 20);
        }

        THEN("size returns total across all shards")
        {
            REQUIRE(fc.size() == 20 * 100);
        }

        THEN("keys returns all keys")
        {
            auto k = fc.keys();
            REQUIRE(k.size() == 20);
        }

        WHEN("we clear")
        {
            fc.clear();
            THEN("everything is gone")
            {
                REQUIRE(fc.count() == 0);
                REQUIRE(fc.size() == 0);
            }
        }

        THEN("check returns true") { REQUIRE(fc.check()); }
    }
}
```

- [ ] **Step 3: Build and run tests**

Run: `meson compile -C build && meson test -C build sciqlop-cache:fanout`

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(fanout): add count/size/keys/clear/check aggregations"
```

---

## Task 4: Add eviction, expiration, tags, stats, incr/decr, meta, transact

**Files:**
- Modify: `include/sciqlop_cache/fanout_store.hpp`
- Modify: `tests/fanout/main.cpp`

- [ ] **Step 1: Add remaining methods**

Add to `FanoutStore`:

```cpp
    // --- Expiration ---

    inline bool touch(const std::string& key, DurationConcept auto expire)
        requires requires(StoreType& s) { s.touch(key, expire); }
    {
        return _shard(key).touch(key, expire);
    }

    inline void expire()
        requires requires(StoreType& s) { s.expire(); }
    {
        _for_each_shard([](auto& s) { s.expire(); });
    }

    // --- Eviction ---

    inline std::size_t evict()
        requires requires(StoreType& s) { s.evict(); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict(); });
        return total;
    }

    inline void set_max_cache_size(std::size_t value)
        requires requires(StoreType& s) { s.set_max_cache_size(value); }
    {
        _for_each_shard([value](auto& s) { s.set_max_cache_size(value); });
    }

    [[nodiscard]] inline std::size_t max_cache_size()
        requires requires(StoreType& s) { s.max_cache_size(); }
    {
        return _shards[0]->max_cache_size();
    }

    // --- Tags ---

    inline std::size_t evict_tag(const std::string& tag)
        requires requires(StoreType& s) { s.evict_tag(tag); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict_tag(tag); });
        return total;
    }

    // --- Stats ---

    struct Stats { uint64_t hits; uint64_t misses; };

    auto stats()
        requires requires(StoreType& s) { s.stats(); }
    {
        uint64_t h = 0, m = 0;
        _for_each_shard([&](auto& s) {
            auto st = s.stats();
            h += st.hits;
            m += st.misses;
        });
        return Stats { h, m };
    }

    void reset_stats()
        requires requires(StoreType& s) { s.reset_stats(); }
    {
        _for_each_shard([](auto& s) { s.reset_stats(); });
    }

    // --- incr / decr ---

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).incr(key, delta, default_value);
    }

    inline int64_t decr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).decr(key, delta, default_value);
    }

    // --- meta (per-shard, keyed) ---

    inline void set_meta(const std::string& key, const std::string& value)
    {
        _shard(key).set_meta(key, value);
    }

    [[nodiscard]] inline std::optional<std::string> get_meta(const std::string& key)
    {
        return _shard(key).get_meta(key);
    }

    // --- transact (scoped to shard for key) ---

    auto begin_user_transaction(const std::string& key)
    {
        return _shard(key).begin_user_transaction();
    }
```

- [ ] **Step 2: Write tests for eviction, tags, stats**

Add to `tests/fanout/main.cpp`:

```cpp
SCENARIO("FanoutCache eviction", "[fanout][eviction]")
{
    AutoCleanDirectory db_path { "FanoutTestEvict" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with max_size per shard")
    {
        FanoutCache fc(db_path.path(), 4, 200);

        WHEN("we exceed max_size across shards")
        {
            for (int i = 0; i < 40; ++i)
                fc.set("key" + std::to_string(i), v1);

            fc.evict();

            THEN("total size is reduced") { REQUIRE(fc.size() <= 4 * 200); }
        }
    }
}

SCENARIO("FanoutCache tags", "[fanout][tags]")
{
    AutoCleanDirectory db_path { "FanoutTestTags" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with tagged entries")
    {
        FanoutCache fc(db_path.path(), 4);
        for (int i = 0; i < 10; ++i)
            fc.set("tagged" + std::to_string(i), v1, "group1");
        for (int i = 0; i < 5; ++i)
            fc.set("other" + std::to_string(i), v1);

        WHEN("we evict by tag")
        {
            auto evicted = fc.evict_tag("group1");

            THEN("tagged entries are removed")
            {
                REQUIRE(evicted == 10);
                REQUIRE(fc.count() == 5);
            }
        }
    }
}

SCENARIO("FanoutCache stats", "[fanout][stats]")
{
    AutoCleanDirectory db_path { "FanoutTestStats" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with some entries")
    {
        FanoutCache fc(db_path.path(), 4);
        fc.set("k1", v1);
        fc.set("k2", v1);

        WHEN("we get existing and missing keys")
        {
            fc.get("k1");
            fc.get("k2");
            fc.get("missing");

            auto s = fc.stats();
            THEN("hits and misses are aggregated")
            {
                REQUIRE(s.hits == 2);
                REQUIRE(s.misses == 1);
            }
        }

        WHEN("we reset stats")
        {
            fc.get("k1");
            fc.reset_stats();
            auto s = fc.stats();
            THEN("counters are zero")
            {
                REQUIRE(s.hits == 0);
                REQUIRE(s.misses == 0);
            }
        }
    }
}

SCENARIO("FanoutCache incr/decr", "[fanout][incr]")
{
    AutoCleanDirectory db_path { "FanoutTestIncr" };

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we incr a new key")
        {
            auto val = fc.incr("counter", 5, 10);
            THEN("it starts from default + delta") { REQUIRE(val == 15); }
        }

        WHEN("we decr")
        {
            fc.incr("counter", 0, 100);
            auto val = fc.decr("counter", 3);
            THEN("value decreases") { REQUIRE(val == 97); }
        }
    }
}

SCENARIO("FanoutCache meta", "[fanout][meta]")
{
    AutoCleanDirectory db_path { "FanoutTestMeta" };

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we set and get meta")
        {
            fc.set_meta("version", "1.0");
            THEN("we retrieve it") { REQUIRE(fc.get_meta("version") == "1.0"); }
        }
    }
}
```

- [ ] **Step 3: Build and run tests**

Run: `meson compile -C build && meson test -C build sciqlop-cache:fanout`

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(fanout): add eviction, expiration, tags, stats, incr/decr, meta, transact"
```

---

## Task 5: FanoutIndex tests

**Files:**
- Modify: `tests/fanout/main.cpp`

- [ ] **Step 1: Add FanoutIndex test**

Add to `tests/fanout/main.cpp`:

```cpp
SCENARIO("FanoutIndex basic CRUD", "[fanout][index]")
{
    AutoCleanDirectory db_path { "FanoutIndexTest01" };
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');

    GIVEN("a new FanoutIndex with 4 shards")
    {
        FanoutIndex fi(db_path.path(), 4);

        WHEN("we set, get, count, size, keys, del, clear")
        {
            fi.set("k1", v1);
            fi.set("k2", v2);

            THEN("count and size aggregate")
            {
                REQUIRE(fi.count() == 2);
                REQUIRE(fi.size() == 300);
            }

            THEN("keys returns all keys")
            {
                auto k = fi.keys();
                REQUIRE(k.size() == 2);
            }

            THEN("get returns correct values")
            {
                REQUIRE(fi.get("k1")->to_vector() == v1);
                REQUIRE(fi.get("k2")->to_vector() == v2);
            }
        }

        WHEN("we delete and clear")
        {
            fi.set("k1", v1);
            fi.set("k2", v2);
            fi.del("k1");
            REQUIRE(fi.count() == 1);
            fi.clear();
            REQUIRE(fi.count() == 0);
        }
    }
}
```

- [ ] **Step 2: Build and run tests**

Run: `meson compile -C build && meson test -C build sciqlop-cache:fanout`

- [ ] **Step 3: Commit**

```bash
git commit -am "test(fanout): add FanoutIndex tests"
```

---

## Task 6: Python bindings

**Files:**
- Modify: `pysciqlop_cache/pysciqlop_cache.cpp`
- Modify: `tests/python/test_python_interface.py`

- [ ] **Step 1: Add FanoutCache and FanoutIndex bindings**

Add to `pysciqlop_cache/pysciqlop_cache.cpp`, after the `Index` class binding. Also add helper functions before `NB_MODULE`:

```cpp
inline void _fanout_set_item(FanoutCache& c, const std::string& key, nb::bytes& buffer,
                              OptDuration expire = std::nullopt,
                              OptString tag = std::nullopt)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    if (expire && tag)
        c.set(key, data, *expire, *tag);
    else if (expire)
        c.set(key, data, *expire);
    else if (tag)
        c.set(key, data, *tag);
    else
        c.set(key, data);
}

inline bool _fanout_add_item(FanoutCache& c, const std::string& key, nb::bytes& buffer,
                              OptDuration expire = std::nullopt,
                              OptString tag = std::nullopt)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    if (expire && tag)
        return c.add(key, data, *expire, *tag);
    else if (expire)
        return c.add(key, data, *expire);
    else if (tag)
        return c.add(key, data, *tag);
    else
        return c.add(key, data);
}

inline void _fanout_index_set_item(FanoutIndex& idx, const std::string& key, nb::bytes& buffer)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    idx.set(key, data);
}

inline bool _fanout_index_add_item(FanoutIndex& idx, const std::string& key, nb::bytes& buffer)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    return idx.add(key, data);
}
```

Then inside `NB_MODULE`, after the `Index` binding:

```cpp
    nb::class_<FanoutCache>(m, "FanoutCache")
        .def(nb::init<const std::string&, std::size_t, std::size_t>(),
             "cache_path"_a = ".cache/", "shard_count"_a = 8, "max_size"_a = 0)
        .def("count", &FanoutCache::count)
        .def("__len__", &FanoutCache::count)
        .def("set", _fanout_set_item, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("__setitem__", [](FanoutCache& c, const std::string& key, nb::bytes& buffer)
             { return _fanout_set_item(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutCache::get, nb::arg("key"))
        .def("__getitem__", &FanoutCache::get, nb::arg("key"))
        .def("keys", &FanoutCache::keys)
        .def("exists", &FanoutCache::exists, nb::arg("key"))
        .def("add", _fanout_add_item, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("delete", &FanoutCache::del, nb::arg("key"))
        .def("pop", &FanoutCache::pop, nb::arg("key"))
        .def("touch", [](FanoutCache& c, const std::string& key,
                          std::chrono::system_clock::duration expire)
             { return c.touch(key, expire); }, nb::arg("key"), nb::arg("expire"))
        .def("expire", &FanoutCache::expire)
        .def("evict", &FanoutCache::evict)
        .def("evict_tag", &FanoutCache::evict_tag, nb::arg("tag"))
        .def("incr", &FanoutCache::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &FanoutCache::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &FanoutCache::clear)
        .def("check", &FanoutCache::check)
        .def("set_meta", &FanoutCache::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &FanoutCache::get_meta, nb::arg("key"))
        .def("size", &FanoutCache::size)
        .def("shard_count", &FanoutCache::shard_count)
        .def("set_max_cache_size", &FanoutCache::set_max_cache_size, nb::arg("value"))
        .def("path", [](FanoutCache& c) { return c.path().string(); })
        .def("stats", [](FanoutCache& c) {
            auto s = c.stats();
            nb::dict d;
            d["hits"] = s.hits;
            d["misses"] = s.misses;
            return d;
        })
        .def("reset_stats", &FanoutCache::reset_stats);

    nb::class_<FanoutIndex>(m, "FanoutIndex")
        .def(nb::init<const std::string&, std::size_t, std::size_t>(),
             "path"_a = ".index/", "shard_count"_a = 8, "max_size"_a = 0)
        .def("count", &FanoutIndex::count)
        .def("__len__", &FanoutIndex::count)
        .def("set", _fanout_index_set_item, nb::arg("key"), nb::arg("value"))
        .def("__setitem__", _fanout_index_set_item, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutIndex::get, nb::arg("key"))
        .def("__getitem__", &FanoutIndex::get, nb::arg("key"))
        .def("keys", &FanoutIndex::keys)
        .def("exists", &FanoutIndex::exists, nb::arg("key"))
        .def("add", _fanout_index_add_item, nb::arg("key"), nb::arg("value"))
        .def("delete", &FanoutIndex::del, nb::arg("key"))
        .def("pop", &FanoutIndex::pop, nb::arg("key"))
        .def("incr", &FanoutIndex::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &FanoutIndex::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &FanoutIndex::clear)
        .def("check", &FanoutIndex::check)
        .def("size", &FanoutIndex::size)
        .def("shard_count", &FanoutIndex::shard_count)
        .def("set_meta", &FanoutIndex::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &FanoutIndex::get_meta, nb::arg("key"))
        .def("path", [](FanoutIndex& idx) { return idx.path().string(); });
```

- [ ] **Step 2: Add Python tests**

Add to `tests/python/test_python_interface.py`:

```python
class TestFanoutCache(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.cache = pysciqlop_cache.FanoutCache(self.tmpdir, shard_count=4)

    def tearDown(self):
        del self.cache
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_set_get(self):
        self.cache.set("key1", b"value1")
        result = self.cache.get("key1")
        self.assertIsNotNone(result)
        self.assertEqual(bytes(result.memoryview()), b"value1")

    def test_count_and_size(self):
        self.cache.set("k1", b"aaa")
        self.cache.set("k2", b"bbbbb")
        self.assertEqual(self.cache.count(), 2)
        self.assertEqual(self.cache.size(), 8)

    def test_keys(self):
        self.cache.set("k1", b"v1")
        self.cache.set("k2", b"v2")
        self.assertEqual(sorted(self.cache.keys()), ["k1", "k2"])

    def test_delete(self):
        self.cache.set("k1", b"v1")
        self.cache.delete("k1")
        self.assertIsNone(self.cache.get("k1"))

    def test_clear(self):
        for i in range(10):
            self.cache.set(f"key{i}", b"val")
        self.cache.clear()
        self.assertEqual(self.cache.count(), 0)

    def test_shard_count(self):
        self.assertEqual(self.cache.shard_count(), 4)

    def test_stats(self):
        self.cache.set("k1", b"v1")
        self.cache.get("k1")
        self.cache.get("missing")
        s = self.cache.stats()
        self.assertEqual(s["hits"], 1)
        self.assertEqual(s["misses"], 1)

    def test_evict_tag(self):
        self.cache.set("t1", b"v1", tag="group")
        self.cache.set("t2", b"v2", tag="group")
        self.cache.set("t3", b"v3")
        evicted = self.cache.evict_tag("group")
        self.assertEqual(evicted, 2)
        self.assertEqual(self.cache.count(), 1)


class TestFanoutIndex(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.index = pysciqlop_cache.FanoutIndex(self.tmpdir, shard_count=4)

    def tearDown(self):
        del self.index
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_set_get(self):
        self.index.set("key1", b"value1")
        result = self.index.get("key1")
        self.assertIsNotNone(result)
        self.assertEqual(bytes(result.memoryview()), b"value1")

    def test_count_and_size(self):
        self.index.set("k1", b"aaa")
        self.index.set("k2", b"bbbbb")
        self.assertEqual(self.index.count(), 2)
        self.assertEqual(self.index.size(), 8)
```

- [ ] **Step 3: Build and run all tests**

Run: `meson compile -C build && meson test -C build`

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(fanout): add Python bindings for FanoutCache and FanoutIndex"
```

---

## Task 7: Update CLAUDE.md and backlog

**Files:**
- Modify: `CLAUDE.md` — add FanoutStore to architecture section
- Modify: backlog — mark FanoutCache as done

- [ ] **Step 1: Update CLAUDE.md**

Add `fanout_store.hpp` to the core layer description and add `FanoutCache`/`FanoutIndex` aliases.

- [ ] **Step 2: Mark backlog item done**

- [ ] **Step 3: Commit**

```bash
git commit -am "docs: update CLAUDE.md and backlog for FanoutStore"
```
