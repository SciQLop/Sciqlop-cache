# `check()` Command Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the trivial `check()` (just `SELECT COUNT(*)`) with a real diagnostic that detects and optionally repairs five categories of file/DB inconsistency.

**Architecture:** `check()` returns a `CheckResult` struct with per-category counts. When `fix=true`, it repairs what it can: removes orphaned files, deletes dangling DB rows, corrects size mismatches, reloads counters. SQLite corruption is detected but not repaired. The `_Store` method does the work; `FanoutStore` aggregates across shards. Python bindings expose the result as a dict.

**Tech Stack:** C++20, SQLite (`PRAGMA integrity_check`), `std::filesystem`, Catch2 BDD tests, nanobind Python bindings.

---

## File Structure

| File | Role |
|------|------|
| `include/sciqlop_cache/store.hpp` | Replace `check()` → `check(bool fix)`, add `CheckResult` struct, add private helpers |
| `include/sciqlop_cache/fanout_store.hpp` | Update `check()` to aggregate `CheckResult` across shards |
| `pysciqlop_cache/pysciqlop_cache.cpp` | Expose `check(fix)` returning a Python dict for all 4 types |
| `tests/check/main.cpp` | New test file — all check/fix scenarios |
| `meson.build` | Register `check` test suite |

---

## Chunk 1: CheckResult struct and detection

### Task 1: Add `CheckResult` struct and test scaffolding

**Files:**
- Modify: `include/sciqlop_cache/store.hpp` (near line 1063, the current `check()`)
- Create: `tests/check/main.cpp`
- Modify: `meson.build:109` (add `'check'` to the test list)

- [ ] **Step 1: Create test file with first test — clean cache returns all-ok**

```cpp
// tests/check/main.cpp
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <sqlite3.h>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

SCENARIO("check() on a clean cache reports no issues", "[check]")
{
    AutoCleanDirectory dir("check_clean");
    Cache cache(dir.path().string());

    GIVEN("A cache with a few entries")
    {
        std::string small = "hello";
        std::vector<char> large(16 * 1024, 'x');
        cache.set("small", std::span(small.data(), small.size()));
        cache.set("large", std::span(large.data(), large.size()));

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("Everything is ok")
            {
                REQUIRE(result.ok);
                REQUIRE(result.orphaned_files == 0);
                REQUIRE(result.dangling_rows == 0);
                REQUIRE(result.size_mismatches == 0);
                REQUIRE(result.counters_consistent);
                REQUIRE(result.sqlite_integrity_ok);
            }
        }
    }
}
```

- [ ] **Step 2: Register test in meson.build**

In `meson.build:109`, change:
```
foreach test_name:['database', 'basic', 'basic_index', 'intermediate', 'multithreads', 'fanout']
```
to:
```
foreach test_name:['database', 'basic', 'basic_index', 'intermediate', 'multithreads', 'fanout', 'check']
```

- [ ] **Step 3: Build and verify the test fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `check()` returns `bool`, not `CheckResult`.

- [ ] **Step 4: Add `CheckResult` struct and replace `check()` signature**

In `store.hpp`, replace the current `check()` method (around line 1063) with:

```cpp
    struct CheckResult
    {
        bool ok = true;
        std::size_t orphaned_files = 0;
        std::size_t dangling_rows = 0;
        std::size_t size_mismatches = 0;
        bool counters_consistent = true;
        bool sqlite_integrity_ok = true;
    };

    CheckResult check(bool fix = false)
    {
        auto db = this->db();
        CheckResult result;

        // 1. SQLite integrity
        result.sqlite_integrity_ok = _check_sqlite_integrity(db);

        // 2. Dangling rows (DB points to missing file)
        result.dangling_rows = _check_dangling_rows(db, fix);

        // 3. Size mismatches (file size != DB size column)
        result.size_mismatches = _check_size_mismatches(db, fix);

        // 4. Orphaned files (files on disk not in DB)
        result.orphaned_files = _check_orphaned_files(db, fix);

        // 5. Counter consistency
        result.counters_consistent = _check_counters(db, fix);

        result.ok = result.sqlite_integrity_ok
                 && result.dangling_rows == 0
                 && result.size_mismatches == 0
                 && result.orphaned_files == 0
                 && result.counters_consistent;
        return result;
    }
```

Add the private helpers as stubs that return "no problems" for now:

```cpp
private:
    bool _check_sqlite_integrity(DbGuard& db)
    {
        if (auto r = db->template exec<std::string>("PRAGMA integrity_check;"))
            return *r == "ok";
        return false;
    }

    std::size_t _check_dangling_rows([[maybe_unused]] DbGuard& db, [[maybe_unused]] bool fix)
    {
        return 0;
    }

    std::size_t _check_size_mismatches([[maybe_unused]] DbGuard& db, [[maybe_unused]] bool fix)
    {
        return 0;
    }

    std::size_t _check_orphaned_files([[maybe_unused]] DbGuard& db, [[maybe_unused]] bool fix)
    {
        return 0;
    }

    bool _check_counters([[maybe_unused]] DbGuard& db, [[maybe_unused]] bool fix)
    {
        return true;
    }
```

- [ ] **Step 5: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 6: Commit**

```
feat(check): add CheckResult struct and check() scaffolding

Replaces trivial SELECT COUNT(*) check with structured CheckResult
returning per-category diagnostics. All helpers stubbed for now.
```

---

### Task 2: Implement dangling row detection and fix

**Files:**
- Modify: `tests/check/main.cpp`
- Modify: `include/sciqlop_cache/store.hpp` (`_check_dangling_rows`)

- [ ] **Step 1: Write failing test — dangling row detected**

Append to `tests/check/main.cpp`:

```cpp
SCENARIO("check() detects dangling rows", "[check]")
{
    AutoCleanDirectory dir("check_dangling");
    Cache cache(dir.path().string());

    GIVEN("A large entry whose file is deleted externally")
    {
        std::vector<char> large(16 * 1024, 'x');
        cache.set("big", std::span(large.data(), large.size()));
        auto size_before = cache.size();
        auto count_before = cache.count();

        // Query the DB directly to get the file path
        std::filesystem::path file_path;
        {
            sqlite3* raw_db = nullptr;
            auto db_path = dir.path() / "sciqlop-cache.db";
            sqlite3_open(db_path.string().c_str(), &raw_db);
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(raw_db,
                "SELECT path FROM cache WHERE key = 'big';", -1, &stmt, nullptr);
            if (sqlite3_step(stmt) == SQLITE_ROW)
                file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
            sqlite3_close(raw_db);
        }
        REQUIRE(!file_path.empty());
        REQUIRE(std::filesystem::exists(file_path));
        std::filesystem::remove(file_path);

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the dangling row")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.dangling_rows == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The dangling row is removed and counters adjusted")
            {
                REQUIRE(result.dangling_rows == 1);
                REQUIRE(cache.count() == count_before - 1);
                REQUIRE(cache.size() < size_before);
                REQUIRE_FALSE(cache.exists("big"));
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `dangling_rows == 0` because stub returns 0.

- [ ] **Step 3: Implement `_check_dangling_rows`**

Replace the stub in `store.hpp`:

```cpp
    std::size_t _check_dangling_rows(DbGuard& db, bool fix)
    {
        std::size_t count = 0;

        struct Dangling { std::string key; std::size_t entry_size; };
        std::vector<Dangling> to_fix;

        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(),
                "SELECT key, path, size FROM cache WHERE path IS NOT NULL;",
                -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                if (path && !std::filesystem::exists(path))
                {
                    ++count;
                    if (fix)
                    {
                        auto key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                        auto sz = static_cast<std::size_t>(sqlite3_column_int64(stmt, 2));
                        to_fix.push_back({ key, sz });
                    }
                }
            }
            sqlite3_finalize(stmt);
        }

        if (fix)
        {
            for (auto& [key, sz] : to_fix)
            {
                db->exec(DELETE_STMT, key);
                _total_size.fetch_sub(sz, std::memory_order_relaxed);
                _total_count.fetch_sub(1, std::memory_order_relaxed);
            }
        }

        return count;
    }
```

- [ ] **Step 4: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): detect and fix dangling rows (DB → missing file)
```

---

### Task 3: Implement orphaned file detection and fix

**Files:**
- Modify: `tests/check/main.cpp`
- Modify: `include/sciqlop_cache/store.hpp` (`_check_orphaned_files`)

- [ ] **Step 1: Write failing test — orphaned file detected**

Append to `tests/check/main.cpp`:

```cpp
SCENARIO("check() detects orphaned files", "[check]")
{
    AutoCleanDirectory dir("check_orphan");
    Cache cache(dir.path().string());

    GIVEN("An extra file planted in the storage directory")
    {
        // Plant an orphan file in the UUID directory structure
        auto orphan_dir = dir.path() / "ab" / "cd";
        std::filesystem::create_directories(orphan_dir);
        auto orphan_path = orphan_dir / "abcd-fake-uuid-orphan";
        {
            std::ofstream ofs(orphan_path, std::ios::binary);
            ofs << "orphan data";
        }
        REQUIRE(std::filesystem::exists(orphan_path));

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the orphaned file")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.orphaned_files == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The orphan is deleted")
            {
                REQUIRE(result.orphaned_files == 1);
                REQUIRE_FALSE(std::filesystem::exists(orphan_path));
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `orphaned_files == 0`.

- [ ] **Step 3: Implement `_check_orphaned_files`**

The strategy: collect all `path` values from DB into a set, then walk the filesystem. Any regular file that isn't the DB file (or WAL/SHM) and isn't in the set is an orphan.

```cpp
    std::size_t _check_orphaned_files(DbGuard& db, bool fix)
    {
        // Collect all known file paths from DB
        std::unordered_set<std::string> known_paths;
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(),
                "SELECT path FROM cache WHERE path IS NOT NULL;",
                -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                if (auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)))
                    known_paths.insert(p);
            }
            sqlite3_finalize(stmt);
        }

        std::size_t count = 0;

        if (!std::filesystem::exists(cache_path))
            return 0;

        for (auto& entry : std::filesystem::recursive_directory_iterator(cache_path))
        {
            if (!entry.is_regular_file())
                continue;

            auto fname = entry.path().filename().string();
            // Skip database files (db, WAL, SHM, journal)
            if (fname == db_fname || fname.starts_with(std::string(db_fname)))
                continue;

            auto path_str = entry.path().string();
            if (known_paths.find(path_str) == known_paths.end())
            {
                ++count;
                if (fix)
                    std::filesystem::remove(entry.path());
            }
        }

        return count;
    }
```

Add `#include <unordered_set>` at the top of `store.hpp` (it is not currently included).

- [ ] **Step 4: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): detect and fix orphaned files (file on disk, no DB row)
```

---

### Task 4: Implement size mismatch detection and fix

**Files:**
- Modify: `tests/check/main.cpp`
- Modify: `include/sciqlop_cache/store.hpp` (`_check_size_mismatches`)

- [ ] **Step 1: Write failing test — size mismatch detected**

Append to `tests/check/main.cpp`:

```cpp
SCENARIO("check() detects size mismatches", "[check]")
{
    AutoCleanDirectory dir("check_size");
    Cache cache(dir.path().string());

    GIVEN("A large entry whose file is truncated externally")
    {
        std::vector<char> large(16 * 1024, 'x');
        cache.set("truncated", std::span(large.data(), large.size()));
        auto original_size = cache.size();

        // Query the DB directly to get the file path
        std::filesystem::path file_path;
        {
            sqlite3* raw_db = nullptr;
            auto db_path = dir.path() / "sciqlop-cache.db";
            sqlite3_open(db_path.string().c_str(), &raw_db);
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(raw_db,
                "SELECT path FROM cache WHERE key = 'truncated';", -1, &stmt, nullptr);
            if (sqlite3_step(stmt) == SQLITE_ROW)
                file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
            sqlite3_close(raw_db);
        }
        REQUIRE(!file_path.empty());
        REQUIRE(std::filesystem::exists(file_path));
        {
            std::ofstream ofs(file_path, std::ios::binary | std::ios::trunc);
            ofs << "short";
        }

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the size mismatch")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.size_mismatches == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The DB size column is corrected")
            {
                REQUIRE(result.size_mismatches == 1);
                REQUIRE(cache.size() < original_size);
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `size_mismatches == 0`.

- [ ] **Step 3: Implement `_check_size_mismatches`**

```cpp
    std::size_t _check_size_mismatches(DbGuard& db, bool fix)
    {
        std::size_t count = 0;

        struct Mismatch { std::string key; std::size_t db_size; std::size_t file_size; };
        std::vector<Mismatch> to_fix;

        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(),
                "SELECT key, path, size FROM cache WHERE path IS NOT NULL;",
                -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                if (!path || !std::filesystem::exists(path))
                    continue; // dangling row — handled separately

                auto db_size = static_cast<std::size_t>(sqlite3_column_int64(stmt, 2));
                auto file_size = std::filesystem::file_size(path);
                if (db_size != file_size)
                {
                    ++count;
                    if (fix)
                    {
                        auto key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                        to_fix.push_back({ key, db_size, file_size });
                    }
                }
            }
            sqlite3_finalize(stmt);
        }

        if (fix)
        {
            for (auto& [key, db_size, file_size] : to_fix)
            {
                db->exec("UPDATE cache SET size = ? WHERE key = ?;", file_size, key);
                if (file_size > db_size)
                    _total_size.fetch_add(file_size - db_size, std::memory_order_relaxed);
                else
                    _total_size.fetch_sub(db_size - file_size, std::memory_order_relaxed);
            }
        }

        return count;
    }
```

- [ ] **Step 4: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): detect and fix size mismatches (file size != DB size)
```

---

### Task 5: Implement counter consistency check and fix

**Files:**
- Modify: `tests/check/main.cpp`
- Modify: `include/sciqlop_cache/store.hpp` (`_check_counters`)

- [ ] **Step 1: Write failing test — counter drift detected**

Append to `tests/check/main.cpp`:

```cpp
SCENARIO("check() detects counter drift", "[check]")
{
    AutoCleanDirectory dir("check_counters");
    Index index(dir.path().string());

    GIVEN("An index with entries added via raw SQL (bypassing counters)")
    {
        std::string val = "hello";
        index.set("legit", std::span(val.data(), val.size()));

        // Insert a row directly via SQL, bypassing counter updates
        // Use set_meta to verify DB access works, then raw SQL
        sqlite3* raw_db = nullptr;
        auto db_path = dir.path() / "sciqlop-cache.db";
        sqlite3_open(db_path.string().c_str(), &raw_db);
        sqlite3_exec(raw_db,
            "INSERT INTO cache (key, value, size) VALUES ('sneaky', X'AABB', 2);",
            nullptr, nullptr, nullptr);
        sqlite3_close(raw_db);

        WHEN("check() is called without fix")
        {
            auto result = index.check();

            THEN("It detects counter inconsistency")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE_FALSE(result.counters_consistent);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = index.check(true);

            THEN("Counters are reloaded from DB")
            {
                REQUIRE_FALSE(result.counters_consistent);
                // After fix, count and size should reflect both rows
                REQUIRE(index.count() == 2);
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = index.check();
                REQUIRE(result2.ok);
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `counters_consistent` always returns `true`.

- [ ] **Step 3: Implement `_check_counters`**

```cpp
    // Note: the background eviction thread (_bg_evict) can delete rows and
    // adjust counters concurrently. During the window between its DELETE and
    // its fetch_sub, check() may observe a transient inconsistency. This is
    // harmless — a subsequent check() after the bg thread settles will pass.
    bool _check_counters(DbGuard& db, bool fix)
    {
        auto db_size = db->template exec<std::size_t>(
            "SELECT COALESCE(SUM(size), 0) FROM cache;");
        auto db_count = db->template exec<std::size_t>(
            "SELECT COUNT(*) FROM cache;");

        if (!db_size || !db_count)
            return false;

        bool consistent =
            _total_size.load(std::memory_order_relaxed) == *db_size
         && _total_count.load(std::memory_order_relaxed) == *db_count;

        if (!consistent && fix)
        {
            _total_size.store(*db_size, std::memory_order_relaxed);
            _total_count.store(*db_count, std::memory_order_relaxed);
        }

        return consistent;
    }
```

- [ ] **Step 4: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): detect and fix counter drift (atomics vs DB)
```

---

## Chunk 2: FanoutStore, Python bindings, Index tests

### Task 6: Update FanoutStore `check()`

**Files:**
- Modify: `include/sciqlop_cache/fanout_store.hpp` (around line 149)

- [ ] **Step 1: Write failing test — FanoutCache check aggregates shards**

Append to `tests/check/main.cpp`:

```cpp
SCENARIO("FanoutCache check() aggregates across shards", "[check][fanout]")
{
    AutoCleanDirectory dir("check_fanout");
    FanoutCache cache(dir.path().string(), 4);

    GIVEN("A fanout cache with entries")
    {
        std::string val = "data";
        cache.set("key1", std::span(val.data(), val.size()));
        cache.set("key2", std::span(val.data(), val.size()));

        WHEN("check() is called on a clean fanout cache")
        {
            auto result = cache.check();

            THEN("It reports all-ok")
            {
                REQUIRE(result.ok);
                REQUIRE(result.orphaned_files == 0);
                REQUIRE(result.dangling_rows == 0);
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: FAIL — `FanoutStore::check()` returns `bool`, not `CheckResult`.

- [ ] **Step 3: Update FanoutStore check()**

In `fanout_store.hpp`, replace `check()` with:

```cpp
    using CheckResult = typename StoreType::CheckResult;

    CheckResult check(bool fix = false)
    {
        CheckResult combined;
        _for_each_shard([&](auto& s) {
            auto r = s.check(fix);
            combined.orphaned_files += r.orphaned_files;
            combined.dangling_rows += r.dangling_rows;
            combined.size_mismatches += r.size_mismatches;
            if (!r.counters_consistent) combined.counters_consistent = false;
            if (!r.sqlite_integrity_ok) combined.sqlite_integrity_ok = false;
        });
        combined.ok = combined.sqlite_integrity_ok
                   && combined.dangling_rows == 0
                   && combined.size_mismatches == 0
                   && combined.orphaned_files == 0
                   && combined.counters_consistent;
        return combined;
    }
```

- [ ] **Step 4: Build and run test**

Run: `meson compile -C build && meson test -C build sciqlop-cache:check`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): FanoutStore aggregates CheckResult across shards
```

---

### Task 7: Update Python bindings

**Files:**
- Modify: `pysciqlop_cache/pysciqlop_cache.cpp`

- [ ] **Step 1: Write failing Python test**

Append to `tests/python/test_python_interface.py` (find the appropriate location):

```python
def test_check_returns_dict():
    import tempfile, os
    with tempfile.TemporaryDirectory() as d:
        c = pysciqlop_cache.Cache(os.path.join(d, "check_test"))
        c["key"] = b"value"
        result = c.check()
        assert isinstance(result, dict)
        assert result["ok"] is True
        assert result["orphaned_files"] == 0
        assert result["dangling_rows"] == 0
        assert result["size_mismatches"] == 0
        assert result["counters_consistent"] is True
        assert result["sqlite_integrity_ok"] is True

def test_check_fix():
    import tempfile, os
    with tempfile.TemporaryDirectory() as d:
        c = pysciqlop_cache.Cache(os.path.join(d, "check_fix"))
        c["key"] = b"value"
        result = c.check(fix=True)
        assert isinstance(result, dict)
        assert result["ok"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `meson test -C build sciqlop-cache:test_python_interface`
Expected: FAIL — `check()` returns `bool`, not `dict`.

- [ ] **Step 3: Update Python bindings for all four types**

In `pysciqlop_cache.cpp`, replace each `.def("check", &Type::check)` with a lambda that converts `CheckResult` to a Python dict. Create a helper to avoid repetition:

```cpp
// Near the top, after includes
template <typename T>
auto check_to_dict(T& store, bool fix) {
    auto r = store.check(fix);
    nb::dict d;
    d["ok"] = r.ok;
    d["orphaned_files"] = r.orphaned_files;
    d["dangling_rows"] = r.dangling_rows;
    d["size_mismatches"] = r.size_mismatches;
    d["counters_consistent"] = r.counters_consistent;
    d["sqlite_integrity_ok"] = r.sqlite_integrity_ok;
    return d;
}
```

Then for each of Cache, Index, FanoutCache, FanoutIndex replace:
```cpp
.def("check", &Cache::check)
```
with:
```cpp
.def("check", [](Cache& c, bool fix) { return check_to_dict(c, fix); },
     nb::arg("fix") = false)
```

(Same pattern for all four types.)

- [ ] **Step 4: Build and run Python tests**

Run: `meson test -C build sciqlop-cache:test_python_interface`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(check): Python bindings return dict with all check diagnostics
```

---

### Task 8: Run full test suite and verify nothing broke

- [ ] **Step 1: Run all tests**

Run: `meson test -C build`
Expected: All tests pass (basic, basic_index, database, intermediate, multithreads, fanout, check, python tests).

- [ ] **Step 2: Commit (if any fixups needed)**

```
chore: fixups from full test run
```
