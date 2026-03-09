# Bugfixes & Easy Performance Wins Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all known bugs and apply low-risk performance improvements to the existing codebase.

**Architecture:** All changes are localized to 4 files: `sciqlop_cache.hpp` (cache logic), `database.hpp` (SQL binding), `pysciqlop_cache.cpp` (Python bindings), `__init__.py` (Python wrapper). No new files, no API changes beyond making `expire` optional.

**Tech Stack:** C++20, SQLite 3.50, nanobind, Meson/Ninja, Catch2

**Baseline:** 6/8 tests pass. `intermediate` aborts on corrupt DB test. `test_python_multiprocess` fails with RuntimeError.

---

### Task 1: Fix `clear()` — not recursive, misses file subdirectories

The file storage uses a two-level UUID directory (`ab/cd/uuid`), but `clear()` uses non-recursive `directory_iterator` and only skips the exact db filename. It needs to skip all DB-related files (`-wal`, `-shm`) and recurse into subdirectories.

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:397-408`

**Step 1: Write a failing test in `tests/basic/main.cpp`**

Add at end of file:

```cpp
SCENARIO("Testing sciqlop_cache clear with big values", "[cache]")
{
    AutoCleanDirectory db_path {"ClearTest01"};

    GIVEN("a cache with large values stored as files")
    {
        Cache cache(db_path.path(), 1000);
        std::vector<char> big_value(1024 * 1024); // 1MB, above 8KB threshold
        std::generate(big_value.begin(), big_value.end(), std::rand);

        cache.set("big1", big_value);
        cache.set("big2", big_value);
        REQUIRE(cache.count() == 2);

        WHEN("we clear the cache")
        {
            cache.clear();

            THEN("count should be zero")
            {
                REQUIRE(cache.count() == 0);
            }

            THEN("no data files should remain on disk")
            {
                namespace fs = std::filesystem;
                int file_count = 0;
                for (const auto& entry : fs::recursive_directory_iterator(db_path.path()))
                {
                    if (entry.is_regular_file())
                    {
                        auto fname = entry.path().filename().string();
                        // Only DB files should remain
                        bool is_db_file = fname == "sciqlop-cache.db"
                            || fname.ends_with("-wal") || fname.ends_with("-shm");
                        if (!is_db_file)
                            ++file_count;
                    }
                }
                REQUIRE(file_count == 0);
            }
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `ninja -C build/bugfix && ./build/bugfix/test-basic "[cache]" --name "Testing sciqlop_cache clear with big values"`
Expected: FAIL — data files remain on disk.

**Step 3: Fix `clear()` in `sciqlop_cache.hpp`**

Replace lines 397-408 with:

```cpp
    inline void clear()
    {
        sqlite3_exec(db().get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
        if (std::filesystem::exists(cache_path) && std::filesystem::is_directory(cache_path))
        {
            for (const auto& entry : std::filesystem::directory_iterator(cache_path))
            {
                auto fname = entry.path().filename().string();
                if (fname != db_fname && !fname.starts_with(std::string(db_fname)))
                    std::filesystem::remove_all(entry);
            }
        }
    }
```

This uses `remove_all` (handles subdirs) and skips `sciqlop-cache.db`, `sciqlop-cache.db-wal`, `sciqlop-cache.db-shm`.

**Step 4: Run test to verify it passes**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 5: Commit**

```
fix: make clear() remove file subdirectories and skip WAL/SHM files
```

---

### Task 2: Fix `add()` TOCTOU race — use INSERT OR IGNORE instead of exists() + INSERT

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:66-72` (change INSERT statements to INSERT OR IGNORE)
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:302-332` (rewrite `add()`)

**Step 1: Existing tests already cover `add()` behavior**

The basic test at `tests/basic/main.cpp:203-217` verifies that `add()` returns false for existing keys and true for new keys. This is sufficient.

**Step 2: Replace the INSERT statements with INSERT OR IGNORE**

In `sciqlop_cache.hpp`, change:

```cpp
    CompiledStatement INSERT_VALUE_STMT {
        "INSERT OR IGNORE INTO cache (key, value, expire, size) VALUES (?, ?, (strftime('%s', 'now') + ?), ?);"
    };
    CompiledStatement INSERT_PATH_STMT {
        "INSERT OR IGNORE INTO cache (key, path, expire, size) VALUES (?, ?, (strftime('%s', 'now') + ?), ?);"
    };
```

**Step 3: Rewrite `add()` to remove the `exists()` call and detect conflicts via sqlite changes count**

Replace the `add()` method (both overloads):

```cpp
    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return add(key, value, std::chrono::seconds { 3600 });
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        const double expires_secs
            = std::chrono::duration_cast<std::chrono::seconds>(expire).count();

        auto& _db = db();
        if (std::size(value) <= _file_size_threshold)
        {
            _db.exec(INSERT_VALUE_STMT, key, value, expires_secs, std::size(value));
            return sqlite3_changes(_db.get()) > 0;
        }

        auto file_path = storage->store(value);
        if (!file_path)
            return false;

        _db.exec(INSERT_PATH_STMT, key, file_path->string(), expires_secs, std::size(value));
        if (sqlite3_changes(_db.get()) == 0)
        {
            storage->remove(*file_path);
            return false;
        }
        return true;
    }
```

**Step 4: Run tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`
Expected: all existing `add()` tests still pass.

**Step 5: Commit**

```
fix: eliminate TOCTOU race in add() using INSERT OR IGNORE
```

---

### Task 3: Fix `del()` — remove redundant exists() call, delete file after DB row

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:334-357`

**Step 1: Existing tests cover `del()` at `tests/basic/main.cpp:176-181`**

**Step 2: Rewrite `del()`**

Replace lines 334-357:

```cpp
    inline bool del(const std::string& key)
    {
        auto& _db = db();
        auto filepath = _db.template exec<std::filesystem::path>(GET_PATH_SIMPLE_STMT, key);
        if (!_db.exec(DELETE_STMT, key))
            return false;
        if (sqlite3_changes(_db.get()) == 0)
            return false;
        if (filepath && !filepath->empty())
            storage->remove(*filepath);
        return true;
    }
```

This: (1) removes the `exists()` call, (2) deletes the DB row first, file second, (3) returns false silently for missing keys instead of printing to stderr.

**Step 3: Run tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 4: Commit**

```
fix: del() now single-query, deletes file after DB row
```

---

### Task 4: Fix `pop()` — don't call del() when get() returns nullopt

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:359-366`

**Step 1: Existing test at `tests/basic/main.cpp:220-227` covers pop()**

**Step 2: Fix `pop()`**

Replace:

```cpp
    inline std::optional<Buffer> pop(const std::string& key)
    {
        auto result = get(key);
        if (result)
            del(key);
        return result;
    }
```

**Step 3: Run tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 4: Commit**

```
fix: pop() no longer calls del() on missing keys
```

---

### Task 5: Fix Python `add()` missing return + default expiration mismatch

**Files:**
- Modify: `pysciqlop_cache/__init__.py:34,73-74`

**Step 1: Fix both issues**

Line 34 — change `36000` to `3600`:
```python
        super().set(key, pickle.dumps(value, self._pickle_protocol), expire=expire or timedelta(seconds=3600))
```

Lines 73-74 — add `return` and fix default:
```python
        return super().add(key, pickle.dumps(value, self._pickle_protocol),
                    expire=expire or timedelta(seconds=3600))
```

**Step 2: Run Python tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 3: Commit**

```
fix(python): add() now returns bool, fix default expiration to 1 hour
```

---

### Task 6: Fix double include in pysciqlop_cache.cpp

**Files:**
- Modify: `pysciqlop_cache/pysciqlop_cache.cpp:12`

**Step 1: Remove line 12**

Delete: `#include "include/sciqlop_cache/sciqlop_cache.hpp"`

Line 1 already includes the correct path.

**Step 2: Build to verify**

Run: `ninja -C build/bugfix`

**Step 3: Commit**

```
fix: remove duplicate include in pysciqlop_cache.cpp
```

---

### Task 7: Fix corrupt DB test — `db().opened()` throws instead of being catchable

The intermediate test expects `REQUIRE_THROWS_AS(..., std::runtime_error)` but the throw happens inside `db()` which is called lazily from `opened()`. The test calls `c.opened()` via assert but the Catch2 macro setup doesn't properly catch the throw through `[&](){}()`. The actual issue is the lambda + assert combination — the `opened()` call triggers the throw, but `assert()` aborts in debug builds before Catch2 can catch it.

**Files:**
- Modify: `tests/intermediate/main.cpp:78-88`

**Step 1: Fix the test to use REQUIRE_THROWS directly without assert**

Replace:

```cpp
        THEN("Cache initialization should throw")
        {
            REQUIRE_THROWS_AS(
                [&]()
                {
                    Cache c(db_path.path(), 1000);
                    c.opened(); // This triggers the lazy DB open which throws
                }(),
                std::runtime_error);
        }
```

**Step 2: Run the intermediate test**

Run: `ninja -C build/bugfix && ./build/bugfix/test-intermediate`
Expected: PASS

**Step 3: Commit**

```
fix(test): corrupt DB test now properly catches runtime_error
```

---

### Task 8: Perf — incremental size tracking triggers

The current triggers do `SELECT SUM(size) FROM cache` on every insert/update/delete — O(n). Replace with O(1) incremental math.

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp:132-151` (triggers in `_INIT_STMTS`)
- Modify: `tests/common.hpp:49-91` (same triggers duplicated there)

**Step 1: Existing `size()` test coverage**

The test at `tests/intermediate/main.cpp` and the `size()` method are sufficient. We need to verify size is correct after a sequence of operations.

**Step 2: Add a size tracking test to `tests/basic/main.cpp`**

```cpp
SCENARIO("Testing cache size tracking", "[cache]")
{
    AutoCleanDirectory db_path {"SizeTest01"};
    Cache cache(db_path.path(), 1000);

    GIVEN("an empty cache")
    {
        REQUIRE(cache.size() == 0);

        WHEN("we add items and delete them")
        {
            std::vector<char> v100(100, 'a');
            std::vector<char> v200(200, 'b');

            cache.set("k1", v100);
            REQUIRE(cache.size() == 100);

            cache.set("k2", v200);
            REQUIRE(cache.size() == 300);

            cache.del("k1");
            REQUIRE(cache.size() == 200);

            cache.set("k2", v100); // overwrite with smaller
            REQUIRE(cache.size() == 100);

            cache.clear();
            REQUIRE(cache.size() == 0);
        }
    }
}
```

**Step 3: Replace triggers with incremental versions**

In `sciqlop_cache.hpp` `_INIT_STMTS`, replace the three triggers with:

```sql
            CREATE TRIGGER IF NOT EXISTS cache_size_insert
            AFTER INSERT ON cache
            BEGIN
                UPDATE meta SET value = value + NEW.size WHERE key = 'size';
            END;

            CREATE TRIGGER IF NOT EXISTS cache_size_delete
            AFTER DELETE ON cache
            BEGIN
                UPDATE meta SET value = value - OLD.size WHERE key = 'size';
            END;

            CREATE TRIGGER IF NOT EXISTS cache_size_update
            AFTER UPDATE OF size ON cache
            BEGIN
                UPDATE meta SET value = value - OLD.size + NEW.size WHERE key = 'size';
            END;
```

Apply same change to `tests/common.hpp`.

**Step 4: Run tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 5: Commit**

```
perf: O(1) incremental size tracking triggers instead of SUM(size)
```

---

### Task 9: Perf — replace `strftime('%s', 'now')` with `unixepoch('now')`

`unixepoch()` is faster and available since SQLite 3.38 (we have 3.50). Simple find-and-replace across all SQL strings.

**Files:**
- Modify: `include/sciqlop_cache/sciqlop_cache.hpp` — all SQL strings
- Modify: `tests/common.hpp` — schema SQL

**Step 1: Replace all occurrences**

In `sciqlop_cache.hpp`, replace every `strftime('%s', 'now')` with `unixepoch('now')`.

In `tests/common.hpp`, same replacement.

**Step 2: Run tests**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`

**Step 3: Commit**

```
perf: use unixepoch('now') instead of strftime('%s','now')
```

---

### Task 10: Perf — remove redundant `exists()` call pattern in `del()` already fixed in Task 3

Already done. No action needed.

---

### Task 11: Final verification

**Step 1: Run full test suite**

Run: `ninja -C build/bugfix && ninja test -C build/bugfix`
Expected: 8/8 pass.

**Step 2: Review all changes**

Run: `git diff --stat`

Verify no unintended changes.
