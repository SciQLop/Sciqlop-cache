/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include "database.hpp"
#include "disk_storage.hpp"
#include "utils/concepts.hpp"
#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <sqlite3.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

template <typename Storage>
class _Cache
{
    std::filesystem::path cache_path;
    size_t max_size;
    bool auto_clean = false;
    std::unique_ptr<Storage> storage;
    std::size_t _file_size_threshold = 8 * 1024;

    std::atomic<std::size_t> _access_seq { 0 };

    std::thread _checkpoint_thread;
    std::atomic<bool> _stop_checkpoint { false };
    std::mutex _checkpoint_mutex;
    std::condition_variable _checkpoint_cv;
    pid_t _owner_pid;

    void _bg_evict(sqlite3* bg_db)
    {
        // Remove expired entries and their files
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(bg_db,
            "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');",
            -1, &stmt, nullptr);
        std::vector<std::filesystem::path> files;
        while (stmt && sqlite3_step(stmt) == SQLITE_ROW)
        {
            auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (p && p[0])
                files.emplace_back(p);
        }
        if (stmt) sqlite3_finalize(stmt);
        sqlite3_exec(bg_db,
            "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');",
            nullptr, nullptr, nullptr);
        for (auto& f : files)
            storage->remove(f);

        if (max_size == 0)
            return;

        // Recompute and cache total size in meta table
        stmt = nullptr;
        sqlite3_exec(bg_db,
            "UPDATE meta SET value = (SELECT COALESCE(SUM(size), 0) FROM cache) WHERE key = 'size';",
            nullptr, nullptr, nullptr);

        // LRU eviction if over budget
        stmt = nullptr;
        sqlite3_prepare_v2(bg_db, "SELECT value FROM meta WHERE key = 'size';", -1, &stmt, nullptr);
        std::size_t current_size = 0;
        if (stmt && sqlite3_step(stmt) == SQLITE_ROW)
            current_size = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
        if (stmt) sqlite3_finalize(stmt);

        if (current_size <= max_size)
            return;

        auto target = max_size * 9 / 10;
        stmt = nullptr;
        sqlite3_prepare_v2(bg_db, "SELECT key, path, size FROM cache ORDER BY last_use ASC;",
                           -1, &stmt, nullptr);
        struct Entry { std::string key; std::filesystem::path path; };
        std::vector<Entry> to_evict;
        while (stmt && current_size > target && sqlite3_step(stmt) == SQLITE_ROW)
        {
            auto k = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            auto sz = static_cast<std::size_t>(sqlite3_column_int64(stmt, 2));
            to_evict.push_back({ k ? k : "", p ? p : "" });
            current_size -= std::min(current_size, sz);
        }
        if (stmt) sqlite3_finalize(stmt);

        stmt = nullptr;
        sqlite3_prepare_v2(bg_db, "DELETE FROM cache WHERE key = ?;", -1, &stmt, nullptr);
        for (auto& [key, path] : to_evict)
        {
            if (stmt)
            {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_step(stmt);
                sqlite3_reset(stmt);
            }
            if (!path.empty())
                storage->remove(path);
        }
        if (stmt) sqlite3_finalize(stmt);
    }

    void _checkpoint_loop()
    {
        auto db_path = (cache_path / db_fname).string();
        sqlite3* cp_db = nullptr;

        while (!_stop_checkpoint.load(std::memory_order_relaxed))
        {
            if (sqlite3_open_v2(db_path.c_str(), &cp_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
                                nullptr)
                == SQLITE_OK)
                break;
            if (cp_db)
            {
                sqlite3_close(cp_db);
                cp_db = nullptr;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!cp_db)
            return;

        while (!_stop_checkpoint.load(std::memory_order_relaxed))
        {
            std::unique_lock lock(_checkpoint_mutex);
            _checkpoint_cv.wait_for(lock, std::chrono::seconds(1));
            if (_stop_checkpoint.load(std::memory_order_relaxed))
                break;
            sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
            _bg_evict(cp_db);
        }

        sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
        sqlite3_close(cp_db);
    }

    CompiledStatement COUNT_STMT {
        "SELECT COUNT(*) FROM cache WHERE (expire IS NULL OR expire > unixepoch('now'));"
    };
    CompiledStatement KEYS_STMT {
        "SELECT key FROM cache WHERE (expire IS NULL OR expire > unixepoch('now'));"
    };
    CompiledStatement EXISTS_STMT {
        "SELECT 1 FROM cache WHERE key = ? AND (expire IS NULL OR expire > unixepoch('now')) "
        "LIMIT 1;"
    };
    CompiledStatement GET_STMT {
        "SELECT value, path FROM cache WHERE key = ? AND (expire IS NULL OR expire > "
        "unixepoch('now'));"
    };
    CompiledStatement GET_PATH_STMT {
        "SELECT path FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', "
        "'now'));"
    };
    CompiledStatement GET_PATH_SIMPLE_STMT { "SELECT path FROM cache WHERE key = ?;" };
    CompiledStatement REPLACE_VALUE_STMT {
        R"(
            REPLACE INTO cache (key, value, expire, size, path, last_use, tag) VALUES (?, ?, (unixepoch('now') + ?), ?, NULL, ?, ?);
        )"
    };
    CompiledStatement REPLACE_PATH_STMT {
        R"(
            REPLACE INTO cache (key, path, expire, size, value, last_use, tag) VALUES (?, ?, (unixepoch('now') + ?), ?, NULL, ?, ?);
        )"
    };
    CompiledStatement CLEAR_PATH_STMT {
        R"(
            UPDATE cache SET path = NULL WHERE key = ?;
        )"
    };
    CompiledStatement INSERT_VALUE_STMT {
        "INSERT OR IGNORE INTO cache (key, value, expire, size, last_use, tag) VALUES (?, ?, (unixepoch('now') + ?), "
        "?, ?, ?);"
    };
    CompiledStatement INSERT_PATH_STMT {
        "INSERT OR IGNORE INTO cache (key, path, expire, size, last_use, tag) VALUES (?, ?, (unixepoch('now') + ?), ?, ?, ?);"
    };
    CompiledStatement DELETE_STMT { "DELETE FROM cache WHERE key = ?;" };
    CompiledStatement TOUCH_STMT {
        "UPDATE cache SET last_update = unixepoch('now', 'subsec'), expire = unixepoch('now') + ?, "
        "last_use = unixepoch('now', 'subsec') WHERE key = ?;"
    };
    CompiledStatement EXPIRE_STMT {
        "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');"
    };
    CompiledStatement EVICT_EXPIRED_STMT {
        "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');"
    };
    CompiledStatement UPDATE_LAST_USE_STMT {
        "UPDATE cache SET last_use = ?, "
        "access_count_since_last_update = access_count_since_last_update + 1 "
        "WHERE key = ?;"
    };
    CompiledStatement EVICT_LRU_STMT {
        "SELECT key, path, size FROM cache ORDER BY last_use ASC;"
    };
    CompiledStatement EVICT_TAG_PATH_STMT {
        "SELECT path FROM cache WHERE tag = ?;"
    };
    CompiledStatement EVICT_TAG_STMT {
        "DELETE FROM cache WHERE tag = ?;"
    };
    CompiledStatement INCR_GET_STMT {
        "SELECT value FROM cache WHERE key = ? AND (expire IS NULL OR expire > unixepoch('now'));"
    };
    CompiledStatement INCR_UPDATE_STMT {
        "UPDATE cache SET value = ?, size = ?, last_use = ?, path = NULL WHERE key = ?;"
    };

    mutable CompiledStatement* statements[20]
        = { &COUNT_STMT,         &KEYS_STMT,           &EXISTS_STMT,
            &GET_STMT,           &GET_PATH_STMT,       &GET_PATH_SIMPLE_STMT,
            &REPLACE_VALUE_STMT, &REPLACE_PATH_STMT,   &INSERT_VALUE_STMT,
            &INSERT_PATH_STMT,   &DELETE_STMT,         &TOUCH_STMT,
            &EXPIRE_STMT,        &EVICT_EXPIRED_STMT,  &UPDATE_LAST_USE_STMT,
            &EVICT_LRU_STMT,     &EVICT_TAG_PATH_STMT, &EVICT_TAG_STMT,
            &INCR_GET_STMT,      &INCR_UPDATE_STMT };


    static inline constexpr auto _INIT_STMTS = {
        R"(
            -- Use Write-Ahead Logging for better concurrency
            PRAGMA journal_mode=WAL;
            -- Set synchronous mode to NORMAL for performance
            PRAGMA synchronous=NORMAL;
            PRAGMA wal_autocheckpoint=0;
            -- Set the number of cache pages
            PRAGMA cache_size=10000;
            -- Store temporary tables in memory
            PRAGMA temp_store=MEMORY;
            -- Set memory-mapped I/O size for performance
            PRAGMA mmap_size=268435456;
            -- Limit the number of rows analyzed for query planning
            PRAGMA analysis_limit=1000;
            PRAGMA busy_timeout=600000;
            PRAGMA recursive_triggers=ON;
            )",

        R"(
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY NOT NULL,
                path TEXT DEFAULT NULL,
                value BLOB DEFAULT NULL,
                expire REAL DEFAULT NULL,
                last_update REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
                last_use REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
                access_count_since_last_update INT NOT NULL DEFAULT 0,
                size INT NOT NULL DEFAULT 0,
                tag TEXT DEFAULT NULL
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag);

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value
            );

            INSERT OR IGNORE INTO meta (key, value) VALUES ('size', '0');
)"

    };

    inline bool _compile_statements() const
    {
        bool result = true;
        for (auto& stmt : statements)
            result &= stmt->compile(db().get());
        return result;
    }

    inline bool _finalize_statements()
    {
        bool result = true;
        for (auto& stmt : statements)
            result &= stmt->finalize();
        return result;
    }

    Database& db() const
    {
        thread_local Database _db;
        if (!_db.opened())
        {
            for (int attempt = 0; attempt < 5; ++attempt)
            {
                if (_db.open(this->cache_path / db_fname, _INIT_STMTS) && _compile_statements())
                {
                    // Migration: add tag column for databases created before tag support
                    sqlite3_exec(_db.get(),
                        "ALTER TABLE cache ADD COLUMN tag TEXT DEFAULT NULL;",
                        nullptr, nullptr, nullptr);
                    sqlite3_exec(_db.get(),
                        "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag);",
                        nullptr, nullptr, nullptr);
                    return _db;
                }
                _db.close();
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * (1 << attempt)));
            }
            throw std::runtime_error("Failed to initialize database schema.");
        }
        return _db;
    }


public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    _Cache(const std::filesystem::path& cache_path = ".cache/", size_t max_size = 0)
            : cache_path(cache_path)
            , max_size(max_size)
            , storage(std::make_unique<Storage>(cache_path))
            , _checkpoint_thread(&_Cache::_checkpoint_loop, this)
            , _owner_pid(getpid())
    {
    }

    ~_Cache()
    {
        if (getpid() != _owner_pid)
        {
            // Forked child: the checkpoint thread doesn't exist in this
            // process, but std::thread still thinks it's joinable.
            // Move it to the heap to avoid std::terminate() from
            // std::thread's destructor. The OS cleans up on process exit.
            if (_checkpoint_thread.joinable())
                (void)new std::thread(std::move(_checkpoint_thread));
            return;
        }
        _stop_checkpoint.store(true, std::memory_order_relaxed);
        _checkpoint_cv.notify_one();
        if (_checkpoint_thread.joinable())
            _checkpoint_thread.join();
        try { close(); } catch (...) {}
    }

    [[nodiscard]] inline bool opened() const { return db().opened(); }

    inline bool close() { return _finalize_statements() & db().close(); }

    [[nodiscard]] inline std::filesystem::path path() { return cache_path; }

    [[nodiscard]] inline size_t max_cache_size() { return max_size; }

    [[nodiscard]] inline std::size_t file_size_threshold() { return _file_size_threshold; }

    [[nodiscard]] inline std::size_t count()
    {
        if (auto r = db().template exec<std::size_t>(COUNT_STMT))
            return *r;
        return 0;
    }

    [[nodiscard]] inline size_t size()
    {
        if (auto r = db().template exec<size_t>("SELECT COALESCE(SUM(size), 0) FROM cache;"))
            return *r;
        return 0;
    }

    [[nodiscard]] inline std::vector<std::string> keys()
    {
        if (auto r = db().template exec<std::vector<std::string>>(KEYS_STMT))
            return *r;
        return {};
    }

    [[nodiscard]] inline bool exists(const std::string& key)
    {
        if (auto r = db().template exec<bool>(EXISTS_STMT, key))
            return *r;
        return false;
    }

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return _set_impl(key, value, std::optional<double> {});
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        return _set_impl(key, value,
            std::optional<double> { static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(expire).count()) });
    }

    inline bool set(const std::string& key, const Bytes auto& value, const std::string& tag)
    {
        return _set_impl(key, value, std::optional<double> {}, std::optional<std::string> { tag });
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire,
                    const std::string& tag)
    {
        return _set_impl(key, value,
            std::optional<double> { static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(expire).count()) },
            std::optional<std::string> { tag });
    }

private:
    inline bool _set_impl(const std::string& key, const Bytes auto& value,
                           std::optional<double> expires_secs,
                           std::optional<std::string> tag = std::nullopt)
    {
        auto seq = _access_seq.fetch_add(1, std::memory_order_relaxed);
        if (std::size(value) <= _file_size_threshold)
        {
            db().exec(REPLACE_VALUE_STMT, key, value, expires_secs, std::size(value), seq, tag);
            return true;
        }

        auto& _db = db();
        auto transaction = _db.begin_transaction(true);
        auto filepath = _db.template exec<std::filesystem::path>(GET_PATH_SIMPLE_STMT, key);
        auto new_filepath = storage->store(value);
        if (!new_filepath)
        {
            transaction.rollback();
            return false;
        }
        _db.exec(REPLACE_PATH_STMT, key, new_filepath->string(), expires_secs, std::size(value), seq, tag);
        transaction.commit();
        if (filepath && !filepath->empty())
            storage->remove(*filepath);
        return true;
    }

public:

    inline std::optional<Buffer> get(const std::string& key)
    {
        auto& _db = db();
        if (auto values = _db.template exec<std::vector<char>, std::filesystem::path>(GET_STMT, key))
        {
            if (max_size > 0)
                _db.exec(UPDATE_LAST_USE_STMT,
                         _access_seq.fetch_add(1, std::memory_order_relaxed), key);
            const auto& [_, path] = *values;

            if (!path.empty())
            {
                if (auto result = storage->load(path))
                    return result;
                else
                {
                    del(key);
                    std::cerr << "Error loading file for key: " << key << ", deleting entry."
                              << std::endl;
                    return std::nullopt;
                }
            }
            return Buffer(std::move(std::get<0>(*values)));
        }
        return std::nullopt;
    }

    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return _add_impl(key, value, std::optional<double> {});
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        return _add_impl(key, value,
            std::optional<double> { static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(expire).count()) });
    }

    inline bool add(const std::string& key, const Bytes auto& value, const std::string& tag)
    {
        return _add_impl(key, value, std::optional<double> {}, std::optional<std::string> { tag });
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire,
                    const std::string& tag)
    {
        return _add_impl(key, value,
            std::optional<double> { static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(expire).count()) },
            std::optional<std::string> { tag });
    }

private:
    inline bool _add_impl(const std::string& key, const Bytes auto& value,
                           std::optional<double> expires_secs,
                           std::optional<std::string> tag = std::nullopt)
    {
        auto& _db = db();
        auto seq = _access_seq.fetch_add(1, std::memory_order_relaxed);
        if (std::size(value) <= _file_size_threshold)
        {
            _db.exec(INSERT_VALUE_STMT, key, value, expires_secs, std::size(value), seq, tag);
            if (sqlite3_changes(_db.get()) > 0)
                return true;
            return false;
        }

        auto file_path = storage->store(value);
        if (!file_path)
            return false;

        _db.exec(INSERT_PATH_STMT, key, file_path->string(), expires_secs, std::size(value), seq, tag);
        if (sqlite3_changes(_db.get()) == 0)
        {
            storage->remove(*file_path);
            return false;
        }
        return true;
    }

public:

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

    inline std::optional<Buffer> pop(const std::string& key)
    {
        auto result = get(key);
        if (result)
            del(key);
        return result;
    }

    // Touch a key to update its expiration time
    inline bool touch(const std::string& key, DurationConcept auto expire)
    {
        auto expire_secs = std::chrono::duration_cast<std::chrono::seconds>(expire).count();
        return db().exec(TOUCH_STMT, expire_secs, key);
    }

    inline void expire()
    {
        auto& _db = db();
        {
            auto binded = EXPIRE_STMT.bind_all();
            while (auto r = _db.template step<std::filesystem::path>(binded))
            {
                const auto file_path = *r;
                if (!storage->remove(file_path))
                {
                    std::cerr << "Failed to delete file: " << file_path << std::endl;
                }
            }
        }

        if (!_db.exec(EVICT_EXPIRED_STMT))
            std::cerr << "Error deleting expired items from cache database." << std::endl;
    }

    inline std::size_t evict()
    {
        if (max_size == 0)
            return 0;

        expire();

        auto current_size = size();
        if (current_size <= max_size)
            return 0;

        auto target = max_size * 9 / 10;
        auto& _db = db();

        struct Entry { std::string key; std::filesystem::path path; std::size_t entry_size; };
        std::vector<Entry> to_evict;
        {
            auto binded = EVICT_LRU_STMT.bind_all();
            while (current_size > target)
            {
                auto r = _db.template step<std::string, std::filesystem::path, std::size_t>(binded);
                if (!r) break;
                auto& [key, path, entry_size] = *r;
                to_evict.push_back({ std::move(key), std::move(path), entry_size });
                current_size -= std::min(current_size, entry_size);
            }
        }

        for (auto& [key, path, _] : to_evict)
        {
            _db.exec(DELETE_STMT, key);
            if (!path.empty())
                storage->remove(path);
        }

        return to_evict.size();
    }

    inline std::size_t evict_tag(const std::string& tag)
    {
        auto& _db = db();
        std::vector<std::filesystem::path> files;
        {
            auto binded = EVICT_TAG_PATH_STMT.bind_all(tag);
            while (auto r = _db.template step<std::filesystem::path>(binded))
            {
                if (!r->empty())
                    files.push_back(std::move(*r));
            }
        }
        _db.exec(EVICT_TAG_STMT, tag);
        auto count = static_cast<std::size_t>(sqlite3_changes(_db.get()));
        for (auto& f : files)
            storage->remove(f);
        return count;
    }

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        auto& _db = db();
        auto transaction = _db.begin_transaction(true);

        int64_t current = default_value;
        if (auto blob = _db.template exec<std::vector<char>>(INCR_GET_STMT, key))
        {
            if (blob->size() == sizeof(int64_t))
                std::memcpy(&current, blob->data(), sizeof(int64_t));
        }

        int64_t new_value = current + delta;
        auto seq = _access_seq.fetch_add(1, std::memory_order_relaxed);

        std::array<char, sizeof(int64_t)> buf;
        std::memcpy(buf.data(), &new_value, sizeof(int64_t));
        auto data = std::span<const char>(buf.data(), buf.size());

        _db.exec(INCR_UPDATE_STMT, data, sizeof(int64_t), seq, key);
        if (sqlite3_changes(_db.get()) == 0)
            _db.exec(REPLACE_VALUE_STMT, key, data, std::optional<double> {},
                     sizeof(int64_t), seq, std::optional<std::string> {});

        transaction.commit();
        return new_value;
    }

    inline int64_t decr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return incr(key, -delta, default_value);
    }

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

    // Check the cache statistics
    // CacheStats stats()
    // {
    //     ;
    // }

    // Check if the cache is valid
    // need explaination and actually test everything
    inline bool check()
    {
        const char* sql = "SELECT COUNT(*) FROM cache;";
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db().get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::cerr << "Error preparing statement: " << sqlite3_errmsg(db().get()) << std::endl;
            return false;
        }

        bool valid = (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) >= 0);
        sqlite3_finalize(stmt);
        return valid;
    }
};

using Cache = _Cache<DiskStorage>;
