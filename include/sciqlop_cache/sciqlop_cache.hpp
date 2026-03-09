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

template <typename Storage>
class _Cache
{
    std::filesystem::path cache_path;
    size_t max_size;
    bool auto_clean = false;
    std::unique_ptr<Storage> storage;
    std::size_t _file_size_threshold = 8 * 1024;

    std::thread _checkpoint_thread;
    std::atomic<bool> _stop_checkpoint { false };
    std::mutex _checkpoint_mutex;
    std::condition_variable _checkpoint_cv;

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
            REPLACE INTO cache (key, value, expire, size, path) VALUES (?, ?, (unixepoch('now') + ?), ?, NULL);
        )"
    };
    CompiledStatement REPLACE_PATH_STMT {
        R"(
            REPLACE INTO cache (key, path, expire, size, value) VALUES (?, ?, (unixepoch('now') + ?), ?, NULL);
        )"
    };
    CompiledStatement CLEAR_PATH_STMT {
        R"(
            UPDATE cache SET path = NULL WHERE key = ?;
        )"
    };
    CompiledStatement INSERT_VALUE_STMT {
        "INSERT OR IGNORE INTO cache (key, value, expire, size) VALUES (?, ?, (unixepoch('now') + ?), "
        "?);"
    };
    CompiledStatement INSERT_PATH_STMT {
        "INSERT OR IGNORE INTO cache (key, path, expire, size) VALUES (?, ?, (unixepoch('now') + ?), ?);"
    };
    CompiledStatement DELETE_STMT { "DELETE FROM cache WHERE key = ?;" };
    CompiledStatement TOUCH_STMT {
        "UPDATE cache SET last_update = unixepoch('now'), expire = unixepoch('now') + ?, "
        "last_use = unixepoch('now') WHERE key = ?;"
    };
    CompiledStatement EXPIRE_STMT {
        "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');"
    };
    CompiledStatement EVICT_STMT {
        "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');"
    };

    mutable CompiledStatement* statements[14]
        = { &COUNT_STMT,         &KEYS_STMT,         &EXISTS_STMT,
            &GET_STMT,           &GET_PATH_STMT,     &GET_PATH_SIMPLE_STMT,
            &REPLACE_VALUE_STMT, &REPLACE_PATH_STMT, &INSERT_VALUE_STMT,
            &INSERT_PATH_STMT,   &DELETE_STMT,       &TOUCH_STMT,
            &EXPIRE_STMT,        &EVICT_STMT };


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
                last_update REAL NOT NULL DEFAULT (unixepoch('now')),
                last_use REAL NOT NULL DEFAULT (unixepoch('now')),
                access_count_since_last_update INT NOT NULL DEFAULT 0,
                size INT NOT NULL DEFAULT 0
            ) WITHOUT ROWID;

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
                    return _db;
                _db.close();
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * (1 << attempt)));
            }
            throw std::runtime_error("Failed to initialize database schema.");
        }
        return _db;
    }


public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    _Cache(const std::filesystem::path& cache_path = ".cache/", size_t max_size = 1000)
            : cache_path(cache_path)
            , max_size(max_size)
            , storage(std::make_unique<Storage>(cache_path))
            , _checkpoint_thread(&_Cache::_checkpoint_loop, this)
    {
    }

    ~_Cache()
    {
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
        return set(key, value, std::chrono::seconds { 3600 });
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        const double expires_secs
            = std::chrono::duration_cast<std::chrono::seconds>(expire).count();

        if (std::size(value) <= _file_size_threshold)
        {
            db().exec(REPLACE_VALUE_STMT, key, value, expires_secs, std::size(value));
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
        _db.exec(REPLACE_PATH_STMT, key, *new_filepath, expires_secs, std::size(value));
        transaction.commit();
        if (filepath && !filepath->empty())
            storage->remove(*filepath);
        return true;
    }

    inline std::optional<Buffer> get(const std::string& key)
    {
        if (auto values = db().template exec<std::vector<char>, std::filesystem::path>(GET_STMT, key))
        {
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

        if (!_db.exec(EVICT_STMT))
            std::cerr << "Error deleting expired items from cache database." << std::endl;
    }

    // Delete items based on policy
    inline void evict() { ; }

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
