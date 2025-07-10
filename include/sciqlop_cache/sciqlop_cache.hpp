/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include <bitset>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <mutex>
#include <optional>
#include <sqlite3.h>
#include <string>

#include "database.hpp"
#include "utils/time.hpp"


template <typename T>
concept Bytes = requires(T t) {
    { std::size(t) } -> std::convertible_to<std::size_t>;
    { std::data(t) } -> std::convertible_to<const char*>;
};

/*
don't use lock guard everywhere, use it only when you need to ensure thread safety
while (sqlite3_step(stmt) == SQLITE_ROW) {;}
std::atomic<int> for frequency tracking
Optionally consider lock-free structures (like concurrent_hash_map from TBB) if perf is critical.
*/

class Cache
{
private:
    size_t max_size;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    bool auto_clean = false;
    Database db;

public:
    Cache(const std::string& db_path = "sciqlop-cache.db", size_t max_size_ = 1000)
            : max_size(max_size_), stmt(nullptr)
    {
        if (db.open(db_path) != SQLITE_OK || !init())
            throw std::runtime_error("Failed to initialize database schema.");
    }

    bool init()
    {
        const std::string sql = R"(
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                expire REAL,
                last_update REAL
            );
        )";
        bool result = db.exec(sql);

        if (!result)
        {
            std::cerr << "SQL exec failed in init(). SQL: " << sql << std::endl;
        }
        return result;
    }

    [[nodiscard]] std::size_t count()
    {
        return execute_stmt<std::size_t>(
            "SELECT COUNT(*) FROM cache;", [&](sqlite3_stmt*) {},
            [&](sqlite3_stmt*) { return sqlite3_column_int64(stmt, 0); }, false);
    }

[[nodiscard]] std::vector<std::string> keys()
    {
        std::vector<std::string> keys;
        execute_stmt_void(
            "SELECT key FROM cache;",
            [&](sqlite3_stmt* stmt)
            {
                while (sqlite3_step(stmt) == SQLITE_ROW)
                {
                    const char* key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                    if (key)
                        keys.emplace_back(key);
                }
            });
        return keys;
    }


    template <typename T>
    T execute_stmt(const std::string& sql, std::function<void(sqlite3_stmt*)> binder,
        std::function<T(sqlite3_stmt*)> extractor, T default_value = T())
    {
        std::lock_guard<std::mutex> lock(global_mutex); // maybe sql ensure the thread safe aspect

        if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            return default_value;

        binder(stmt);
        T result = default_value;

        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = extractor(stmt);

        sqlite3_finalize(stmt);
        return result;
    }

    bool execute_stmt_void(const std::string& sql, std::function<void(sqlite3_stmt*)> binder)
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            return false;

        binder(stmt);
        bool success = (sqlite3_step(stmt) == SQLITE_DONE);
        sqlite3_finalize(stmt);
        return success;
    }

    bool exists(const std::string& key)
    {
        return execute_stmt<bool>(
            "SELECT 1 FROM cache WHERE key = ? LIMIT 1;",
            [&](sqlite3_stmt* stmt) { sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC); },
            [](sqlite3_stmt*) { return true; }, false);
    }

    inline bool set(const std::string& key, const Bytes auto & value)
    {
        return set(key, value, std::chrono::seconds { 3600 });
    }

    bool set(const std::string& key, const Bytes auto & value, Duration auto expire)
    {
        const auto now = std::chrono::system_clock::now();
        return execute_stmt_void(
            "REPLACE INTO cache (key, value, expire, last_update) VALUES (?, ?, ?, ?);",
            [&](sqlite3_stmt* stmt)
            {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_blob(
                    stmt, 2, std::data(value), static_cast<int>(std::size(value)), SQLITE_STATIC);
                sqlite3_bind_double(stmt, 3, time_point_to_epoch(now + expire));
                sqlite3_bind_double(stmt, 4, time_point_to_epoch(now));
            });
    }

    std::optional<std::vector<char>> get(const std::string& key)
    {
        return execute_stmt<std::optional<std::vector<char>>>(
            "SELECT value FROM cache WHERE key = ?;",
            [&](sqlite3_stmt* stmt) { sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC); },
            [](sqlite3_stmt* stmt) -> std::optional<std::vector<char>>
            {
                const void* blob = sqlite3_column_blob(stmt, 0);
                int size = sqlite3_column_bytes(stmt, 0);
                if (blob && size > 0)
                {
                    const char* start = static_cast<const char*>(blob);
                    return std::vector<char>(start, start + size);
                }
                return std::nullopt;
            },
            std::nullopt);
    }

    inline bool add(const std::string& key, const Bytes auto & value)
    {
        return add(key, value, std::chrono::seconds { 3600 });
    }

    // Add a value if the key doesn't already exist
    bool add(const std::string& key, const Bytes auto & value, Duration auto expire)
    {
        const auto now = std::chrono::system_clock::now();
        bool success = execute_stmt_void(
            "INSERT INTO cache (key, value, expire, last_update) VALUES (?, ?, ?, ?);",
            [&](sqlite3_stmt* stmt)
            {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_blob(
                    stmt, 2, std::data(value), static_cast<int>(std::size(value)), SQLITE_STATIC);
                sqlite3_bind_double(stmt, 3, time_point_to_epoch(now + expire));
                sqlite3_bind_double(stmt, 4, time_point_to_epoch(now));
            });
        return success;
    }

    bool del(const std::string& key)
    {
        if (!exists(key))
        {
            std::cerr << "Key not found: " << key << std::endl;
            return false;
        }

        bool success = execute_stmt_void("DELETE FROM cache WHERE key = ?;", [&](sqlite3_stmt* stmt)
            { sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC); });
        if (success)
        {
            return true;
        }
        else
        {
            std::cerr << "Error deleting key: " << key << std::endl;
            return false;
        }
    }

    std::optional<std::vector<char>> pop(const std::string& key)
    {
        std::optional<std::vector<char>> result = get(key);

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    bool touch(const std::string& key, Duration auto expire)
    {
        return execute_stmt_void("UPDATE cache SET expire = ? WHERE key = ?;",
            [&](sqlite3_stmt* stmt)
            {
                sqlite3_bind_int(
                    stmt, 1, time_point_to_epoch(std::chrono::system_clock::now() + expire));
                sqlite3_bind_text(stmt, 2, key.c_str(), -1, SQLITE_STATIC);
            });
    }

    // Delete expired items
    void expire()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();
        double now_ = time_point_to_epoch(now);

        const std::string sql = "DELETE FROM cache WHERE expire <= " + std::to_string(now_) + ";";
        sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, nullptr);
    }

    // Delete items based on policy
    void evict() { ; }

    // Get the cache directory
    void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db.get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
    }

    // Check the cache statistics
    // CacheStats stats()
    // {
    //     ;
    // }

    // Check if the cache is valid
    // need explaination and actually test everything
    bool check()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        const char* sql = "SELECT COUNT(*) FROM cache;";
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db.get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::cerr << "Error preparing statement: " << sqlite3_errmsg(db.get()) << std::endl;
            return false;
        }

        bool valid = (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) >= 0);
        sqlite3_finalize(stmt);
        return valid;
    }
};
