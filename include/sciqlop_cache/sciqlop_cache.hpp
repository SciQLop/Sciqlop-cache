/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include <iostream>
#include <string>
#include <bitset>
#include <unordered_map>
#include <memory>
#include <vector>
#include <sqlite3.h>
#include <optional>
#include <stdexcept>
#include <mutex>
#include <functional>

/*
use shared_mutex for thread safety
std::atomic<int> for frequency tracking
Optionally consider lock-free structures (like concurrent_hash_map from TBB) if perf is critical.
*/

// Base Cache class
class Cache {
    sqlite3 *db;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    size_t current_size;
    size_t max_size;
    int check_;

    Cache(const std::string &db_path, size_t max_size = 1000)
    {
        check_ = open();

        if (!check_)
            exit(84);
    }

    ~Cache() {;}

    int open()
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        int check = sqlite3_open("sciqlop-cache.db", &db);

        if (check) {
            std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
            sqlite3_close(db);
        } else
            std::cout << "Database opened successfully." << std::endl;
        return check;
    }

    void close()
    {
        if (db)
            sqlite3_close(db);
    }

    template<typename T>
    T execute_stmt(const std::string &sql, std::function<void(sqlite3_stmt*)> binder,
        std::function<T(sqlite3_stmt*)> extractor, T default_value = T())
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            return default_value;

        binder(stmt);
        T result = default_value;

        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = extractor(stmt);

        sqlite3_finalize(stmt);
        return result;
    }

    bool execute_stmt_void(const std::string& sql,
        std::function<void(sqlite3_stmt*)> binder)
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            return false;

        binder(stmt);
        bool success = (sqlite3_step(stmt) == SQLITE_DONE);
        sqlite3_finalize(stmt);
        return success;
    }

    bool set(const std::string& key, const std::string& value, int expire = 3600)
    {
        return execute_stmt_void(
            "REPLACE INTO cache (key, value, expire) VALUES (?, ?, ?);",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 3, expire);
            }
        );
    }

    std::optional<std::string> get(const std::string& key)
    {
        return execute_stmt<std::optional<std::string>>(
            "SELECT value FROM cache WHERE key = ?;",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
            },
            [](sqlite3_stmt* stmt) -> std::optional<std::string> {
                const unsigned char* text = sqlite3_column_text(stmt, 0);
                return text ? std::optional<std::string>(reinterpret_cast<const char*>(text)) : std::nullopt;
            },
            std::nullopt
        );
    }

    // Add a value if the key doesn't already exist
    bool add(const std::string& key, const std::string& value, int expire = 3600)
    {
        return execute_stmt_void(
            "INSERT INTO cache (key_column, data_column) VALUES (?, ?; ?);",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 3, expire);
            }
        );
    }

    bool del(const std::string& key)
    {
        return execute_stmt_void(
            "DELETE FROM cache WHERE key = ?;",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
            }
        );
    }

    std::optional<std::string> pop(const std::string& key)
    {
        std::optional<std::string> result = get(key)

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    bool touch(const std::string& key, int expire)
    {
        return execute_stmt_void(
            "UPDATE cache SET expire = ? WHERE key = ?;",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_int(stmt, 3, expire);
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
            }
        );
    }

    // Delete expired items
    void expire()
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        const char *sql = "DELETE FROM cache WHERE expire < datetime('now');";
        sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    }

    // Delete items based on policy
    void evict()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_stmt *stmt;

        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            return default_value;

        binder(stmt);
        T result = default_value;

        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = extractor(stmt);

        sqlite3_finalize(stmt);
        return result;
    }

    // Get the cache directory
    void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db, "DELETE FROM cache;", nullptr, nullptr, nullptr);
    }

    // Check the cache statistics
    CacheStats stats()
    {
        ;
    }

    // Check if the cache is valid
    bool check()
    {
        return false;
    }

    // Remove old items or least recently used items
    bool cull()
    {
        return false;
    }
};
