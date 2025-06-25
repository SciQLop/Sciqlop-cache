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
#include <memory>
#include <sqlite3.h>
#include <bitset>
#include <optional>
#include <stdexcept>
#include <mutex>
#include <functional>
#include <chrono>
#include <ctime>
#include <iomanip>

#include "utils/time.hpp"
#include "db.hpp"

/*
don't use lock guard everywhere, use it only when you need to ensure thread safety
while (sqlite3_step(stmt) == SQLITE_ROW) {;}
std::atomic<int> for frequency tracking
Optionally consider lock-free structures (like concurrent_hash_map from TBB) if perf is critical.
*/

// Base Cache class
class Cache {
    private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    size_t current_size;
    size_t max_size;
    int check_;
    bool auto_clean = false;
    // put sql related function in another class

    public:
    Cache(const std::string &db_path = "sciqlop-cache.db", size_t max_size_ = 1000)
    {
        check_ = open(db_path);

        if (check_)
            exit(84);
        
        if (!init(db)) {
            std::cerr << "Failed to initialize database schema." << std::endl;
            exit(84);
        }
        
        max_size = max_size_;
    }

    ~Cache() {;}

    int open(const std::string &db_path)
    {
        std::lock_guard<std::mutex> lock(global_mutex);

        const char *buffer = db_path.c_str();
        int check = sqlite3_open(buffer, &db);

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
        std::lock_guard<std::mutex> lock(global_mutex); // maybe sql ensure the thread safe aspect

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
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();
        const auto expire_time = std::chrono::system_clock::now() + std::chrono::seconds(expire);

        return execute_stmt_void(
            "REPLACE INTO cache (key, value, expire, last_update) VALUES (?, ?, ?, ?);",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_double(stmt, 3, epoch_to_double(time_to_epoch(now)));
                sqlite3_bind_double(stmt, 4, epoch_to_double(time_to_epoch(expire_time)));
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
            "INSERT INTO cache (key_column, data_column) VALUES (?, ?, ?);",
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
        std::optional<std::string> result = get(key);

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    bool touch(const std::string& key, int expire = 3600)
    {
        return execute_stmt_void(
            "UPDATE cache SET expire = ? WHERE key = ?;",
            [&](sqlite3_stmt* stmt) {
                sqlite3_bind_int(stmt, 1, expire);
                sqlite3_bind_text(stmt, 2, key.c_str(), -1, SQLITE_STATIC);
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
        ;
    }

    // Get the cache directory
    void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db, "DELETE FROM cache;", nullptr, nullptr, nullptr);
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
        const char *sql = "SELECT COUNT(*) FROM cache;";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "Error preparing statement: " << sqlite3_errmsg(db) << std::endl;
            return false;
        }

        bool valid = (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) >= 0);
        sqlite3_finalize(stmt);
        return valid;
    }
};
