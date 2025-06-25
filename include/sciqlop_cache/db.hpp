/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include <iostream>
#include <string>
#include <unique_ptr>
#include <sqlite3.h>
#include <optional>
#include <mutex>

bool init(sqlite3 *db, std::mutex global_mutex)
{
    std::lock_guard<std::mutex> lock(global_mutex);

    const char *sql = R"(
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value BLOB,
            timestamp INTEGER
        );
    )";
    char *errMsg = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);

    if (rc != SQLITE_OK) {
        std::cerr << "Error creating table: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return false;
    }

    return true;
}

struct SQLiteDeleter {
    void operator()(sqlite3* db) const {
        if (db) {
            sqlite3_close(db);
        }
    }
};

class Database {
    private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    size_t current_size;
    size_t max_size;

    public:
    Database();

    ~Database() {
        if (db) {
            sqlite3_close();
        }
    }
};
