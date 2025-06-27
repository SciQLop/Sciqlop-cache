/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include <iostream>
#include <string>
#include <memory>
#include <sqlite3.h>
#include <optional>
#include <mutex>

struct SQLiteDeleter {
    void operator()(sqlite3* db) const {
        if (db)
            sqlite3_close(db);
    }
};

class Database {
    private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;
    std::mutex db_mutex;

    public:
    Database();

    ~Database()
    {
        if (db) {
            sqlite3_close(db.get());
        }
    }

    int open(const std::string &db_path)
    {
        std::lock_guard<std::mutex> lock(db_mutex);

        const char *buffer = db_path.c_str();
        sqlite3 *tmp_db = db.get();
        int check = sqlite3_open(buffer, &tmp_db);

        if (check) {
            std::cerr << "Error opening database: " << sqlite3_errmsg(tmp_db) << std::endl;
            sqlite3_close(tmp_db);
        } else
            std::cout << "Database opened successfully." << std::endl;
        return check;
    }

    void close()
    {
        if (db)
            sqlite3_close(db.get());
    }

    sqlite3* get() const { return db.get(); }

    std::mutex& mutex() { return db_mutex; }

    bool exec(const std::string& sql)
    {
        std::lock_guard<std::mutex> lock(db_mutex);
        char* errMsg = nullptr;
        int rc = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            return false;
        }
        return true;
    }

    bool valid() const { return db != nullptr; }
};
