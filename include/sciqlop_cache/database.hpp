/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include "sciqlop_cache/utils/concepts.hpp"
#include "sciqlop_cache/utils/time.hpp"
#include "sciqlop_cache/Profiling.hpp"
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>
#include <cpp_utils/types/detectors.hpp>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sqlite3.h>
#include <string>
#include <tuple>
#include <vector>

struct SQLiteDeleter
{
    void operator()(sqlite3* db) const
    {
        if (db)
            sqlite3_close(db);
    }
};

void sql_bind(const auto& stmt, int col, TimePoint auto&& value)
{
    sqlite3_bind_double(stmt, col, time_point_to_epoch(value));
}

void sql_bind(const auto& stmt, int col, Bytes auto&& value)
{
    sqlite3_bind_blob(stmt, col, std::data(value), static_cast<int>(std::size(value)),
                      SQLITE_STATIC);
}

void sql_bind(const auto& stmt, int col, const std::string& value)
{
    sqlite3_bind_text(stmt, col, value.c_str(), -1, SQLITE_STATIC);
}

void sql_bind(const auto& stmt, int col, const std::size_t value)
{
    sqlite3_bind_int64(stmt, col, value);
}

void sql_bind(const auto& stmt, int col, const std::optional<double>& value)
{
    if (value)
        sqlite3_bind_double(stmt, col, *value);
    else
        sqlite3_bind_null(stmt, col);
}

void sql_bind(const auto& stmt, int col, const std::optional<std::string>& value)
{
    if (value)
        sqlite3_bind_text(stmt, col, value->c_str(), -1, SQLITE_STATIC);
    else
        sqlite3_bind_null(stmt, col);
}

void sql_bind_all(const auto& stm, auto&&... values)
{
    int i = 1;
    ((sql_bind(stm, i++, std::forward<decltype(values)>(values))), ...);
}

template <typename rtype>
auto sql_get(const auto& stmt, int col)
{
    PROFILE_HERE_N(std::source_location::current().function_name());
    static_assert(cpp_utils::types::detectors::is_any_of_v<rtype, std::vector<char>, std::string,
                                                           std::filesystem::path, bool, std::size_t,
                                                           std::vector<std::string>>
                      || TimePoint<rtype>,
                  "Unsupported return type for sql_get");

    if constexpr (std::is_same_v<rtype, std::vector<char>>)
    {
        const void* blob = sqlite3_column_blob(stmt, col);
        int size = sqlite3_column_bytes(stmt, col);
        if (blob && size > 0)
        {
            const char* start = static_cast<const char*>(blob);
            return std::vector<char>(start, start + size);
        }
        else
        {
            return std::vector<char> {};
        }
    }
    else if constexpr (std::is_same_v<rtype, std::string>)
    {
        const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
        if (v)
            return std::string(v);
        else
            return std::string {};
    }
    else if constexpr (std::is_same_v<rtype, std::filesystem::path>)
    {
        const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
        if (v)
            return std::filesystem::path(v);
        else
            return std::filesystem::path {};
    }
    else if constexpr (TimePoint<rtype>)
    {
        double v = sqlite3_column_double(stmt, col);
        return epoch_to_time_point(v);
    }
    else if constexpr (std::is_same_v<rtype, bool>)
    {
        return true;
    }
    else if constexpr (std::is_same_v<rtype, std::size_t>)
    {
        return static_cast<std::size_t>(sqlite3_column_int64(stmt, col));
    }
    else if constexpr (std::is_same_v<rtype, std::vector<std::string>>)
    {
        std::vector<std::string> result;
        do
        {
            const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
            if (v)
                result.emplace_back(v);
        } while (sqlite3_step(stmt) == SQLITE_ROW);
        return result;
    }
    else
        return nullptr;
}

template <typename... rtypes, std::size_t... Is>
std::tuple<rtypes...> _sql_get_all(const auto& stmt, std::index_sequence<Is...>)
{
    return std::tuple { sql_get<rtypes>(stmt, Is)... };
}

template <typename... rtypes>
auto sql_get_all(const auto& stm)
{
    return _sql_get_all<rtypes...>(stm, std::index_sequence_for<rtypes...> {});
}

class BindedCompiledStatement
{
    sqlite3_stmt* stmt;

public:
    BindedCompiledStatement() : stmt(nullptr) { }

    BindedCompiledStatement(sqlite3_stmt* s) : stmt(s) { }

    ~BindedCompiledStatement()
    {
        PROFILE_HERE;
        if (stmt)
            sqlite3_reset(stmt);
    }

    [[nodiscard]] inline bool valid() const { return stmt != nullptr; }

    [[nodiscard]] inline sqlite3_stmt* get() const { return stmt; }
};

class CompiledStatement
{
    sqlite3_stmt* stmt;
    std::string source_sql;

public:
    CompiledStatement(const std::string& sql) : stmt(nullptr), source_sql(std::move(sql)) { }

    CompiledStatement(sqlite3* db, const std::string& sql) : stmt(nullptr), source_sql(sql)
    {
        compile(db);
    }

    ~CompiledStatement() { finalize(); }

    inline bool compile(sqlite3* db)
    {
        PROFILE_HERE;
        finalize();
        if (sqlite3_prepare_v2(db, source_sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
        {
            auto msg = std::string("Failed to prepare SQL statement: ")
                + sqlite3_errmsg(db);
            stmt = nullptr;
            throw std::runtime_error(msg);
        }
        return true;
    }

    inline bool finalize()
    {
        bool result = true;
        if (stmt)
        {
            if (sqlite3_finalize(stmt) != SQLITE_OK)
            {
                std::cerr << "Error finalizing statement: "
                          << sqlite3_errmsg(sqlite3_db_handle(stmt)) << std::endl;
                result = false;
                sqlite3_reset(stmt);
            }
            else
            {
                result = true;
            }
            stmt = nullptr;
        }
        return result;
    }

    [[nodiscard]] inline bool valid() const { return stmt != nullptr; }

    [[nodiscard]] inline sqlite3_stmt* get() const { return stmt; }

    [[nodiscard]] inline BindedCompiledStatement bind_all(const auto&... values) const
    {
        PROFILE_HERE_N(std::source_location::current().function_name());
        if (valid())
        {
            sql_bind_all(stmt, values...);
            return BindedCompiledStatement(stmt);
        }
        return BindedCompiledStatement();
    }
};

class Transaction
{
    sqlite3* db;
    bool committed;
    static inline constexpr auto _begin_sql = "BEGIN TRANSACTION;";
    static inline constexpr auto _begin_exclusive_sql = "BEGIN EXCLUSIVE TRANSACTION;";

public:
    Transaction(sqlite3* database, bool exclusive = false)
            : db(database), committed(false)
    {
        if (db)
        {
            PROFILE_HERE;
            char* errMsg = nullptr;
            auto begin = exclusive ? _begin_exclusive_sql : _begin_sql;

                if (sqlite3_exec(db, begin, nullptr, nullptr, &errMsg) != SQLITE_OK)
                {
                    auto msg = std::string("Failed to begin transaction: ")
                        + (errMsg ? errMsg : "unknown error");
                    if (errMsg)
                        sqlite3_free(errMsg);
                    db = nullptr;
                    throw std::runtime_error(msg);
                }
        }
    }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    Transaction(Transaction&& other) noexcept : db(other.db), committed(other.committed)
    {
        other.db = nullptr;
        other.committed = true;
    }

    Transaction& operator=(Transaction&& other) = delete;

    ~Transaction() { try_commit(); }

    [[nodiscard]] inline bool rollback()
    {
        if (db && !committed)
        {
            char* errMsg = nullptr;
            if (sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, &errMsg) != SQLITE_OK)
            {
                std::cerr << "Error rolling back transaction: "
                          << (errMsg ? errMsg : "unknown error") << std::endl;
                if (errMsg)
                    sqlite3_free(errMsg);
                return false;
            }
            committed = true;
            return true;
        }
        return false;
    }

    inline bool try_commit() noexcept
    {
        PROFILE_HERE;
        if (db && !committed)
        {
            char* errMsg = nullptr;
            if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg) != SQLITE_OK)
            {
                std::cerr << "Error committing transaction: " << (errMsg ? errMsg : "unknown error")
                          << std::endl;
                if (errMsg)
                    sqlite3_free(errMsg);
                return false;
            }
            committed = true;
            return true;
        }
        return false;
    }

    inline bool commit()
    {
        PROFILE_HERE;
        if (db && !committed)
        {
            char* errMsg = nullptr;
            if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg) != SQLITE_OK)
            {
                auto msg = std::string("Failed to commit transaction: ")
                    + (errMsg ? errMsg : "unknown error");
                if (errMsg)
                    sqlite3_free(errMsg);
                throw std::runtime_error(msg);
            }
            committed = true;
            return true;
        }
        return false;
    }
};

class Database
{
private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;

    void _ensure_parent_directory(const std::filesystem::path& db_path)
    {
        auto parent_path = db_path.parent_path();
        if (!std::filesystem::exists(parent_path))
        {
            std::filesystem::create_directories(parent_path);
        }
    }

    CompiledStatement BEGIN_STMT { "BEGIN TRANSACTION;" };
    CompiledStatement COMMIT_STMT { "COMMIT;" };

public:
    Database() : db(nullptr, SQLiteDeleter()) { ; }

    ~Database() { close(); }

    inline bool open(const std::filesystem::path& db_path, const auto init_sql)
    {
        open(db_path);
        for (const auto& sql : init_sql)
            (void)exec(sql);
        BEGIN_STMT.compile(db.get());
        COMMIT_STMT.compile(db.get());
        return true;
    }

    inline bool open(const std::filesystem::path& db_path)
    {
        PROFILE_HERE;
        sqlite3* tmp_db = nullptr;
        auto db_path_str = db_path.string();
        _ensure_parent_directory(db_path);
        int check = sqlite3_open_v2(
            db_path_str.c_str(), &tmp_db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);

        if (tmp_db && check == SQLITE_OK)
        {
            if (sqlite3_busy_timeout(tmp_db, 600'000) != SQLITE_OK)
            {
                auto msg = std::string("Failed to set busy timeout: ")
                    + sqlite3_errmsg(tmp_db);
                sqlite3_close(tmp_db);
                throw std::runtime_error(msg);
            }
            db.reset(tmp_db);
        }
        else
        {
            auto msg = std::string("Failed to open database: ")
                + (tmp_db ? sqlite3_errmsg(tmp_db) : "unknown error");
            if (tmp_db)
                sqlite3_close(tmp_db);
            throw std::runtime_error(msg);
        }
        return true;
    }

    inline bool close()
    {
        bool result = true;
        if (db)
        {
            result = sqlite3_close_v2(db.get()) == SQLITE_OK;
            if (!result)
            {
                std::cerr << "Error closing database: " << sqlite3_errmsg(db.get()) << std::endl;
                result = false;
            }
            db.reset();
        }
        return result;
    }

    [[nodiscard]] inline sqlite3* get() const { return db.get(); }

    [[nodiscard]] inline bool opened() const { return db != nullptr; }

    [[nodiscard]] inline bool exec(const std::string& sql)
    {
        PROFILE_HERE_N(std::source_location::current().function_name());
        char* errMsg = nullptr;

        int rc = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK)
        {
            auto msg = std::string("SQL error: ") + (errMsg ? errMsg : "unknown error")
                + " while executing: " + sql;
            if (errMsg)
                sqlite3_free(errMsg);
            throw std::runtime_error(msg);
        }
        return true;
    }

    [[nodiscard]] inline CompiledStatement prepare(const char* sql)
    {
        return CompiledStatement(db.get(), sql);
    }

    template <typename... rtypes>
    consteval auto exec_return_type()
    {
        if constexpr (sizeof...(rtypes) == 0)
            return bool {};
        else if constexpr (sizeof...(rtypes) == 1)
            return std::optional<rtypes...> {};
        else
            return std::optional<std::tuple<rtypes...>> {};
    }

    template <typename... rtypes>
    auto step(const BindedCompiledStatement& stmt) -> decltype(exec_return_type<rtypes...>())
    {
        PROFILE_HERE_N(std::source_location::current().function_name());
        int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_ROW)
        {
            if constexpr (sizeof...(rtypes) == 1)
            {
                return sql_get<rtypes...>(stmt.get(), 0);
            }
            if constexpr (sizeof...(rtypes) > 1)
            {
                return sql_get_all<rtypes...>(stmt.get());
            }
        }
        else if (rc == SQLITE_CONSTRAINT)
        { // handle when trying to insert a duplicate key
            if constexpr (sizeof...(rtypes) == 0)
                return false;
            else
                return std::nullopt;
        }
        else if (rc == SQLITE_DONE)
        {
            if constexpr (sizeof...(rtypes) == 0)
                return true;
            else
                return std::nullopt;
        }
        {
            std::string msg = "SQLite step unexpected return code " + std::to_string(rc);
            if (stmt.get())
            {
                auto* dbh = sqlite3_db_handle(stmt.get());
                if (dbh) msg += std::string(": ") + sqlite3_errmsg(dbh);
            }
            throw std::runtime_error(msg);
        }
    }

    template <typename... rtypes>
    auto exec(const CompiledStatement& stmt, const auto&... values)
        -> decltype(exec_return_type<rtypes...>())
    {
        PROFILE_HERE_N(std::source_location::current().function_name());
        using namespace cpp_utils::lifetime;
        if (stmt.valid())
        {
            auto binded = stmt.bind_all(std::forward<decltype(values)>(values)...);
            return step<rtypes...>(binded);
        }
        if constexpr (sizeof...(rtypes) == 0)
            return false;
        else
            return std::nullopt;
    }

    template <typename... rtypes>
    auto exec(const std::string& sql, const auto&... values)
        -> decltype(exec_return_type<rtypes...>())
    {
        PROFILE_HERE_N(std::source_location::current().function_name());
        return exec<rtypes...>(CompiledStatement { db.get(), sql },
                               std::forward<decltype(values)>(values)...);
    }

    Transaction begin_transaction(bool exclusive = false) { return Transaction(db.get(),exclusive); }
};
