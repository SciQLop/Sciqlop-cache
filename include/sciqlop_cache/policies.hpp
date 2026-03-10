#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <type_traits>

template <typename Policy, typename... Policies>
inline constexpr bool has_policy_v = (std::is_same_v<Policy, Policies> || ...);

// Each policy that contributes columns to INSERT/REPLACE must define
// insert_columns() and insert_placeholders(). The fold expression in _Store
// concatenates them in Policies... declaration order, which must match
// extra_columns() order. This eliminates the hardcoded ordering trap.

struct WithExpiration
{
    static std::string where_valid() { return " AND (expire IS NULL OR expire > unixepoch('now'))"; }
    static std::string extra_columns() { return ", expire REAL DEFAULT NULL"; }
    static std::string extra_indexes() { return ""; }
    static std::string insert_columns() { return ", expire"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithEviction
{
    std::atomic<std::size_t> _access_seq { 0 };

    static std::string where_valid() { return ""; }
    static std::string extra_columns()
    {
        return ", last_update REAL NOT NULL DEFAULT 0"
               ", last_use REAL NOT NULL DEFAULT 0"
               ", access_count_since_last_update INT NOT NULL DEFAULT 0";
    }
    static std::string extra_indexes() { return ""; }
    static std::string insert_columns() { return ", last_use"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithTags
{
    static std::string where_valid() { return ""; }
    static std::string extra_columns() { return ", tag TEXT DEFAULT NULL"; }
    static std::string extra_indexes()
    {
        return "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag) WHERE tag IS NOT NULL;";
    }
    static std::string insert_columns() { return ", tag"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithStats
{
    std::atomic<uint64_t> _hits { 0 };
    std::atomic<uint64_t> _misses { 0 };

    static std::string where_valid() { return ""; }
    static std::string extra_columns() { return ""; }
    static std::string extra_indexes() { return ""; }
    static std::string insert_columns() { return ""; }
    static std::string insert_placeholders() { return ""; }
};
