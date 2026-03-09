#include <filesystem>
#include <random>
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

class AutoCleanDirectory
{
    std::filesystem::path path_;

    static std::string unique_suffix()
    {
        return std::to_string(getpid()) + "_" + std::to_string(std::random_device{}());
    }

public:

    AutoCleanDirectory(const std::string& test_name, bool use_temp_dir = true)
    {
        auto dir_name = test_name + "_" + unique_suffix();
        if (use_temp_dir)
            path_ = std::filesystem::temp_directory_path() / dir_name;
        else
            path_ = std::filesystem::path{"."} / dir_name;

        std::filesystem::create_directories(path_);
    }

    ~AutoCleanDirectory()
    {
        if (std::filesystem::exists(path_))
        {
            std::filesystem::remove_all(path_);
        }
    }
    const std::filesystem::path& path() const { return path_; }
};

static inline constexpr auto INIT_STMTS = {
    R"(
            -- Use Write-Ahead Logging for better concurrency
            PRAGMA journal_mode=WAL;
            -- Set synchronous mode to NORMAL for performance
            PRAGMA synchronous=NORMAL;
            -- Set the number of cache pages
            PRAGMA cache_size=10000;
            -- Store temporary tables in memory
            PRAGMA temp_store=MEMORY;
            -- Set memory-mapped I/O size for performance
            PRAGMA mmap_size=268435456;
            -- Limit the number of rows analyzed for query planning
            PRAGMA analysis_limit=1000;
            PRAGMA recursive_triggers=ON;)",

    R"(
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY NOT NULL,
                path TEXT DEFAULT NULL,
                value BLOB DEFAULT NULL,
                expire REAL DEFAULT NULL,
                last_update REAL NOT NULL DEFAULT (unixepoch('now')),
                last_use REAL NOT NULL DEFAULT (unixepoch('now')),
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

