#include <filesystem>
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

class AutoCleanDirectory
{
    std::filesystem::path path_;

public:

    AutoCleanDirectory(const std::string& test_name, bool use_temp_dir = true)
    {
        if (use_temp_dir)
            path_ = std::filesystem::temp_directory_path() / test_name;
        else
            path_ = std::filesystem::path{"."}/test_name;

        if (std::filesystem::exists(path_))
        {
            std::filesystem::remove_all(path_);
        }
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
            PRAGMA analysis_limit=1000;)",

    R"(
            CREATE TABLE IF NOT EXISTS cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                path TEXT DEFAULT NULL,
                value BLOB DEFAULT NULL,
                expire REAL DEFAULT NULL,
                last_update REAL NOT NULL DEFAULT (strftime('%s', 'now')),
                last_use REAL NOT NULL DEFAULT (strftime('%s', 'now')),
                access_count_since_last_update INT NOT NULL DEFAULT 0,
                size INT NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_key ON cache (key);

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value
            );

            INSERT OR IGNORE INTO meta (key, value) VALUES ('size', '0');

            -- Trigger for INSERT
            CREATE TRIGGER IF NOT EXISTS cache_size_insert
            AFTER INSERT ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;

            -- Trigger for DELETE
            CREATE TRIGGER IF NOT EXISTS cache_size_delete
            AFTER DELETE ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;

            -- Trigger for UPDATE OF size
            CREATE TRIGGER IF NOT EXISTS cache_size_update
            AFTER UPDATE OF size ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;
)"

};

