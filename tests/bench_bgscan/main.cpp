// Does bulk-insert throughput decay with row count, and is the decay caused by
// the background checkpoint thread's O(N) `SELECT COUNT(*), SUM(size)` running
// every second while holding _mtx?
//
// Discriminator: a B-tree/page-cache slowdown raises latency smoothly for every
// op. A background scan holding the writer's lock produces PERIODIC STALLS --
// median latency stays flat, max latency per window grows linearly with N.

#include <sciqlop_cache.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

using clk = std::chrono::steady_clock;

static double ms(clk::duration d)
{
    return std::chrono::duration<double, std::milli>(d).count();
}

template <typename Store>
static void run(const std::string& path, std::size_t n_total, std::size_t window,
                std::size_t value_size)
{
    std::filesystem::remove_all(path);
    Store store { path };

    const std::string value(value_size, 'x');
    std::vector<double> lat;
    lat.reserve(window);

    std::printf("rows,ops_per_s,median_us,p99_us,max_ms,stalls_over_10ms,db_mb,wal_mb,elapsed_s\n");

    const auto t_start = clk::now();
    auto t_win = t_start;

    for (std::size_t i = 0; i < n_total; ++i)
    {
        char key[32];
        std::snprintf(key, sizeof(key), "key-%09zu", i);

        const auto t0 = clk::now();
        store.set(std::string(key), value);
        lat.push_back(ms(clk::now() - t0));

        if ((i + 1) % window == 0)
        {
            const auto now = clk::now();
            const double win_s = ms(now - t_win) / 1000.0;

            std::vector<double> sorted = lat;
            std::sort(sorted.begin(), sorted.end());
            const double median = sorted[sorted.size() / 2];
            const double p99 = sorted[static_cast<std::size_t>(sorted.size() * 0.99)];
            const double max = sorted.back();
            const std::size_t stalls
                = std::count_if(lat.begin(), lat.end(), [](double v) { return v > 10.0; });

            double db_mb = 0, wal_mb = 0;
            std::error_code ec;
            for (auto& e : std::filesystem::directory_iterator(path, ec))
            {
                if (!e.is_regular_file(ec)) continue;
                const auto ext = e.path().extension().string();
                if (ext == ".db") db_mb += static_cast<double>(e.file_size(ec)) / 1e6;
                if (ext == ".db-wal") wal_mb += static_cast<double>(e.file_size(ec)) / 1e6;
            }

            std::printf("%zu,%.0f,%.1f,%.1f,%.1f,%zu,%.1f,%.1f,%.1f\n", i + 1,
                        static_cast<double>(window) / win_s, median * 1000.0, p99 * 1000.0,
                        max, stalls, db_mb, wal_mb, ms(now - t_start) / 1000.0);
            std::fflush(stdout);

            lat.clear();
            t_win = now;
        }
    }
}

int main(int argc, char** argv)
{
    std::size_t n_total = 200000, window = 5000, value_size = 4096;
    std::string path = "/tmp/bench-bgscan-store";
    std::string type = "cache";

    for (int i = 1; i + 1 < argc; i += 2)
    {
        const std::string a = argv[i];
        if (a == "--n") n_total = std::stoul(argv[i + 1]);
        else if (a == "--window") window = std::stoul(argv[i + 1]);
        else if (a == "--value-size") value_size = std::stoul(argv[i + 1]);
        else if (a == "--path") path = argv[i + 1];
        else if (a == "--type") type = argv[i + 1];
    }

    std::fprintf(stderr, "type=%s n=%zu window=%zu value_size=%zu\n", type.c_str(), n_total,
                 window, value_size);

    if (type == "index")
        run<Index>(path, n_total, window, value_size);
    else
        run<Cache>(path, n_total, window, value_size);

    std::filesystem::remove_all(path);
    return 0;
}
