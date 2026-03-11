#include "sciqlop_cache/sciqlop_cache.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/chrono.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <fmt/ranges.h>

using namespace std::chrono_literals;

namespace nb = nanobind;
using namespace nb::literals;

using OptDuration = std::optional<std::chrono::system_clock::duration>;
using OptString = std::optional<std::string>;

template <typename T>
inline void _set_item_impl(T& c, const std::string& key, nb::bytes& buffer,
                            OptDuration expire = std::nullopt,
                            OptString tag = std::nullopt)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    if (expire && tag)
        c.set(key, data, *expire, *tag);
    else if (expire)
        c.set(key, data, *expire);
    else if (tag)
        c.set(key, data, *tag);
    else
        c.set(key, data);
}

template <typename T>
inline bool _add_item_impl(T& c, const std::string& key, nb::bytes& buffer,
                            OptDuration expire = std::nullopt,
                            OptString tag = std::nullopt)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    if (expire && tag)
        return c.add(key, data, *expire, *tag);
    else if (expire)
        return c.add(key, data, *expire);
    else if (tag)
        return c.add(key, data, *tag);
    else
        return c.add(key, data);
}

template <typename T>
inline void _simple_set_item(T& s, const std::string& key, nb::bytes& buffer)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    s.set(key, data);
}

template <typename T>
inline bool _simple_add_item(T& s, const std::string& key, nb::bytes& buffer)
{
    auto data = std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size());
    return s.add(key, data);
}

NB_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    nb::class_<Buffer>(m, "Buffer")
        .def("memoryview",
             [](Buffer& b) -> nb::object
             {
                 // Check if the buffer is valid
                 if (!b) {
                     throw std::runtime_error("Cannot create memory view of invalid buffer");
                 }

                 // Handle the case where data is null but size is zero
                 if (b.data() == nullptr && b.size() == 0) {
                     return nb::steal(PyMemoryView_FromMemory(nullptr, 0, PyBUF_READ));
                 }

                 Py_buffer view;

                 if (PyBuffer_FillInfo(&view, NULL, const_cast<char*>(b.data()),
                                       b.size(), 1, PyBUF_SIMPLE) == -1) {
                     throw std::runtime_error("Failed to create memory view");
                 }

                 PyObject* memview = PyMemoryView_FromBuffer(&view);
                 if (!memview) {
                     throw std::runtime_error("Failed to create memory view object");
                 }

                 return nb::borrow(memview);
             });


    nb::class_<Cache::CheckResult>(m, "CacheCheckResult")
        .def_ro("ok", &Cache::CheckResult::ok)
        .def_ro("orphaned_files", &Cache::CheckResult::orphaned_files)
        .def_ro("dangling_rows", &Cache::CheckResult::dangling_rows)
        .def_ro("size_mismatches", &Cache::CheckResult::size_mismatches)
        .def_ro("counters_consistent", &Cache::CheckResult::counters_consistent)
        .def_ro("sqlite_integrity_ok", &Cache::CheckResult::sqlite_integrity_ok);

    nb::class_<Index::CheckResult>(m, "IndexCheckResult")
        .def_ro("ok", &Index::CheckResult::ok)
        .def_ro("orphaned_files", &Index::CheckResult::orphaned_files)
        .def_ro("dangling_rows", &Index::CheckResult::dangling_rows)
        .def_ro("size_mismatches", &Index::CheckResult::size_mismatches)
        .def_ro("counters_consistent", &Index::CheckResult::counters_consistent)
        .def_ro("sqlite_integrity_ok", &Index::CheckResult::sqlite_integrity_ok);

    nb::class_<Cache::TransactionGuard>(m, "CacheTransactionGuard")
        .def("commit", &Cache::TransactionGuard::commit)
        .def("rollback", &Cache::TransactionGuard::rollback);

    nb::class_<Index::TransactionGuard>(m, "IndexTransactionGuard")
        .def("commit", &Index::TransactionGuard::commit)
        .def("rollback", &Index::TransactionGuard::rollback);

    nb::class_<Cache>(m, "Cache")
        .def(nb::init<const std::string&, size_t>(), "cache_path"_a = ".cache/",
             "max_size"_a = 0)
        .def("count", &Cache::count)
        .def("__len__", &Cache::count)
        .def("set", _set_item_impl<Cache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def(
            "__setitem__", [](Cache& c, const std::string& key, nb::bytes& buffer)
            { _set_item_impl(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &Cache::get, nb::arg("key"))
        .def("__getitem__", &Cache::get, nb::arg("key"))
        .def("keys", &Cache::keys)
        .def("exists", &Cache::exists, nb::arg("key"))
        .def("add", _add_item_impl<Cache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("delete", &Cache::del, nb::arg("key"))
        .def("pop", &Cache::pop, nb::arg("key"))
        .def(
            "touch",
            [](Cache& c, const std::string& key, std::chrono::system_clock::duration expire)
            { return c.touch(key, expire); }, nb::arg("key"), nb::arg("expire"))
        .def("expire", &Cache::expire)
        .def("evict", &Cache::evict)
        .def("evict_tag", &Cache::evict_tag, nb::arg("tag"))
        .def("incr", &Cache::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &Cache::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &Cache::clear)
        .def("check", &Cache::check, nb::arg("fix") = false)
        .def("set_meta", &Cache::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &Cache::get_meta, nb::arg("key"))
        .def("size", &Cache::size)
        .def("set_max_cache_size", &Cache::set_max_cache_size, nb::arg("value"))
        .def("path", [](Cache& c) { return c.path().string(); })
        .def("stats", [](Cache& c) {
            auto s = c.stats();
            nb::dict d;
            d["hits"] = s.hits;
            d["misses"] = s.misses;
            return d;
        })
        .def("reset_stats", &Cache::reset_stats)
        .def("begin_user_transaction", &Cache::begin_user_transaction);

    nb::class_<Index>(m, "Index")
        .def(nb::init<const std::string&>(), "path"_a = ".index/")
        .def("count", &Index::count)
        .def("__len__", &Index::count)
        .def("set", _simple_set_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("__setitem__", _simple_set_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("get", &Index::get, nb::arg("key"))
        .def("__getitem__", &Index::get, nb::arg("key"))
        .def("keys", &Index::keys)
        .def("exists", &Index::exists, nb::arg("key"))
        .def("add", _simple_add_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("delete", &Index::del, nb::arg("key"))
        .def("pop", &Index::pop, nb::arg("key"))
        .def("incr", &Index::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &Index::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &Index::clear)
        .def("check", &Index::check, nb::arg("fix") = false)
        .def("size", &Index::size)
        .def("set_meta", &Index::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &Index::get_meta, nb::arg("key"))
        .def("path", [](Index& idx) { return idx.path().string(); })
        .def("begin_user_transaction", &Index::begin_user_transaction);

    nb::class_<FanoutCache>(m, "FanoutCache")
        .def(nb::init<const std::string&, std::size_t, std::size_t>(),
             "cache_path"_a = ".cache/", "shard_count"_a = 8, "max_size"_a = 0)
        .def("count", &FanoutCache::count)
        .def("__len__", &FanoutCache::count)
        .def("set", _set_item_impl<FanoutCache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def(
            "__setitem__", [](FanoutCache& c, const std::string& key, nb::bytes& buffer)
            { _set_item_impl(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutCache::get, nb::arg("key"))
        .def("__getitem__", &FanoutCache::get, nb::arg("key"))
        .def("keys", &FanoutCache::keys)
        .def("exists", &FanoutCache::exists, nb::arg("key"))
        .def("add", _add_item_impl<FanoutCache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("delete", &FanoutCache::del, nb::arg("key"))
        .def("pop", &FanoutCache::pop, nb::arg("key"))
        .def(
            "touch",
            [](FanoutCache& c, const std::string& key, std::chrono::system_clock::duration expire)
            { return c.touch(key, expire); }, nb::arg("key"), nb::arg("expire"))
        .def("expire", &FanoutCache::expire)
        .def("evict", &FanoutCache::evict)
        .def("evict_tag", &FanoutCache::evict_tag, nb::arg("tag"))
        .def("incr", &FanoutCache::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &FanoutCache::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &FanoutCache::clear)
        .def("check", &FanoutCache::check, nb::arg("fix") = false)
        .def("set_meta", &FanoutCache::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &FanoutCache::get_meta, nb::arg("key"))
        .def("size", &FanoutCache::size)
        .def("shard_count", &FanoutCache::shard_count)
        .def("set_max_cache_size", &FanoutCache::set_max_cache_size, nb::arg("value"))
        .def("path", [](FanoutCache& c) { return c.path().string(); })
        .def("stats", [](FanoutCache& c) {
            auto s = c.stats();
            nb::dict d;
            d["hits"] = s.hits;
            d["misses"] = s.misses;
            return d;
        })
        .def("reset_stats", &FanoutCache::reset_stats)
        .def("begin_user_transaction", &FanoutCache::begin_user_transaction, nb::arg("key"));

    nb::class_<FanoutIndex>(m, "FanoutIndex")
        .def(nb::init<const std::string&, std::size_t>(),
             "path"_a = ".index/", "shard_count"_a = 8)
        .def("count", &FanoutIndex::count)
        .def("__len__", &FanoutIndex::count)
        .def("set", _simple_set_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("__setitem__", _simple_set_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutIndex::get, nb::arg("key"))
        .def("__getitem__", &FanoutIndex::get, nb::arg("key"))
        .def("keys", &FanoutIndex::keys)
        .def("exists", &FanoutIndex::exists, nb::arg("key"))
        .def("add", _simple_add_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("delete", &FanoutIndex::del, nb::arg("key"))
        .def("pop", &FanoutIndex::pop, nb::arg("key"))
        .def("incr", &FanoutIndex::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("decr", &FanoutIndex::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0)
        .def("clear", &FanoutIndex::clear)
        .def("check", &FanoutIndex::check, nb::arg("fix") = false)
        .def("size", &FanoutIndex::size)
        .def("shard_count", &FanoutIndex::shard_count)
        .def("set_meta", &FanoutIndex::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &FanoutIndex::get_meta, nb::arg("key"))
        .def("path", [](FanoutIndex& idx) { return idx.path().string(); })
        .def("begin_user_transaction", &FanoutIndex::begin_user_transaction, nb::arg("key"));
}
