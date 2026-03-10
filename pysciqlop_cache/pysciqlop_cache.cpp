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

inline void _set_item(Cache& c, const std::string& key, nb::bytes& buffer,
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

inline bool _add_item(Cache& c, const std::string& key, nb::bytes& buffer,
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


    nb::class_<Cache>(m, "Cache")
        .def(nb::init<const std::string&, size_t>(), "cache_path"_a = ".cache/",
             "max_size"_a = 0)
        .def("count", &Cache::count)
        .def("__len__", &Cache::count)
        .def("set", _set_item, nb::arg("key"), nb::arg("value"), nb::arg("expire") = nb::none(),
             nb::arg("tag") = nb::none())
        .def(
            "__setitem__", [](Cache& c, const std::string& key, nb::bytes& buffer)
            { return _set_item(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &Cache::get, nb::arg("key"))
        .def("__getitem__", &Cache::get, nb::arg("key"))
        .def("keys", &Cache::keys)
        .def("exists", &Cache::exists, nb::arg("key"))
        .def("add", _add_item, nb::arg("key"), nb::arg("value"),
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
        .def("check", &Cache::check)
        .def("set_meta", &Cache::set_meta, nb::arg("key"), nb::arg("value"))
        .def("get_meta", &Cache::get_meta, nb::arg("key"))
        .def("set_max_cache_size", &Cache::set_max_cache_size, nb::arg("value"))
        .def("path", [](Cache& c) { return c.path().string(); });
}
