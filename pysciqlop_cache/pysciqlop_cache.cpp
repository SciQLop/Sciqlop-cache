#include "sciqlop_cache/sciqlop_cache.hpp"

#include <pybind11/chrono.h>
#include <pybind11/iostream.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <fmt/ranges.h>

#include "include/sciqlop_cache/sciqlop_cache.hpp"
using namespace std::chrono_literals;

namespace py = pybind11;

struct _bytes
{
    std::vector<char> data;
};

inline void _set_item(Cache& c, const std::string& key, py::bytes& buffer,
    std::chrono::system_clock::duration* expire = nullptr)
{
    py::buffer_info info(py::buffer(buffer).request());
    if (expire == nullptr)
    {
        c.set(key, std::span<const char>( static_cast<const char*>(info.ptr), info.size));
    }
    else
    {
        c.set(key, std::span<const char>( static_cast<const char*>(info.ptr), info.size), *expire);
    }
}

inline void _add_item(Cache& c, const std::string& key, py::bytes& buffer,
    std::chrono::system_clock::duration* expire = nullptr)
{
    py::buffer_info info(py::buffer(buffer).request());
    if (expire == nullptr)
    {
        c.add(key, std::span<const char>( static_cast<const char*>(info.ptr), info.size));
    }
    else
    {
        c.add(key, std::span<const char>( static_cast<const char*>(info.ptr), info.size), *expire);
    }
}

inline std::optional<_bytes> _get_item(Cache& c, const std::string& key)
{
    if (auto value = c.get(key))
    {
        return _bytes { std::move( *value) };
    }
    return std::nullopt;
}


PYBIND11_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    py::class_<_bytes>(m, "_bytes", py::buffer_protocol())
        .def_buffer(
            [](_bytes& b) -> py::buffer_info
            {
                py::gil_scoped_release release;
                return py::buffer_info(b.data.data(), std::size(b.data), true);
            });

    py::class_<Cache>(m, "Cache")
        .def(py::init<const std::string&, size_t>(), py::arg("cache_path") = ".cache/",
            py::arg("max_size") = 1000)
        .def("count", &Cache::count)
        .def("__len__", &Cache::count)
        .def("set", _set_item, py::arg("key"), py::arg("value"),
            py::arg("expire") = 3600s)
        .def(
            "__setitem__", [](Cache& c, const std::string& key, py::bytes& buffer)
            { return _set_item(c, key, buffer); }, py::arg("key"), py::arg("value"))
        .def("get", _get_item, py::arg("key"))
        .def("__getitem__", _get_item, py::arg("key"))
        .def("keys", &Cache::keys)
        .def("exists", &Cache::exists, py::arg("key"))
        .def("add", _add_item,
            py::arg("key"), py::arg("value"), py::arg("expire").none(true) = 3600s)
        .def("delete", &Cache::del, py::arg("key"))
        .def("pop", &Cache::pop, py::arg("key"))
        .def("touch",
            [&](Cache& c, const std::string& key, std::chrono::system_clock::duration expire)
            { return c.touch(key, expire); }, py::arg("key"), py::arg("expire").none(true) = 3600s)
        .def("expire", &Cache::expire)
        .def("evict", &Cache::evict)
        .def("clear", &Cache::clear)
        .def("check", &Cache::check);
}
