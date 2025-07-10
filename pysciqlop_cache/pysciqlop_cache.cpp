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

PYBIND11_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    py::class_<Cache>(m, "Cache")
        .def(py::init<const std::string&, size_t>(), py::arg("db_path") = "sciclop-cache.db",
            py::arg("max_size") = 1000)
        .def("count", &Cache::count)
        .def("__len__", &Cache::count)
        .def(
            "set",
            [](Cache& c, const std::string& key, const std::string& value,
                std::chrono::system_clock::duration expire) { return c.set(key, value, expire); },
            py::arg("key"), py::arg("value"), py::arg("expire") = 3600s)
        .def(
            "__setitem__", [](Cache& c, const std::string& key, const std::string& value)
            { return c.set(key, value); }, py::arg("key"), py::arg("value"))
        .def("get", &Cache::get, py::arg("key"))
        .def(
            "__getitem__", &Cache::get, py::arg("key"))
        .def("keys", &Cache::keys)
        .def(
            "add",
            [](Cache& c, const std::string& key, const std::string& value,
                std::chrono::system_clock::duration expire) { return c.add(key, value, expire); },
            py::arg("key"), py::arg("value"), py::arg("expire") = 3600s)
        .def("del", &Cache::del, py::arg("key"))
        .def("pop", &Cache::pop, py::arg("key"))
        .def(
            "touch",
            [&](Cache& c, const std::string& key, std::chrono::system_clock::duration expire)
            { return c.touch(key, expire); }, py::arg("key"), py::arg("expire") = 3600s)
        .def("expire", &Cache::expire)
        .def("evict", &Cache::evict)
        .def("clear", &Cache::clear)
        .def("check", &Cache::check);
}
