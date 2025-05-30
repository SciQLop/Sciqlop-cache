pysciqlop_cache_python_sources = [files('__init__.py')]
fs = import('fs')
fs.copyfile('__init__.py', '__init__.py')

pymod = import('python')
python3 = pymod.find_installation('python3')

if meson.get_compiler('cpp').get_id() == 'msvc'
    pysciqlop_cache_cpp_args = ['/bigobj']
else
    pysciqlop_cache_cpp_args = []
endif


_pysciqlop_cache = python3.extension_module('_pysciqlop_cache', 'pysciqlop_cache.cpp',
                         dependencies: [sciqlop_cache_dep, fmt_dep, pybind11_dep,python3.dependency()],
                         link_args: link_args,
                         cpp_args: pysciqlop_cache_cpp_args,
                         subdir: 'pysciqlop_cache',
                         install: true,
                         extra_files: [pysciqlop_cache_headers, sciqlop_cache_headers, sciqlop_cache_extra_files]
                        )

python3.install_sources(pysciqlop_cache_python_sources, pure: false, subdir: 'pysciqlop_cache')