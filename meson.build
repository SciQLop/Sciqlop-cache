project(
  'sciqlop-cache',
  'cpp','c',
  meson_version: '>= 1.4.0',
  version : run_command('scripts/version.py',
                        capture:true,
                        env:{'SRC_ROOT':meson.project_source_root()}
                        ).stdout().strip() ,
  default_options : ['warning_level=3', 'cpp_std=c++20', 'default_library=static', 'buildtype=release'],
  license : 'MIT'
)

if get_option('buildtype').contains('debug')
    add_project_arguments('-DCDFPP_ENABLE_ASSERT', language : ['cpp'])
    add_project_arguments('-DCDFPP_HEDLEY', language : ['cpp'])
endif

conf_data = configuration_data()
conf_data.set_quoted('SCIQLOP_CACHE_VERSION', meson.project_version())

configure_file(output : 'sciqlop_cache_config.h',
               install : true,
               install_dir : 'include/sciqlop_cache',
               configuration : conf_data)

cpp = meson.get_compiler('cpp')
if('clang'==cpp.get_id())
    add_global_arguments('-fsized-deallocation', language : 'cpp')
endif

pybind11_dep = dependency('pybind11')
hedley_dep = dependency('hedley')
fmt_dep = dependency('fmt')
sqlite3_dep = dependency('sqlite3')
cpp_utils = dependency('cpp_utils', required : true,  default_options : ['cpp_std=c++20'])

if build_machine.system() == 'windows'
    link_args = ['-static-libstdc++','-static-libgcc','-static']
elif build_machine.system() == 'darwin' and cpp.get_id() != 'clang'
    link_args = ['-static-libstdc++','-static-libgcc']
else
    link_args = []
endif

sciqlop_cache_headers = files(
    'include/sciqlop_cache/sciqlop_cache.hpp',
    'include/sciqlop_cache/database.hpp'
)

pysciqlop_cache_headers = files(
)

sciqlop_cache_extra_files = files(
    'pyproject.toml',
    'README.md',
    '.bumpversion.cfg',
    'version.txt',
    'scripts/version.py',
    '.github/workflows/CI.yml',
    '.github/workflows/tests-with-coverage.yml'
)

sciqlop_cache_dep_inc = include_directories('include', '.')

sciqlop_cache_dep = declare_dependency(include_directories: sciqlop_cache_dep_inc,
                                dependencies: [hedley_dep, fmt_dep, sqlite3_dep, cpp_utils])

if get_option('disable_python_wrapper')
    message('building without Python wrapper')
else
    subdir('pysciqlop_cache')
endif

if get_option('with_tests')

    configure_file(output : 'tests_config.hpp',
      configuration : {
        'DATA_PATH' : '"' + meson.current_source_dir() / 'tests/resources' + '"'
      }
    )

catch_dep = dependency('catch2-with-main', version:'>3.0.0', required : true)
my_include = include_directories('include/sciqlop_cache')

foreach test:['basic', 'intermediate']
    exe = executable(
        'test-'+test,'tests/'+test+'/main.cpp',
        include_directories: my_include,
        dependencies:[catch_dep, sciqlop_cache_dep, sqlite3_dep],
        install: false
    )
    test(test, exe)
endforeach

endif

summary({'C': meson.get_compiler('c').cmd_array(),
        'C++': meson.get_compiler('cpp').cmd_array()
        }, section: 'Compilers')
