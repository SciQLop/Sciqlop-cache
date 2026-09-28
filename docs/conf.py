#!/usr/bin/env python
# Sphinx configuration for the SciQLop Cache documentation.
import os

_here = os.path.dirname(os.path.abspath(__file__))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosectionlabel',
    'sphinx_design',
    'sphinx_copybutton',
    'sphinxcontrib.mermaid',
]

autosectionlabel_prefix_document = True
autodoc_member_order = 'bysource'
autodoc_default_options = {'undoc-members': False}

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
# docs/ also holds design notes and investigations that are not part of the site.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'superpowers', 'plans', 'known-issues',
                    'compat-probes', '*.md']

project = 'SciQLop Cache'
copyright = '2024-2026, Alexis Jeandet'
author = 'Alexis Jeandet'

with open(os.path.join(_here, '..', 'version.txt')) as f:
    version = f.read().strip()
release = version

language = 'en'

html_theme = 'furo'
html_title = f'SciQLop Cache {version}'
html_theme_options = {
    'source_repository': 'https://github.com/SciQLop/Sciqlop-cache/',
    'source_branch': 'main',
    'source_directory': 'docs/',
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'diskcache': ('https://grantjenks.com/docs/diskcache/', None),
}
