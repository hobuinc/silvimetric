# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


import sys, os, re
import time
import datetime
import silvimetric

if os.environ.get('SOURCE_DATE_EPOCH'):
    year  = datetime.datetime.utcfromtimestamp(int(os.environ.get('SOURCE_DATE_EPOCH', time.gmtime()))).year
    today = datetime.datetime.utcfromtimestamp(int(os.environ.get('SOURCE_DATE_EPOCH', time.gmtime()))).strftime('%B %d, %Y')
else:
    year  = datetime.datetime.now().year




project = 'SilviMetric'
copyright = u'%d. Hobu, Inc' % year
author = 'Kyle Mann, Howard Butler, Bob McGaughey'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ['_static']
html_context = {
  'display_github': True,
  'theme_vcs_pageview_mode': 'edit',
  'github_user': 'hobuinc',
  'github_repo': 'silvimetric',
  'github_version': 'main',
  'conf_py_path': '/docs/source/'
}



# TODO remove this
# def read_version(filename):
#     #
#     # project(PDAL VERSION 0.9.8 LANGUAGES CXX C)
#     data = open(filename).readlines()

#     token = '__version__'

#     version = None
#     for line in data:
#         if str(token) in line:
#             match = re.search(r'\d.\d.\d', line)
#             if match is not None:
#                 version = match.group(0)
#                 break
#     return version

release = silvimetric.__version__
version = release


pygments_style = 'sphinx'

extensions = [
   'sphinx.ext.autodoc',
   'sphinx.ext.autosummary',
]

autodoc_default_options = {
    'member-order': 'groupwise'
}

autodoc_mock_imports = [
    "pyproj",
    "osgeo",
    "distributed",
    "dask.dataframe",
    "scipy",
    "dill",
    "shapely",
    "pdal",
    "gdal",
    "pandas"
]