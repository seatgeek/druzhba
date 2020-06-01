# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('..'))
from druzhba.main import _get_parser


cli_help = _get_parser().format_help()
cli_help = cli_help.replace("sphinx-build", "druzhba")  # argparse grabs current process name
cli_help = '\n'.join(['  ' + line for line in cli_help.split('\n')])
with open('usage.rst', 'w') as f:
    f.write('.. code-block:: text\n\n')
    f.write(cli_help)
    f.write('\n')

# -- Project information -----------------------------------------------------

project = 'Druzhba'
copyright = '2020, The Druzhba Authors'
author = 'The Druzhba Authors'

# The full version, including alpha/beta/rc tags
release = '0.1.1'
version = 'master' # TODO: FIXME

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = []

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', 'release.rst']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


rst_prolog = """
.. |example-link| raw:: html

    <a href="https://github.com/seatgeek/druzhba/tree/{version}/test/integration/config">examples</a>
""".format(version=version)

