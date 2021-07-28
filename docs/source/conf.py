import os
import sys

import dask_gateway_server

# Project settings
project = "Dask Gateway"
copyright = "2021, Jim Crist-Harif"
author = "Jim Crist-Harif"
release = version = dask_gateway_server.__version__

source_suffix = ".rst"
master_doc = "index"
language = None
pygments_style = "sphinx"
exclude_patterns = []

# Sphinx Extensions
docs = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(docs, "sphinxext"))
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.napoleon",
    "autodoc_traitlets",
]

extlinks = {
    "issue": ("https://github.com/dask/dask-gateway/issues/%s", "Issue #"),
    "pr": ("https://github.com/dask/dask-gateway/pull/%s", "PR #"),
}

# Sphinx Theme
html_theme = "dask_sphinx_theme"
templates_path = ["_templates"]
