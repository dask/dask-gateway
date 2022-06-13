import os
import sys

import dask_gateway_server

# Project settings
project = "Dask Gateway"
copyright = "2021, Jim Crist-Harif"
author = "Jim Crist-Harif"
release = version = dask_gateway_server.__version__

source_suffix = [".rst", ".md"]
root_doc = master_doc = "index"
language = None
# Commenting this out for now, if we register dask pygments,
# then eventually this line can be:
# pygments_style = "dask"
exclude_patterns = []

# Sphinx Extensions
docs = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(docs, "sphinxext"))
extensions = [
    "autodoc_traits",
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.napoleon",
]

extlinks = {
    "issue": ("https://github.com/dask/dask-gateway/issues/%s", "Issue #"),
    "pr": ("https://github.com/dask/dask-gateway/pull/%s", "PR #"),
}

# Sphinx Theme
html_theme = "dask_sphinx_theme"
templates_path = ["_templates"]

# -- Options for linkcheck builder -------------------------------------------
# http://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-the-linkcheck-builder
#
linkcheck_ignore = [
    r"(.*)github\.com(.*)#",  # javascript based anchors
    r"https://github.com/[^/]*$",  # too many github usernames / searches in changelog
    "https://github.com/jupyterhub/oauthenticator/pull/",  # too many PRs in changelog
    "https://github.com/jupyterhub/oauthenticator/compare/",  # too many comparisons in changelog
]
linkcheck_anchors_ignore = [
    "/#!",
    "/#%21",
]
