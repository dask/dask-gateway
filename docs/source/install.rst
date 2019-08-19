Install Dask-Gateway
====================

As mentioned in :doc:`the architecture overview <overview>`, dask-gateway is split into severl parts.  The pieces can easily be install with two packages (pip or conda) or can be installed from source.  Typically, we don't expect users to install from source unless adding new features and/or debugging.


Conda
-----

To install the latest version of dask-gateway from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.anaconda.com/downloads>`_::

    conda install dask-gateway dask-gateway-server -c conda-forge

Pip
---

Or install distributed with ``pip``::

    pip install dask-gateway dask-gateway-server

Source
------

To install dask-gateway from source, clone the repository from `github
<https://github.com/jcrist/dask-gateway>`_.

*Note, installing dask-gateway-server from source requires golang >=1.12 -- if using conda, this can be installed from conda-forge*::

    git clone https://github.com/jcrist/dask-gateway.git
    cd dask-gateway
    pip install -e .
    cd ../dask-gateway-server
    pip install -e .
