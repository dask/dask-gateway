Development
===========

This page provides information on how to build, test, and develop
``dask-gateway``.


Building Dask-Gateway
---------------------

Clone Repository
~~~~~~~~~~~~~~~~

Clone the Dask-Gateway git repository:

.. code-block:: shell

    $ git clone https://github.com/dask/dask-gateway.git


Install Dependencies (Conda)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We recommend using the Conda_ package manager to setup your development
environment. Here we setup a conda environment to contain the build
dependencies.

.. code-block:: shell

    # Create a new conda environment
    $ conda create -n dask-gateway

    # Activate environment
    $ conda activate dask-gateway

    # Install dependencies
    $ conda install -c conda-forge \
        cryptography \
        tornado \
        traitlets \
        sqlalchemy \
        dask

Besides the above dependencies, you'll also need a Go_ compiler. You can
install Go using your system package manager, the `Go website`_, or use Conda:

.. code-block:: shell

    $ conda install -c conda-forge go


Install Dependencies (Pip)
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also setup the development environment using ``pip``.

.. code-block:: shell

    $ pip install \
        cryptography \
        tornado \
        traitlets \
        sqlalchemy \
        dask

Besides the above dependencies, you'll also need a Go_ compiler. You can
install Go using your system package manager or the `Go website`_.


(Optional) Install Dependencies via Conda Environment File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can easily install all dependencies needed for local development with the ``dev-environment.yaml``
environment file:

.. code-block:: shell

    $ conda env create --file dev-environment.yaml


Build and Install Dask-Gateway
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask-Gateway is composed of two packages, both contained in the same
repository:

- ``dask-gateway-server``: the gateway server, located in the
  ``dask-gateway-server`` subdirectory.
- ``dask-gateway``: the gateway client, located in the ``dask-gateway``
  subdirectory.

The install directions below are written assuming you're in the top directory
of the repository.

**Building dask-gateway-server**

.. code-block:: shell

    # Build and install dask-gateway-server as an editable package
    $ pip install -e ./dask-gateway-server

    # or, build and install as a regular package
    $ pip install ./dask-gateway-server

**Building dask-gateway**

.. code-block:: shell

    # Build and install dask-gateway as an editable package
    $ pip install -e ./dask-gateway

    # or, build and install as a regular package
    $ pip install ./dask-gateway


Running the Tests
-----------------

The tests are located in the ``tests`` subdirectory, and test both packages. To
run the tests you also need to install ``pytest``:

.. code-block:: shell

    # Install pytest with conda
    $ conda install -c conda-forge pytest

    # Or install with pip
    $ pip install pytest


The tests can then be run as:

.. code-block:: shell

    # Run the test suite
    $ pytest -v tests


In addition to the main tests, additional tests for the various backends are
run in docker (or in ``minikube`` for kubernetes). The scripts for setting up
these test environments are located in the ``continuous_integration``
subdirectory:

- Hadoop Tests: ``continuous_integration/docker/hadoop``
- PBS Tests: ``continuous_integration/docker/pbs``
- Slurm Tests: ``continuous_integration/docker/slurm``
- Kubernetes Tests: ``continuous_integration/kubernetes``

The particularities of each setup differ, please see the
``.github/workflows/test.yaml`` file for the specifics.


Building the Documentation
--------------------------

Dask-Gateway uses Sphinx_ for documentation. The source files are located in
``dask-gateway/docs/source``. To build the documentation locally, first install
the documentation build requirements. Note that ``autodoc-traits`` is used to
inspect the Python source code, which makes us also need the packages listed
after ``autodoc-traits`` to generate the full configuration reference.

.. code-block:: shell

    # Install docs dependencies with conda
    $ conda install -c conda-forge sphinx dask-sphinx-theme sphinx-autobuild autodoc-traits kubernetes_asyncio skein sqlalchemy

    # Or install with pip
    $ pip install sphinx dask-sphinx-theme sphinx-autobuild autodoc-traits kubernetes_asyncio skein sqlalchemy

Then build the documentation with ``make``

.. code-block:: shell

    # Running from the dask-gateway/docs folder
    $ make html

The resulting HTML files end up in the ``_build/html`` directory.

Then consider using another ``make`` command to rebuild the documentation and
refresh a browser viewing the built documentation.

.. code-block:: shell

    # Running from the dask-gateway/docs folder
    $ make devenv


.. _Conda: https://conda.io/docs/
.. _Go:
.. _Go Website: https://go.dev/
.. _Sphinx: http://www.sphinx-doc.org/
