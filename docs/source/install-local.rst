Install Locally (Quickstart)
============================

This page describes how to deploy and interact with a ``dask-gateway-server``
locally. This can be useful for testing, demos, and development purposes, but
is not a normal method of deployment.

If you're a user connecting to an existing ``dask-gateway-server`` instance,
you may want to start at :doc:`usage` instead.

.. currentmodule:: dask_gateway

Installation
------------

Dask-Gateway can be installed with ``conda`` or ``pip``. It's composed of two packages:

- ``dask-gateway-server``: the gateway server. Administrators usually install this once on a cluster.
- ``dask-gateway``: the client library. Users only need this library to use a running Gateway.

**Install with conda**

.. code-block:: console

    $ conda install -c conda-forge dask-gateway dask-gateway-server-local

**Install with pip**

.. code-block:: console

    $ pip install dask-gateway dask-gateway-server[local]


Start the gateway server
------------------------

To start the Gateway server, run:

.. code-block:: console

    $ dask-gateway-server


This starts ``dask-gateway`` locally with the default configuration. This uses:

- ``UnsafeLocalBackend`` to manage local clusters without any process isolation
- ``SimpleAuthenticator`` to authenticate users using a simple and insecure authentication scheme

*Both of these options are insecure and not-advised for any real-world
deployments.* They are perfectly fine for testing and experimentation though.


Connect to the gateway server
-----------------------------

To connect to the gateway, create a :class:`Gateway` client with the URL output
above. By default this is ``http://127.0.0.1:8000``.

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway("http://127.0.0.1:8000")
    >>> gateway
    Gateway<http://127.0.0.1:8000>

To check that everything is setup properly, query the gateway server to see any
existing clusters (should be an empty list).

.. code-block:: python

    >>> gateway.list_clusters()
    []


Interact with the gateway server
--------------------------------

At this point you can use the :class:`Gateway` client to interact with the
gateway server. You can use the client to create new clusters and interact with
existing clusters. We direct you to the :doc:`usage` documentation for more
information, starting from the :ref:`usage-create-new-cluster` section.


Shutdown the gateway server
---------------------------

When you're done with local usage, you'll want to shutdown the Dask-Gateway
server. To do this, ``Ctrl-C`` in the same terminal you started the process in.
Note that any active clusters will also be shutdown.
