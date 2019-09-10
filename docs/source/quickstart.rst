Quickstart
==========

For testing, demos, and development purposes, you can install and use
``dask-gateway`` locally.


.. currentmodule:: dask_gateway


Installation
------------

Dask-Gateway can be installed with ``conda`` or ``pip``. It's composed of two packages:

- ``dask-gateway-server``: the gateway server. Administrators usually install this once on a cluster.
- ``dask-gateway``: the client library. Users only need this library to use a running Gateway.

**Install with conda**

.. code-block:: console

    $ conda install -c conda-forge dask-gateway dask-gateway-server

**Install with pip**

.. code-block:: console

    $ pip install dask-gateway dask-gateway-server


Start the gateway server
------------------------

To start the Gateway server, run:

.. code-block:: console

    $ dask-gateway-server


This starts ``dask-gateway`` locally with the default configuration. This uses:

- ``UnsafeLocalClusterManager`` to manage local clusters without any process isolation
- ``DummyAuthenticator`` to authenticate users using a simple and insecure authentication scheme

*Both of these options are insecure and not-advised for any real-world
deployments.* They are perfectly fine for testing and experimentation though.


Connect to the gateway
----------------------

To connect to the gateway, create a :class:`Gateway` client with the URL output
above. By default this is ``http://127.0.0.1:8000``.

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway("http://127.0.0.1:8000")
    >>> gateway
    Gateway<http://127.0.0.1:8000>

To check that everything is setup properly, query the gateway to see any
existing clusters (should be an empty list).

.. code-block:: python

    >>> gateway.list_clusters()
    []


Create a new cluster
--------------------

To create a new cluster, you can use the :meth:`Gateway.new_cluster` method.
This will create a new cluster with no workers.

.. code-block:: python

    >>> cluster = gateway.new_cluster()
    >>> cluster
    GatewayCluster<6c14f41343ea462599f126818a14ebd2>


Scale up a cluster
------------------

To scale a cluster to one or more workers, you can use the
:meth:`GatewayCluster.scale` method. Here we scale our cluster up to two
workers.

.. code-block:: python

    >>> cluster.scale(2)


Connect to the cluster
----------------------

To connect to the cluster so you can start doing work, you can use the
:meth:`GatewayCluster.get_client` method. This returns a
``dask.distributed.Client`` object.

.. code-block:: python

    >>> client = cluster.get_client()
    >>> client
    <Client: scheduler='tls://198.51.100.1:65252' processes=2 cores=2>


Run computations on the cluster
-------------------------------

At this point you should be able to use normal ``dask`` methods to do work. For
example, here we take the mean of a random array.

.. code-block:: python

    >>> import dask.array as da
    >>> a = da.random.normal(size=(1000, 1000), chunks=(500, 500))
    >>> a.mean().compute()
    0.0022336223893512945


Shutdown the cluster
--------------------

When you're done using it, you can shutdown the cluster using the
:meth:`Cluster.shutdown` method. This will cleanly close all dask workers, as
well as the scheduler.

.. code-block:: python

    >>> cluster.shutdown()


Connect to an existing cluster
------------------------------

Alternatively, you can leave a cluster running and reconnect to it later. When
using ``dask-gateway``, Dask clusters aren't tied to the lifetime of your
client process.

To see all running clusters, use the :meth:`Gateway.list_clusters` method.

.. code-block:: python

    >>> clusters = gateway.list_clusters()
    >>> clusters
    [ClusterReport<name=ce498e95403741118a8f418ee242e646, status=RUNNING>]

To connect to an existing cluster, use the :meth:`Gateway.connect` method.

.. code-block:: python

    >>> cluster = gateway.connect(clusters[0].name)
    >>> cluster
    GatewayCluster<ce498e95403741118a8f418ee242e646>


Shutdown the gateway server
---------------------------

To shutdown the Dask-Gateway server, ``Ctrl-C`` in the same terminal you
started the process in. Note that any active clusters will also be shutdown.
