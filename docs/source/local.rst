Local Processes
===============

Dask-gateway can run as a local process on a single machine.  This is *not advised* for production but can be useful for development.  To begin write the following to a file, `local-config.py`

.. code-block:: python

    # Configure the 3 main addresses
    c.DaskGateway.public_url = "http://127.0.0.1:8787"
    c.DaskGateway.gateway_url = "tls://127.0.0.1:8786"
    # c.DaskGateway.private_url = "http://127.0.0.1:8081"
    # Use the dummy authenticator for easy login. This should never be used in production
    c.DaskGsateway.authenticator_class = "dask_gateway_server.auth.DummyAuthenticator"
    # Configure the gateway to use YARN as the cluster manager
    c.DaskGateway.cluster_manager_class = ( "dask_gateway_server.managers.local.UnsafeLocalClusterManager" )


With `dask-gateway` and `dask-gateway-server` :doc:`installed <install>`, we can now start the Server and Client

Server
------

Start the `dask-gateway-server`::

    $ dask-gateway-server -f ./local-config.py
    [I 2019-08-19 15:56:56.698 DaskGateway] Generating new cookie secret
    [I 2019-08-19 15:56:56.701 DaskGateway] Generating new auth token for scheduler proxy
    [I 2019-08-19 15:56:56.701 DaskGateway] Starting the Dask gateway scheduler proxy...
    [I 2019-08-19 15:56:57.035 DaskGateway] Dask gateway scheduler proxy running at 'tls://127.0.0.1:8786', api at 'http://127.0.0.1:61435'
    [I 2019-08-19 15:56:57.047 DaskGateway] Generating new auth token for web proxy
    [I 2019-08-19 15:56:57.047 DaskGateway] Starting the Dask gateway web proxy...
    [I 2019-08-19 15:56:57.051 DaskGateway] Dask gateway web proxy running at 'http://127.0.0.1:8787', api at 'http://127.0.0.1:61440'
    [I 2019-08-19 15:56:57.270 DaskGateway] Gateway API listening on http://127.0.0.1:8081


Client
------

With the server up and running, we can now connect to the server's web proxy address

.. code-block:: python

    >>> import dask_gateway
    >>> gateway = dask_gateway.Gateway('http://127.0.0.1:8787') # webproxy
    >>> gateway.list_clusters()
    []

In the above, we have established a connection to the gateway.  Next, we can create a new dask cluster, create workers, and begin computing.

.. code-block:: python

    >>> cluster = gateway.new_cluster()
    >>> cluster
    GatewayCluster<9e9907ba635d4b02a7d14699546d3771>
    >>> client = cluster.get_client() # create dask client
    >>> client
    <Client: scheduler='tls://10.20.22.25:61543' processes=0 cores=0>
    >>> cluster.scale(2)
    >>> client
    <Client: scheduler='tls://10.20.22.25:61543' processes=2 cores=2>
    >>> client.submit(...)
