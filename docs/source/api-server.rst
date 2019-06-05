Configuration API
=================

Gateway
-------

.. autoconfigurable:: dask_gateway_server.app.DaskGateway


Proxies
-------

Web Proxy
^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.proxy.WebProxy

Scheduler Proxy
^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.proxy.SchedulerProxy


Authentication
--------------

Kerberos
^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.KerberosAuthenticator

Basic
^^^^^

.. autoconfigurable:: dask_gateway_server.auth.DummyAuthenticator


Cluster Managers
----------------

Local Processes
^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.managers.local.LocalClusterManager

.. autoconfigurable:: dask_gateway_server.managers.local.UnsafeLocalClusterManager


YARN
^^^^

.. autoconfigurable:: dask_gateway_server.managers.yarn.YarnClusterManager
