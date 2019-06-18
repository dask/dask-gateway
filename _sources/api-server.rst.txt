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

LocalClusterManager
~~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.local.LocalClusterManager

UnsafeLocalClusterManager
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.local.UnsafeLocalClusterManager


YARN
^^^^

YarnClusterManager
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.yarn.YarnClusterManager


Kubernetes
^^^^^^^^^^

KubeClusterManager
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.kubernetes.KubeClusterManager


Job Queues
^^^^^^^^^^

PBSClusterManager
~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.jobqueue.pbs.PBSClusterManager

SlurmClusterManager
~~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.jobqueue.slurm.SlurmClusterManager
