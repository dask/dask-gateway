Configuration API
=================

Gateway
-------

.. autoconfigurable:: dask_gateway_server.app.DaskGateway


Cluster Manager Options
-----------------------

.. autoclass:: dask_gateway_server.options.Options

.. autoclass:: dask_gateway_server.options.Integer

.. autoclass:: dask_gateway_server.options.Float

.. autoclass:: dask_gateway_server.options.String

.. autoclass:: dask_gateway_server.options.Bool

.. autoclass:: dask_gateway_server.options.Select


User Limits
-----------

.. autoconfigurable:: dask_gateway_server.limits.UserLimits


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

.. _yarn-config:

YarnClusterManager
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.yarn.YarnClusterManager


Kubernetes
^^^^^^^^^^

.. _kube-cluster-manager-config:

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
