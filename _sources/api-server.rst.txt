Configuration Reference
=======================

Gateway
-------

.. autoconfigurable:: dask_gateway_server.app.DaskGateway


Authentication
--------------

.. _kerberos-auth-config:

KerberosAuthenticator
^^^^^^^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.KerberosAuthenticator


.. _jupyterhub-auth-config:

JupyterHubAuthenticator
^^^^^^^^^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.JupyterHubAuthenticator


.. _dummy-auth-config:

DummyAuthenticator
^^^^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.DummyAuthenticator


.. _cluster-managers-reference:

Cluster Managers
----------------

Base Class
^^^^^^^^^^

ClusterManager
~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.base.ClusterManager


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


.. _jobqueue-config:

Job Queues
^^^^^^^^^^

PBSClusterManager
~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.jobqueue.pbs.PBSClusterManager

SlurmClusterManager
~~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.managers.jobqueue.slurm.SlurmClusterManager


Proxies
-------

Web Proxy
^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.proxy.WebProxy

Scheduler Proxy
^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.proxy.SchedulerProxy


User Limits
-----------

.. autoconfigurable:: dask_gateway_server.limits.UserLimits


Cluster Manager Options
-----------------------

.. autoclass:: dask_gateway_server.options.Options

.. autoclass:: dask_gateway_server.options.Integer

.. autoclass:: dask_gateway_server.options.Float

.. autoclass:: dask_gateway_server.options.String

.. autoclass:: dask_gateway_server.options.Bool

.. autoclass:: dask_gateway_server.options.Select
