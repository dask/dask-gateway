Configuration Reference
=======================

Gateway Server
--------------

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


.. _simple-auth-config:

SimpleAuthenticator
^^^^^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.SimpleAuthenticator


.. _cluster-backends-reference:

Cluster Backends
----------------

Base Class
^^^^^^^^^^

.. _cluster-config:

ClusterConfig
~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.base.ClusterConfig

Backend
~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.base.Backend


Local Processes
^^^^^^^^^^^^^^^

LocalClusterConfig
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.local.LocalClusterConfig

LocalBackend
~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.local.LocalBackend

UnsafeLocalBackend
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.local.UnsafeLocalBackend


YARN
^^^^

.. _yarn-config:

YarnClusterConfig
~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.yarn.YarnClusterConfig

YarnBackend
~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.yarn.YarnBackend


Kubernetes
^^^^^^^^^^

.. _kube-cluster-config:

KubeClusterConfig
~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.kubernetes.KubeClusterConfig

KubeBackend
~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.kubernetes.KubeBackend

KubeController
~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.kubernetes.controller.KubeController


.. _jobqueue-config:

Job Queues
^^^^^^^^^^

PBSClusterConfig
~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.jobqueue.pbs.PBSClusterConfig

PBSBackend
~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.jobqueue.pbs.PBSBackend

SlurmClusterConfig
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.jobqueue.slurm.SlurmClusterConfig

SlurmBackend
~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.jobqueue.slurm.SlurmBackend


Proxy
-----

Proxy
^^^^^

.. autoconfigurable:: dask_gateway_server.proxy.Proxy


Cluster Manager Options
-----------------------

.. autoclass:: dask_gateway_server.options.Options

.. autoclass:: dask_gateway_server.options.Integer

.. autoclass:: dask_gateway_server.options.Float

.. autoclass:: dask_gateway_server.options.String

.. autoclass:: dask_gateway_server.options.Bool

.. autoclass:: dask_gateway_server.options.Select

.. autoclass:: dask_gateway_server.options.Mapping


Models
------

User
^^^^

.. autoclass:: dask_gateway_server.models.User

Cluster
^^^^^^^

.. autoclass:: dask_gateway_server.models.Cluster
