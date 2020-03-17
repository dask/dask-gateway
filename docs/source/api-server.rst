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


.. _simple-auth-config:

SimpleAuthenticator
^^^^^^^^^^^^^^^^^^^

.. autoconfigurable:: dask_gateway_server.auth.SimpleAuthenticator


.. _cluster-backends-reference:

Cluster Backends
----------------

Base Class
^^^^^^^^^^

Backend
~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.Backend


Local Processes
^^^^^^^^^^^^^^^

LocalBackend
~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.local.LocalBackend

UnsafeLocalBackend
~~~~~~~~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.local.UnsafeLocalBackend


YARN
^^^^

.. _yarn-config:

YarnBackend
~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.yarn.YarnBackend


Kubernetes
^^^^^^^^^^

.. _kube-backend-config:

KubeBackend
~~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.kubernetes.KubeBackend


.. _jobqueue-config:

Job Queues
^^^^^^^^^^

PBSBackend
~~~~~~~~~~

.. autoconfigurable:: dask_gateway_server.backends.jobqueue.pbs.PBSBackend

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
