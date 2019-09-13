Install on a Kubernetes Cluster
===============================

Here we provide instructions for installing and configuring
``dask-gateway-server`` on a `Kubernetes Cluster`_.

.. contents:: :local:


Create a Kubernetes Cluster (optional)
--------------------------------------

If you don't already have a cluster running, you'll want to create one. There
are plenty of guides online for how to do this. We recommend following the
excellent `documentation provided by zero-to-jupyterhub-k8s
<create-k8s-cluster>`_.


Install Helm (optional)
-----------------------

If you don't already have Helm_ installed, you'll need to install it locally,
and ensure ``tiller`` is running on your cluster. As with above, there are
plenty of guides online for doing this. We recommend following the `guide
provided by zero-to-jupyterhub-k8s <install-helm>`_.


Install Dask-Gateway
--------------------

At this point you should have a Kubernetes cluster with Helm installed and
configured. You are now ready to install Dask-Gateway on your cluster.


Download the Helm Chart
~~~~~~~~~~~~~~~~~~~~~~~

Currently you need to download the chart locally before installing (this will
change in the future). You can get the chart from the `dask-gateway github
repo`_, available in the `resources/helm subdirectory`_.

.. code-block:: shell

    $ git clone https://github.com/jcrist/dask-gateway.git
    $ cd resources/helm


Configuration
~~~~~~~~~~~~~

The Helm chart provides access to configure most aspects of the
``dask-gateway-server``. These are provided via a configuration YAML_ file (the
name of this file doesn't matter, we'll use ``config.yaml``).

At a minimum, you'll need to set a value for ``gateway.proxyToken``. This is a
random hex string representing 32 bytes, used as a security token between the
gateway and its proxies. You can generate this using ``openssl``:

.. code-block:: shell

    $ openssl rand -hex 32

Write the following into a new file ``config.yaml``, replacing ``<RANDOM
TOKEN>`` with the output of the previous command above.

.. code-block:: yaml

    gateway:
      proxyToken: "<RANDOM TOKEN>"

There Helm chart exposes many more configuration values, see the `default
values.yaml file`_ for more information.


Install the Helm Chart
~~~~~~~~~~~~~~~~~~~~~~

To install the Dask-Gateway Helm chart, run the following command from within
the ``resources/helm`` directory:

.. code-block:: shell

    RELEASE=dask-gateway
    NAMESPACE=dask-gateway

    helm install \
        --name $RELEASE \
        --namespace $NAMESPACE \
        --values path/to/your/config.yaml \
        ./dask-gateway

where:

- ``path/to/your/config.yaml`` is the path to your ``config.yaml`` file created
  above.
- ``RELEASE`` is the `Helm release name`_ to use (we suggest ``dask-gateway``,
  but any release name is fine).
- ``NAMESPACE`` is the `Kubernetes namespace`_ to install the gateway into (we
  suggest ``dask-gateway``, but any namespace is fine).

Running this command may take some time, as resources are created and images
are downloaded. When everything's ready, running the following command will
show ``EXTERNAL-IP`` addresses for all ``LoadBalancer`` services (highlighted
below).

.. code-block:: shell
    :emphasize-lines: 4,6

    $ kubectl get service --namespace dask-gateway
    NAME                            TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)          AGE
    scheduler-api-dask-gateway      ClusterIP      10.51.245.233   <none>           8001/TCP         6m54s
    scheduler-public-dask-gateway   LoadBalancer   10.51.253.105   35.202.68.87     8786:31172/TCP   6m54s
    web-api-dask-gateway            ClusterIP      10.51.250.11    <none>           8001/TCP         6m54s
    web-public-dask-gateway         LoadBalancer   10.51.247.160   146.148.58.187   80:30304/TCP     6m54s

At this point, you have a fully running ``dask-gateway-server``.


Connecting to the gateway
-------------------------

To connect to the running ``dask-gateway-server``, you'll need the external
IP's from both the ``web-public-*`` and ``scheduler-public-*`` services above.
The ``web-public-*`` service provides access to API requests, and also proxies
out the `Dask Dashboards`_. The ``scheduler-public-*`` service proxies TCP
traffic between Dask clients and schedulers.

To connect, create a :class:`dask_gateway.Gateway` object, specifying the both
addresses (the ``scheduler-proxy-*`` IP/port goes under ``proxy_address``).
Using the same values as above:

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway(
    ...     "http://146.148.58.187",
    ...     proxy_address="tls://35.202.68.87:8786"
    ... )

You should now be able to make API calls. Try
:meth:`dask_gateway.Gateway.list_clusters`, this should return an empty list.

.. code-block:: python

    >>> gateway.list_clusters()
    []


Shutting everything down
------------------------

When you're done with the gateway, you'll want to delete your deployment and
clean everything up. You can do this with ``helm delete``:

.. code-block:: shell

    $ helm delete --purge $RELEASE


Additional Configuration
------------------------

Here we provide a few configuration snippets for common deployment scenarios.
For all available configuration values, see the `default values.yaml file`_.


Authenticating with JupyterHub
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

JupyterHub_ provides a multi-user interactive notebook_ environment. Through
the zero-to-jupyterhub-k8s_ project, many companies and institutions have setup
JuypterHub to run on Kubernetes. When deploying Dask-Gateway alongside
JupyterHub, you can configure Dask-Gateway to use JupyterHub for
authentication. To do this, we register ``dask-gateway`` as a `JupyterHub
Service`_.

First we need to generate an API Token - this is commonly done using
``openssl``:

.. code-block:: shell

    $ openssl rand -hex 32

Then add the following lines to your ``config.yaml`` file:

.. code-block:: yaml

    auth:
      type: jupyterhub
      jupyterhub:
        apiToken: "<API TOKEN>"

replacing ``<API TOKEN>`` with the output from above.

You'll also need to add the following to the ``config.yaml`` file for your
JupyterHub Helm Chart.

.. code-block:: yaml

    hub:
      services:
        dask-gateway:
          apiToken: "<API TOKEN>"

again, replacing ``<API TOKEN>`` with the output from above.

With this configuration, JupyterHub will be used to authenticate requests
between users and the ``dask-gateway-server``. Note that users will need to add
``auth="jupyterhub"`` when they create a Gateway :class:`dask_gateway.Gateway`
object.

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway(
    ...     "http://146.148.58.187",
    ...     proxy_address="tls://35.202.68.87:8786",
    ...     auth="jupyterhub",
    ... )


.. _Kubernetes Cluster: https://kubernetes.io/
.. _Helm: https://helm.sh/
.. _create-k8s-cluster: https://zero-to-jupyterhub.readthedocs.io/en/latest/create-k8s-cluster.html
.. _zero-to-jupyterhub-k8s: https://zero-to-jupyterhub.readthedocs.io/en/latest/
.. _install-helm: https://zero-to-jupyterhub.readthedocs.io/en/latest/setup-helm.html
.. _dask-gateway github repo: https://github.com/jcrist/dask-gateway/
.. _resources/helm subdirectory: https://github.com/jcrist/dask-gateway/tree/master/resources/helm
.. _default values.yaml file: https://github.com/jcrist/dask-gateway/blob/master/resources/helm/dask-gateway/values.yaml
.. _Helm release name: https://docs.helm.sh/glossary/#release
.. _Kubernetes namespace: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
.. _Dask Dashboards: https://docs.dask.org/en/latest/diagnostics-distributed.html
.. _yaml: https://en.wikipedia.org/wiki/YAML
.. _JupyterHub: https://jupyterhub.readthedocs.io/
.. _notebook: https://jupyter.org/
.. _JupyterHub Service: https://jupyterhub.readthedocs.io/en/stable/getting-started/services-basics.html
