Install on a Kubernetes Cluster
===============================

Here we provide instructions for installing and configuring
``dask-gateway-server`` on a `Kubernetes Cluster`_.

Architecture
------------

When running on Kubernetes, Dask Gateway is composed of the following components:

- Multiple active **Dask Clusters** (potentially more than one per user)
- A **Traefik Proxy** for proxying both the connection between the user's client
  and their respective scheduler, and the Dask Web UI for each cluster
- A **Gateway API Server** that handles user API requests
- A **Gateway Controller** for managing the kubernetes objects used by each
  cluster (e.g. pods, secrets, etc...).

.. image:: /_images/architecture-k8s.svg
    :width: 90 %
    :align: center
    :alt: Dask-Gateway high-level kubernetes architecture

Both the Traefik Proxy deployment and the Gateway API Server deployment can be
scaled to multiple replicas, for increased availability and scalability.

Create a Kubernetes Cluster (optional)
--------------------------------------

If you don't already have a cluster running, you'll want to create one. There
are plenty of guides online for how to do this. We recommend following the
excellent `documentation provided by zero-to-jupyterhub-k8s`_.


Install Helm (optional)
-----------------------

If you don't already have Helm_ installed, you'll need to install it locally.
As with above, there are plenty of instructional materials online for doing
this. We recommend following the `guide provided by zero-to-jupyterhub-k8s`_.


Install Dask-Gateway
--------------------

At this point you should have a Kubernetes cluster with Helm installed and
configured. You are now ready to install Dask-Gateway on your cluster.


Add the Helm Chart Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To avoid downloading the chart locally from GitHub, you can use the
`Dask-Gateway Helm chart repository`_.

.. code-block:: shell

    $ helm repo add daskgateway https://dask.org/dask-gateway-helm-repo/
    $ helm repo update


Configuration
~~~~~~~~~~~~~

The Helm chart provides access to configure most aspects of the
``dask-gateway-server``. These are provided via a configuration YAML_ file (the
name of this file doesn't matter, we'll use ``config.yaml``).

The Helm chart exposes many configuration values, see the `default
values.yaml file`_ for more information.


Install the Helm Chart
~~~~~~~~~~~~~~~~~~~~~~

To install the Dask-Gateway Helm chart, run the following command:

.. code-block:: shell

    RELEASE=dask-gateway
    NAMESPACE=dask-gateway
    VERSION=0.9.0

    helm upgrade --install \
        --namespace $NAMESPACE \
        --version $VERSION \
        --values path/to/your/config.yaml \
        $RELEASE \
        daskgateway/dask-gateway

where:

- ``RELEASE`` is the `Helm release name`_ to use (we suggest ``dask-gateway``,
  but any release name is fine).
- ``NAMESPACE`` is the `Kubernetes namespace`_ to install the gateway into (we
  suggest ``dask-gateway``, but any namespace is fine).
- ``VERSION`` is the Helm chart version to use. To use the latest published
  version you can omit the ``--version`` flag entirely. See the `Helm chart
  repository`_ for an index of all available versions.
- ``path/to/your/config.yaml`` is the path to your ``config.yaml`` file created
  above.

Running this command may take some time, as resources are created and images
are downloaded. When everything is ready, running the following command will
show the ``EXTERNAL-IP`` addresses for the ``LoadBalancer`` service (highlighted
below).

.. code-block:: shell
    :emphasize-lines: 4

    $ kubectl get service --namespace dask-gateway
    NAME                              TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)          AGE
    api-<RELEASE>-dask-gateway        ClusterIP      10.51.245.233   <none>           8000/TCP         6m54s
    traefik-<RELEASE>-dask-gateway    LoadBalancer   10.51.247.160   146.148.58.187   80:30304/TCP     6m54s

You can also check to make sure the `daskcluster` CRD has been installed successfully:

.. code-block:: shell

   $ kubectl get daskcluster -o yaml
   apiVersion: v1
   items: []
   kind: List
   metadata:
     resourceVersion: ""
     selfLink: ""

At this point, you have a fully running ``dask-gateway-server``.


Connecting to the gateway
-------------------------

To connect to the running ``dask-gateway-server``, you'll need the external IPs
from the ``traefik-*`` services above. The Traefik service provides access to
API requests, proxies out the `Dask Dashboards`_, and proxies TCP traffic
between Dask clients and schedulers. (You can also choose to have Traefik handle
scheduler traffic over a separate port, see the :ref:`helm-chart-reference`).

To connect, create a :class:`dask_gateway.Gateway` object, specifying the both
addresses (the second ``traefik-*`` port goes under ``proxy_address`` if using
separate ports). Using the same values as above:

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway(
    ...     "http://146.148.58.187",
    ... )

You should now be able to use the gateway client to make API calls. To verify
this, call :meth:`dask_gateway.Gateway.list_clusters`. This should return an
empty list as you have no clusters running yet.

.. code-block:: python

    >>> gateway.list_clusters()
    []


Shutting everything down
------------------------

When you're done with the gateway, you'll want to delete your deployment and
clean everything up. You can do this with ``helm delete``:

.. code-block:: shell

    $ helm delete --purge $RELEASE


Additional configuration
------------------------

Here we provide a few configuration snippets for common deployment scenarios.
For all available configuration fields see the :ref:`helm-chart-reference`.


Using a custom image
~~~~~~~~~~~~~~~~~~~~

By default schedulers/workers started by dask-gateway will use the
``daskgateway/dask-gateway`` image. This is a basic image with only the minimal
dependencies installed. To use a custom image, you can configure:

- ``gateway.backend.image.name``: the default image name
- ``gateway.backend.image.tag``: the default image tag

For an image to work with dask-gateway, it must have a compatible version of
``dask-gateway`` installed (we recommend always using the same version as
deployed on the ``dask-gateway-server``).

Additionally, we recommend using an `init process`_ in your images. This isn't
strictly required, but running without an init process may lead to odd worker
behaviors. We recommend using tini_, but any init process should be fine.

There are no other requirements for images, any image that meets the above
should work fine. You may install any additional libraries or dependencies you
require.

To develop your own image, you may either base it on a compatible version of
``daskgateway/dask-gateway``, or use our `example dockerfile`_ as a reference
and develop your own.

Using ``extraPodConfig``/``extraContainerConfig``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `Kubernetes API`_ is large, and not all configuration fields you may want
to set on scheduler/worker pods are directly exposed by the Helm chart. To
address this, we provide a few fields for forwarding configuration directly to
the underlying kubernetes objects:

- ``gateway.backend.scheduler.extraPodConfig``
- ``gateway.backend.scheduler.extraContainerConfig``
- ``gateway.backend.worker.extraPodConfig``
- ``gateway.backend.worker.extraContainerConfig``

These allow configuring any unexposed fields on the pod/container for
schedulers and workers respectively. Each takes a mapping of key-value pairs,
which is deep-merged with any settings set by dask-gateway itself (with
preference given to the ``extra*Config`` values). Note that keys should be
``camelCase`` (rather than ``snake_case``) to match those in the kubernetes
API.

For example, this can be useful for setting things like tolerations_ or `node
affinities`_ on scheduler or worker pods. Here we configure a node
anti-affinity for scheduler pods to avoid `preemptible nodes`_:

.. code-block:: yaml

  gateway:
    backend:
      scheduler:
        extraPodConfig:
          affinity:
            nodeAffinity:
              requiredDuringSchedulingIgnoredDuringExecution:
                nodeSelectorTerms:
                  - matchExpressions:
                    - key: cloud.google.com/gke-preemptible
                      operator: DoesNotExist

For information on allowed fields, see the Kubernetes documentation:

- `PodSpec Configuration <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#podspec-v1-core>`__
- `Container Configuration <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__


Using ``extraConfig``
~~~~~~~~~~~~~~~~~~~~~

Not all configuration options have been exposed via the helm chart. To set
unexposed options, you can use the ``gateway.extraConfig`` field. This takes
either:

- A single python code-block (as a string) to append to the end of the
  generated ``dask_gateway_config.py`` file.
- A map of keys -> code-blocks (recommended). When applied in this form,
  code-blocks are appended in alphabetical order by key (the keys themselves
  are meaningless).  This allows merging multiple ``values.yaml`` files
  together, as Helm can natively merge maps.

For example, here we use ``gateway.extraConfig`` to set
:data:`c.Backend.cluster_options`, exposing options for worker
resources and image (see :doc:`cluster-options` for more information).

.. code-block:: yaml

    gateway:
      extraConfig:
        # Note that the key name here doesn't matter. Values in the
        # `extraConfig` map are concatenated, sorted by key name.
        clusteroptions: |
            from dask_gateway_server.options import Options, Integer, Float, String

            def option_handler(options):
                return {
                    "worker_cores": options.worker_cores,
                    "worker_memory": "%fG" % options.worker_memory,
                    "image": options.image,
                }

            c.Backend.cluster_options = Options(
                Integer("worker_cores", 2, min=1, max=4, label="Worker Cores"),
                Float("worker_memory", 4, min=1, max=8, label="Worker Memory (GiB)"),
                String("image", default="daskgateway/dask-gateway:latest", label="Image"),
                handler=option_handler,
            )

For information on all available configuration options, see the
:doc:`api-server` (in particular, the :ref:`kube-cluster-config`
section).


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

    gateway:
      auth:
        type: jupyterhub
        jupyterhub:
          apiToken: "<API TOKEN>"

replacing ``<API TOKEN>`` with the output from above.

If you're not deploying Dask-Gateway in the same cluster and namespace as
JupyterHub, you'll also need to specify JupyterHub's API url. This is usually
of the form ``https://<JUPYTERHUB-HOST>:<JUPYTERHUB-PORT>/hub/api``. If
JupyterHub and Dask-Gateway are on the same cluster and namespace you can omit
this configuration key, the address will be inferred automatically.

.. code-block:: yaml

    gateway:
      auth:
        type: jupyterhub
        jupyterhub:
          apiToken: "<API TOKEN>"
          apiUrl: "<API URL>"

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
    ...     auth="jupyterhub",
    ... )


.. _helm-chart-reference:

Helm chart reference
--------------------

The full `default values.yaml file`_ for the dask-gateway Helm chart is included
here for reference:

.. literalinclude:: ../../resources/helm/dask-gateway/values.yaml
    :language: yaml


.. _Kubernetes Cluster: https://kubernetes.io/
.. _Helm: https://helm.sh/
.. _documentation provided by zero-to-jupyterhub-k8s: https://zero-to-jupyterhub.readthedocs.io/en/latest/create-k8s-cluster.html
.. _zero-to-jupyterhub-k8s: https://zero-to-jupyterhub.readthedocs.io/en/latest/
.. _guide provided by zero-to-jupyterhub-k8s: https://zero-to-jupyterhub.readthedocs.io/en/stable/kubernetes/setup-helm.html
.. _Helm chart repository:
.. _dask-gateway helm chart repository: https://dask.org/dask-gateway-helm-repo/
.. _dask-gateway github repo: https://github.com/dask/dask-gateway/
.. _resources/helm subdirectory: https://github.com/dask/dask-gateway/tree/main/resources/helm
.. _default values.yaml file: https://github.com/dask/dask-gateway/blob/main/resources/helm/dask-gateway/values.yaml
.. _Helm release name: https://docs.helm.sh/glossary/#release
.. _Kubernetes namespace: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
.. _Dask Dashboards: https://docs.dask.org/en/latest/diagnostics-distributed.html
.. _yaml: https://en.wikipedia.org/wiki/YAML
.. _JupyterHub: https://jupyterhub.readthedocs.io/
.. _notebook: https://jupyter.org/
.. _JupyterHub Service: https://jupyterhub.readthedocs.io/en/stable/getting-started/services-basics.html
.. _Kubernetes API: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/
.. _tolerations: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/
.. _node affinities: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
.. _preemptible nodes: https://cloud.google.com/blog/products/containers-kubernetes/cutting-costs-with-google-kubernetes-engine-using-the-cluster-autoscaler-and-preemptible-vms
.. _init process: https://en.wikipedia.org/wiki/Init
.. _tini: https://github.com/krallin/tini
.. _example dockerfile: https://github.com/dask/dask-gateway/blob/main/resources/helm/example-images/Dockerfile
