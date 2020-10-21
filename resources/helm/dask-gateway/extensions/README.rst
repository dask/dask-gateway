Extensions
==========

Some Dask Gateway deployments will require non-trivial configuration (e.g. a
new ``Authenticator`` class). You have a few options to add such "extensions":

1. Add all extension code in the ``gateway.extraConfig`` of your Helm values
   file. For simple extensions this is the recommended approach.
2. Package your code as part of a custom image, and configure the Dask Gateway
   api server to use that image via ``gateway.image``. Recommended if your
   extension is large enough (Helm charts have a size limit of 1 MiB) or
   requires additional dependencies.
3. Clone the helm chart locally, and make use of the ``extensions`` directory.
   This approach prevents using the published Helm chart, but may be useful in
   some cases.

To use the ``extensions`` directory, clone the Helm chart locally, and copy
whatever extra files you require into the ``extensions/gateway`` directory.
All files in ``extensions/gateway`` will be copied into ``/etc/dask-gateway``
in the deployed Dask Gateway API server pods. This directory is added to
``PYTHONPATH``, so any Python code will be importable. You can then import what
functionality you need in a smaller section in ``gateway.extraConfig`` to
configure the Dask Gateway server as needed.

Example
-------

For example, say ``myauthenticator.py`` contains a custom ``Authenticator``
class:

.. code-block:: python

   from dask_gateway_server.auth import Authenticator

   class MyAuthenticator(Authenticator):
       """My custom authenticator"""
       ...

After adding ``myauthenticator.py`` to ``extensions/gateway``, you can
configure the Dask Gateway API server to use your authenticator via the proper
fields in ``values.yaml``. For an authenticator, you can make use of
``gateway.auth``:

.. code-block:: yaml

   gateway:
     auth:
       type: custom
       custom:
         class: myauthenticator.MyAuthenticator

For other types of extensions (say ``c.KubeBackend.cluster_options``) you'd
need to import and configure things in ``gateway.extraConfig``:

.. code-block:: yaml

   gateway:
     extraConfig:
       my-extension: |
         # import your extension and configure appropriately
         from myextension import my_cluster_options
         c.KubeBackend.cluster_options = my_cluster_options
