Configuration
=============

.. currentmodule:: dask_gateway

Specifying all parameters to the :class:`Gateway` or :class:`GatewayCluster`
constructors every time may be error prone, especially when sharing this
workflow with new users. To simplify things you can provide defaults in a
configuration file, traditionally held in ``~/.config/dask/gateway.yaml`` or
``/etc/dask/gateway.yaml``.  Note that this configuration is *optional*, and
only changes the defaults when not specified in the constructors. You only need
to set the fields you care about, unset fields will fall back to the `default
configuration`_.

We recommend administrators create a configuration file to share with their
users, specifying the addresses and authentication necessary to connect to
their ``dask-gateway-server``. For example:

**Example:**

.. code-block:: yaml

   # ~/.config/dask/gateway.yaml
   gateway:
     # The full address to the dask-gateway server.
     address: http://146.148.58.187

     # The full address to the dask-gateway scheduler proxy
     proxy-address: tls://35.202.68.87:8786

    auth:
      # Use kerberos for authentication
      type: kerberos


Users can now create :class:`Gateway` or :class:`GatewayCluster` objects
without specifying any additional information.

.. code-block:: python

   from dask_gateway import GatewayCluster

   cluster = GatewayCluster()
   cluster.scale(20)

For more information on Dask configuration see the `Dask configuration
documentation <https://docs.dask.org/en/latest/configuration.html>`_.


Default Configuration
---------------------

The default configuration file is as follows

.. literalinclude:: ../../dask-gateway/dask_gateway/gateway.yaml
    :language: yaml
