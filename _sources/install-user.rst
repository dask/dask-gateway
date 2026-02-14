Installation
============

Dask-Gateway is composed of two packages:

- ``dask-gateway``: the client library, installed by *users*.
- ``dask-gateway-server``: the gateway server, installed by *administrators*.

Dask-Gateway *users* only need the ``dask-gateway`` client package to interact
with the server. It can be installed with ``conda`` or ``pip``.

**Install with conda**

.. code-block:: console

    $ conda install -c conda-forge dask-gateway

**Install with pip**

.. code-block:: console

    $ pip install dask-gateway

The version of the client library should match that of ``dask-gateway-server``
running on the server. If you don't know the version running on your server,
contact your administrator.


Kerberos Authentication Dependencies (Optional)
-----------------------------------------------

If your Dask-Gateway server uses Kerberos_ for authentication, you'll also need
to install the kerberos dependencies. This can be done with either ``conda`` or
``pip``:

**Install with conda**

.. code-block:: console

    $ conda install -c conda-forge dask-gateway-kerberos

**Install with pip**

.. code-block:: console

    $ pip install dask-gateway[kerberos]


.. _Kerberos: https://en.wikipedia.org/wiki/Kerberos_(protocol)
