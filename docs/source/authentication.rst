Authentication
==============

Being a multi-tenant application, ``dask-gateway-server`` needs a method to
authenticate users. Like backends, authentication is also configurable by
setting :data:`c.DaskGateway.authenticator_class`. Here we document a few
common implementations.


Simple authentication for testing
---------------------------------

For testing purposes, a "simple" authentication method can be used. This
method implements authentication with no validation of the password
field. As such, *it is not suitable for production, only for testing*. If
desired, an optional password can be configured to be shared across all users.
Note that this only marginally makes things more secure, as users can still
easily impersonate each other.

This protocol is the default, and is implemented by
:class:`dask_gateway_server.auth.SimpleAuthenticator`. No further configuration
is needed to enable it. If you would like to add a shared password, you can do
so by adding the following line to your ``dask_gateway_config.py`` file:

.. code-block:: python

    # Set a shared password.
    c.SimpleAuthenticator.password = "mypassword"

For more information see the :ref:`simple-auth-config` docs.


Kerberos authentication
-----------------------

Kerberos_ can be used to authenticate users by enabling the
:class:`dask_gateway_server.auth.KerberosAuthenticator`. To do this, you'll
need to create a ``HTTP`` service principal and keytab for the host running
``dask-gateway-server`` (if one doesn't already exist). Keytabs can be created
on the command-line as:

.. code-block:: shell

    # Create the HTTP principal (if not already created)
    $ kadmin -q "addprinc -randkey HTTP/FQDN"

    # Create a keytab
    $ kadmin -q "xst -norandkey -k /etc/dask-gateway/http.keytab HTTP/FQDN"

where ``FQDN`` is the `fully qualified domain name`_ of the host running
``dask-gateway-server``.

Store the keytab file wherever you see fit. We recommend storing it along with
the other configuration (usually in ``/etc/dask-gateway/``). You'll also want
to make sure that ``http.keytab`` is only readable by admin users (usually just
the ``dask`` user running ``dask-gateway-server``).

.. code-block:: shell

    $ chown dask /etc/dask-gateway/http.keytab
    $ chmod 400 /etc/dask-gateway/http.keytab

To configure ``dask-gateway-server`` to use this keytab file, you'll need to
add the following line to your ``dask_gateway_config.py``:

.. code-block:: python

    # Enable Kerberos for user-facing authentication
    c.DaskGateway.authenticator_class = "dask_gateway_server.auth.KerberosAuthenticator"

    # The location of the HTTP keytab
    c.KerberosAuthenticator.keytab = "/etc/dask-gateway/http.keytab"

For more information see the :ref:`kerberos-auth-config` docs.


Using JupyterHub's authentication
---------------------------------

JupyterHub_ provides a multi-user interactive notebook_ environment.  When
deploying Dask-Gateway alongside JupyterHub, you can configure Dask-Gateway to
use JupyterHub for authentication. To do this, we register ``dask-gateway`` as
a `JupyterHub Service`_.

First we need to generate an API Token - this is commonly done using
``openssl``:

.. code-block:: shell

    $ openssl rand -hex 32

Then add the following lines to your ``dask_gateway_config.py`` file:

.. code-block:: python

    c.DaskGateway.authenticator_class = "dask_gateway_server.auth.JupyterHubAuthenticator"
    c.JupyterHubAuthenticator.api_token = "<API TOKEN>"
    c.JupyterHubAuthenticator.api_url = "<API URL>"

Where:

- ``<API TOKEN>`` is the token generated above
- ``<API URL>`` is JupyterHub's API url. This is usually of the form
  ``https://<JUPYTERHUB-HOST>:<JUPYTERHUB-PORT>/hub/api``.

You'll also need to register the API token with JupyterHub. This can be done by
adding the following to the corresponding ``jupyterhub_config.py`` file:

.. code-block:: python

    c.JupyterHub.services = [
        {"name": "dask-gateway", "api_token": "<API TOKEN>"}
    ]

again, replacing ``<API TOKEN>`` with the output from above.

With this configuration, JupyterHub will be used to authenticate requests
between users and the ``dask-gateway-server``.

For more information see the :ref:`jupyterhub-auth-config` docs.


.. _Basic: https://en.wikipedia.org/wiki/Basic_access_authentication
.. _Kerberos: https://web.mit.edu/kerberos/
.. _fully qualified domain name: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
.. _JupyterHub: https://jupyterhub.readthedocs.io/
.. _notebook: https://jupyter.org/
.. _JupyterHub Service: https://jupyterhub.readthedocs.io/en/stable/getting-started/services-basics.html
