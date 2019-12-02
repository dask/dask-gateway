Security settings
=================

Here we present a few common security fields you'll likely want to configure in
a production deployment.


Enabling TLS
------------

As a web application, any production deployment of Dask-Gateway should be run
with TLS encryption (HTTPS_) enabled. There are a few common options for
enabling this.

Using your own TLS certificate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have your own TLS certificate/key pair, you can specify the file
locations in your ``dask_gateway_config.py`` file. The relevant configuration
fields are:

- :data:`c.DaskGateway.tls_cert`
- :data:`c.DaskGateway.tls_key`

.. code-block:: python

    c.DaskGateway.tls_cert = "/path/to/my.cert"
    c.DaskGateway.tls_key = "/path/to/my.key"

Note that the certificate and key *must* be stored in a secure location where
they are readable only by admin users.

Using letsencrypt
^^^^^^^^^^^^^^^^^

It is also possible to use letsencrypt_ to automatically obtain TLS
certificates. If you have letsencrypt running using the default options, you
can configure this by adding the following to your ``dask_gateway_config.py``
file:

.. code-block:: python

    c.DaskGateway.tls_cert = "/etc/letsencrypt/live/{FQDN}/fullchain.pem"
    c.DaskGateway.tls_key = "/etc/letsencrpyt/live/{FQDN}/privkey.pem"

where ``FQDN`` is the  `fully qualified domain name`_ for your server.

Using external TLS termination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If ``dask-gateway-server`` is running behind a proxy that does TLS termination
(e.g. NGINX_), then no further configuration is needed.


Proxy authentication tokens
---------------------------

To secure communication between the proxies and the gateway server, a secret
token is used for each proxy. By default these tokens are generated
automatically. It's necessary for an admin to configure these explicitly if the
proxies are being externally managed (i.e.
:data:`c.WebProxy.externally_managed`/:data:`c.SchedulerProxy.externally_managed`
are set to true). To do this you have two options:

- Configure :data:`c.WebProxy.auth_token` and
  :data:`c.SchedulerProxy.auth_token` in your ``dask_gateway_config.py`` file.
  Since these fields are secrets, the config file *must* be readable only by
  admin users.
- Set the ``DASK_GATEWAY_PROXY_TOKEN`` environment variable. In this case both
  proxies will share the same token. For security reasons, this environment
  variable should only be visible by the gateway server and proxy processes.

In either case all options take 32 byte random strings, encoded as hex. One way
to create these is through the ``openssl`` CLI:

.. code-block:: shell

    $ openssl rand -hex 32


Cookie secret token
-------------------

Dask-Gateway uses cookies to reduce load on the configured authenticator (see
:doc:`authentication`). The cookie secret token is used to encrypt these
cookies.

Most deployments shouldn't need to configure this token. By default it's
created automatically on startup - if it changes on a restart it will force a
relogin for all users, but for most authenticators this shouldn't cause a
disruption.

If you do want to configure this field, you have two options:

- Configure :data:`c.DaskGateway.cookie_secret` in your
  ``dask_gateway_config.py`` file. Since this is a secret, the config file
  *must* be readable only by admin users.
- Set the ``DASK_GATEWAY_COOKIE_SECRET`` environment variable. For security
  reasons, this environment variable should only be visible by the
  ``dask-gateway-server``.

In either case this takes a 32 byte random string, encoded as hex. One way to
create this is through the ``openssl`` CLI:

.. code-block:: shell

    $ openssl rand -hex 32


.. _HTTPS: https://en.wikipedia.org/wiki/HTTPS
.. _letsencrypt: https://letsencrypt.org/
.. _fully qualified domain name: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
.. _NGINX: https://docs.nginx.com/nginx/admin-guide/security-controls/terminating-ssl-http/
