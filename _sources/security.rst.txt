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

- :data:`c.Proxy.tls_cert`
- :data:`c.Proxy.tls_key`

.. code-block:: python

    c.Proxy.tls_cert = "/path/to/my.cert"
    c.Proxy.tls_key = "/path/to/my.key"

Note that the certificate and key *must* be stored in a secure location where
they are readable only by admin users.

Using letsencrypt
^^^^^^^^^^^^^^^^^

It is also possible to use letsencrypt_ to automatically obtain TLS
certificates. If you have letsencrypt running using the default options, you
can configure this by adding the following to your ``dask_gateway_config.py``
file:

.. code-block:: python

    c.Proxy.tls_cert = "/etc/letsencrypt/live/{FQDN}/fullchain.pem"
    c.Proxy.tls_key = "/etc/letsencrpyt/live/{FQDN}/privkey.pem"

where ``FQDN`` is the  `fully qualified domain name`_ for your server.

Using external TLS termination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If ``dask-gateway-server`` is running behind a proxy that does TLS termination
(e.g. NGINX_), then no further configuration is needed.


Proxy authentication tokens
---------------------------

To secure communication between the proxy and the gateway server, a secret
token is used. By default this token is generated automatically. It's necessary
for an admin to configure this explicitly if the proxies are being externally
managed (i.e. :data:`c.Proxy.externally_managed` is set to true). To do this
you have two options:

- Configure :data:`c.Proxy.api_token` in your ``dask_gateway_config.py`` file.
  Since the token should be kept secret, the config file *must* be readable
  only by admin users.
- Set the ``DASK_GATEWAY_PROXY_TOKEN`` environment variable.  For security
  reasons, this environment variable should only be visible by the gateway
  server and proxy.

In either case both options take 32 byte random strings, encoded as hex. One way
to create these is through the ``openssl`` CLI:

.. code-block:: shell

    $ openssl rand -hex 32


.. _HTTPS: https://en.wikipedia.org/wiki/HTTPS
.. _letsencrypt: https://letsencrypt.org/
.. _fully qualified domain name: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
.. _NGINX: https://docs.nginx.com/nginx/admin-guide/security-controls/terminating-ssl-http/
