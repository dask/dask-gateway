Dask Gateway Installation
=========================

Dask-gateway has two components -- the client (dask-gateway) and the server (dask-gateway-server).
Each of these has a few flavors with optional dependencies.

===============================  ===============================  ==================================================
pypi                             conda-forge                      description
===============================  ===============================  ==================================================
dask-gateway                     dask-gateway                     base client
dask-gateway-kerberos            dask-gateway[kerberos]           client with Kerberos auth support
dask-gateway-server              dask-gateway-server              base server
dask-gateway-server-kerberos     dask-gateway-server[kerberos]    server with Kerberos auth support
dask-gateway-server-kubernetes   dask-gateway-server[kubernetes]  server with support for Dask clusters on kubernetes
dask-gateway-server-yarn         dask-gateway-server[yarn]        server with support for Dask clusters on Yarn
===============================  ===============================  ===================================================





Install from a release
----------------------

You can install dask-gateway and dask-gateway-server from pypi or from conda-forge

Install for development
-----------------------
TBD
