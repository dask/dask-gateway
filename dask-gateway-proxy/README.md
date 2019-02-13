## dask-gateway-proxy

A configurable TLS proxy, that dispatches to different routes based on the
connection's [Server Name
Indication](https://en.wikipedia.org/wiki/Server_Name_Indication). Routes can
be added and removed at runtime using the provided REST API.
