User Resource Limits
====================

By default users can create as many concurrent clusters as they want, and use
as many resources as they want. In shared environments this may not always be
desirable. To remedy this administrators can set per-user resource limits.

A few limits are available:

- :data:`c.UserLimits.max_cores`: Maximum number of cores per user
- :data:`c.UserLimits.max_memory`: Maximum amount of memory per user
- :data:`c.UserLimits.max_clusters`: Maximum number of clusters per user

If a user is at capacity for any of these limits, requests for new clusters or
workers will error with an informative message saying they're at capacity.

Example
-------

Here we limit each user to:

- A max of 2 active clusters
- A max of 80 active cores
- A max of 1 TiB of RAM

.. code-block:: python

    c.UserLimits.max_clusters = 2
    c.UserLimits.max_cores = 80
    c.UserLimits.max_memory = "1 T"
