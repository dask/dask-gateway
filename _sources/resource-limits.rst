Cluster Resource Limits
=======================

By default users can create clusters with as many workers and resources as they
want.  In shared environments this may not always be desirable. To remedy this
administrators can set per-cluster resource limits.

A few limits are available:

- :data:`c.ClusterConfig.cluster_max_cores`: Maximum number of cores per cluster
- :data:`c.ClusterConfig.cluster_max_memory`: Maximum amount of memory per cluster
- :data:`c.ClusterConfig.cluster_max_workers`: Maximum number of workers per cluster

If a cluster is at capacity for any of these limits, requests for new workers
or workers will warn with an informative message saying they're at capacity.

Example
-------

Here we limit each cluster to:

- A max of 80 active cores
- A max of 1 TiB of RAM

.. code-block:: python

    c.ClusterConfig.cluster_max_cores = 80
    c.ClusterConfig.cluster_max_memory = "1 T"
