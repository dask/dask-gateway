Dask Gateway
============

Dask Gateway provides a secure, multi-tenant server for managing Dask_
clusters.  It allows users to launch and use Dask clusters in a shared,
centrally managed cluster environment, without requiring users have direct
access to the underlying cluster backend (e.g. Kubernetes, Hadoop/YARN, HPC Job
queues, etc...).


Highlights
----------

- **Centrally Managed**: Administrators do the heavy lifting of configuring the
  Gateway, users only have to connect to get a new cluster. Eases deployment,
  and allows enforcing consistent configuration across all users.

- **Secure by Default**: Cluster communication is automatically encrypted with
  TLS. All operations are authenticated with a configurable protocol, allowing
  you to use what makes sense for your organization.

- **Flexible**: The gateway is designed to support multiple backends, and runs
  equally well in the cloud as on-premise. Natively supports Kubernetes,
  Hadoop/YARN, and HPC Job Queueing systems.

- **Robust to Failure**: The gateway can be restarted or failover without
  losing existing clusters. Allows for seemless upgrades and restarts without
  disrupting users.

.. toctree::
    :hidden:

    overview
    install
    backends
    api-server
    api-client

.. _Dask: https://dask.org/
.. _traitlets: https://traitlets.readthedocs.io/en/stable/
.. _Jupyter: https://jupyter.org/
.. _Hadoop/YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _PBS: https://www.pbspro.org/
.. _Slurm: https://slurm.schedmd.com/
.. _Kubernetes: https://kubernetes.io/
