Dask Gateway
============

Dask Gateway provides a secure, multi-tenant server for managing Dask_
clusters.  It allows users to launch and use Dask clusters in a shared,
centrally managed cluster environment, without requiring users have direct
access to the underlying cluster backend (e.g. Kubernetes, YARN, Job queues,
etc...).

Architecture Overview
---------------------

Dask Gateway is divided into four separate components:

- Multiple active **Dask Clusters** (potentially more than one per user)
- A **Web proxy** for proxying the Dask Web UI for each cluster
- A **Scheduler proxy** for proxying the connection between the user's client
  and their respective scheduler
- A central **Gateway** that manages authentication and cluster startup/shutdown


.. image:: /_images/architecture.svg
    :width: 90 %
    :align: center
    :alt: Dask-Gateway high-level architecture


The gateway is designed to be flexible and pluggable, and makes heavy use of
traitlets_ (the same technology used by the Jupyter_ ecosystem). In particular,
both the cluster backend and the authentication protocol are pluggable.

**Cluster Backends**

- YARN
- Local Processes
- Kubernetes (planned)
- Job Queues (planned)

**Authentication Methods**

- Kerberos
- Basic
- JupyterHub (planned)


.. toctree::
    :hidden:

    api-server
    api-client

.. _Dask: https://dask.org/
.. _traitlets: https://traitlets.readthedocs.io/en/stable/
.. _Jupyter: https://jupyter.org/
