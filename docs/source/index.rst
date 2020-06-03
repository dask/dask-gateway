Dask Gateway
============

Dask Gateway provides a secure, multi-tenant server for managing Dask_
clusters.  It allows users to launch and use Dask clusters in a shared,
centrally managed cluster environment, without requiring users to have direct
access to the underlying cluster backend (e.g. Kubernetes, Hadoop/YARN, HPC Job
queues, etc...).


Highlights
----------

- **Centrally Managed**: Administrators do the heavy lifting of configuring the
  Gateway, users simply connect to the Gateway to get a new cluster. Eases deployment,
  and allows enforcing consistent configuration across all users.

- **Secure by Default**: Cluster communication is automatically encrypted with
  TLS. All operations are authenticated with a configurable protocol, allowing
  you to use what makes sense for your organization.

- **Flexible**: The gateway is designed to support multiple backends, and runs
  equally well in the cloud as on-premise. Natively supports Kubernetes,
  Hadoop/YARN, and HPC Job Queueing systems.

- **Robust to Failure**: The gateway can be restarted or experience failover
  without losing existing clusters. Allows for seamless upgrades and restarts
  without disrupting users.


Architecture Overview
---------------------

Dask Gateway is divided into three separate components:

- Multiple active **Dask Clusters** (potentially more than one per user)
- A **Proxy** for proxying both the connection between the user's client
  and their respective scheduler, and the Dask Web UI for each cluster
- A central **Gateway** that manages authentication and cluster startup/shutdown


.. image:: /_images/architecture.svg
    :width: 90 %
    :align: center
    :alt: Dask-Gateway high-level architecture


The gateway is designed to be flexible and pluggable, and makes heavy use of
traitlets_ (the same technology used by the Jupyter_ ecosystem). In particular,
both the cluster backend and the authentication protocol are pluggable.

**Cluster Backends**

- Kubernetes_
- `Hadoop/YARN`_
- Job Queue Systems (PBS_, Slurm_, ...)
- Local Processes

**Authentication Methods**

- `Kerberos <https://en.wikipedia.org/wiki/Kerberos_(protocol)>`__
- `JupyterHub service plugin <https://jupyterhub.readthedocs.io/en/stable/>`__
- HTTP Basic


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: For Users

   install-user
   usage
   configuration-user

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Admin - Installation

   install-local
   install-hadoop
   install-kube
   install-jobqueue

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Admin - Customization

   authentication
   security
   cluster-options
   resource-limits

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Reference

   api-client
   api-server
   develop


.. _Dask: https://dask.org/
.. _traitlets: https://traitlets.readthedocs.io/en/stable/
.. _Jupyter: https://jupyter.org/
.. _Hadoop/YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
.. _PBS: https://www.pbspro.org/
.. _Slurm: https://slurm.schedmd.com/
.. _Kubernetes: https://kubernetes.io/
