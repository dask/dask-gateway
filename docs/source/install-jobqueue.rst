Install on an HPC Job Queue
===========================

Here we provide instructions for installing and configuring
``dask-gateway-server`` on a HPC Job Queue system like PBS_ or Slurm_. Note
that ``dask-gateway-server`` only needs to be installed on one node (typically
an edge node).

Currently only PBS_ and Slurm_ are supported, but support for additional
backends is planned. If this is something you're interested in, please `file an
issue <https://github.com/dask/dask-gateway/issues>`__.


Create a user account
---------------------

Before installing anything, you'll need to create the user account which will
be used to run the ``dask-gateway-server`` process. The name of the user
doesn't matter, only the permissions they have. Here we'll use ``dask``:

.. code-block:: bash

    $ adduser dask


Create install directories
--------------------------

A ``dask-gateway-server`` installation has three types of files which will need
their own directories created before installation:

- Software files: This includes a Python environment and all required
  libraries. Here we use ``/opt/dask-gateway``.
- Configuration files: Here we use ``/etc/dask-gateway``.
- Runtime files: Here we use ``/var/dask-gateway``.

.. code-block:: bash

    # Software files
    $ mkdir -p /opt/dask-gateway

    # Configuration files
    $ mkdir /etc/dask-gateway

    # Runtime files
    $ mkdir /var/dask-gateway
    $ chown dask /var/dask-gateway


Install a python environment
----------------------------

To avoid interactions between the system python installation and
``dask-gateway-server``, we'll install a full Python environment into the
software directory. The easiest way to do this is to use miniconda_, but this
isn't a strict requirement.

.. code-block:: bash

    $ curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
    $ bash /tmp/miniconda.sh -b -p /opt/dask-gateway/miniconda
    $ rm /tmp/miniconda.sh

We also recommend adding miniconda to the ``root`` user's path to ease further
commands.

.. code-block:: bash

    $ echo 'export PATH="/opt/dask-gateway/miniconda/bin:$PATH"' >> /root/.bashrc
    $ source /root/.bashrc


Install dask-gateway-server
---------------------------

Now we can install ``dask-gateway-server`` and its dependencies.

.. code-block:: bash

    $ conda install -y -c conda-forge dask-gateway-server-jobqueue


Enable permissions for the ``dask`` user
----------------------------------------

For ``dask-gateway-server`` to work properly, you'll need to enable sudo_
permissions for the ``dask`` user account. At a minimum, the ``dask`` account
will need passwordless permissions to run the
``dask-gateway-jobqueue-launcher`` command (installed as part of
``dask-gateway-server`` above) as any dask-gateway user.  The
``dask-gateway-jobqueue-launcher`` script is responsible for launching,
tracking, and stopping batch jobs for individual users, and thus needs to be
sudo-executed as them for permissions to be transferred appropriately.

An example entry in ``sudoers`` might look like:

.. code-block:: text

    Cmnd_Alias DASK_GATEWAY_JOBQUEUE_LAUNCHER = /opt/dask-gateway/miniconda/bin/dask-gateway-jobqueue-launcher

    dask ALL=(%dask_users,!root) NOPASSWD:DASK_GATEWAY_JOBQUEUE_LAUNCHER


Additionaly, when using PBS_ you'll need to make the ``dask`` user a PBS
Operator:

.. code-block:: bash

    $ qmgr -c "set server operators += dask@pbs"

Operator level permissions are needed in PBS_ to allow ``dask-gateway-server``
to more efficiently track the status of all users' jobs.


Configure dask-gateway-server
-----------------------------

Now we're ready to configure our ``dask-gateway-server`` installation.
Configuration is written as a Python file (typically
``/etc/dask-gateway/dask_gateway_config.py``). Options are assigned to a config
object ``c``, which is then loaded by the gateway on startup. You are free to
use any python syntax/libraries in this file that you want, the only things
that matter to ``dask-gateway-server`` are the values set on the ``c`` config
object.

Here we'll walk through a few common configuration options you may want to set.


Specify backend
~~~~~~~~~~~~~~~

First you'll need to specify which backend to use by setting
:data:`c.DaskGateway.backend_class`. You have a few options:

- PBS: ``dask_gateway_server.backends.jobqueue.pbs.PBSBackend``
- Slurm: ``dask_gateway_server.backends.jobqueue.slurm.SlurmBackend``

For example, here we configure the gateway to use the PBS backend:

.. code-block:: python

    # Configure the gateway to use PBS
    c.DaskGateway.backend_class = (
        "dask_gateway_server.backends.jobqueue.pbs.PBSBackend"
    )


Configure the server addresses (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, ``dask-gateway-server`` will serve all traffic through
``0.0.0.0:8000``. This includes both HTTP(S) requests (REST api, dashboards,
etc...) and dask scheduler traffic.

If you'd like to serve at a different address, or serve web and scheduler
traffic on different ports, you can configure the following fields:

- :data:`c.Proxy.address` - Serves HTTP(S) traffic, defaults to ``:8000``.

- :data:`c.Proxy.tcp_address` - Serves dask client-to-scheduler tcp traffic,
  defaults to :data:`c.Proxy.address`.


Here we configure web traffic to serve on port 8000 and scheduler traffic to
serve on port 8001:

.. code-block:: python

    c.Proxy.address = ':8000'
    c.Proxy.tcp_address = ':8001'


Specify user python environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since the Dask workers/schedulers will be running on disparate nodes across the
cluster, you'll need to provide a way for Python environments to be available
on every node. You have a few options here:

- Use a fixed path to a Python environment available on every node
- Allow users to specify the location of the Python environment (recommended)

In either case, the Python environment requires at least the ``dask-gateway``
package be installed to work properly.


Using a fixed environment path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If identical Python environments are available on every node (either local
disk, or NFS mount), you only need to configure ``dask-gateway-server`` to use
the provided Python. This could be done a few different ways:

.. code-block:: python

    # Configure the paths to the dask-scheduler/dask-worker CLIs
    c.JobQueueClusterConfig.scheduler_cmd = "/path/to/dask-scheduler"
    c.JobQueueClusterConfig.worker_cmd = "/path/to/dask-worker"

    # OR
    # Activate a local conda environment before startup
    c.JobQueueClusterConfig.scheduler_setup = 'source /path/to/miniconda/bin/activate /path/to/environment'
    c.JobQueueClusterConfig.worker_setup = 'source /path/to/miniconda/bin/activate /path/to/environment'

    # OR
    # Activate a virtual environment before startup
    c.JobQueueClusterConfig.scheduler_setup = 'source /path/to/your/environment/bin/activate'
    c.JobQueueClusterConfig.worker_setup = 'source /path/to/your/environment/bin/activate'


User-configurable python environments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alternatively, you might want to allow users to provide their own Python
environments. This can be useful, as it allows users to manage package versions
themselves without needing to contact an admin for support.

This can be done by exposing an option for Python environment in
:data:`c.Backend.cluster_options`. Exposing cluster options is
discussed in detail in :doc:`cluster-options` - here we'll only provide a short
example of one way of accomplishing this. Please see :doc:`cluster-options` for
more information.

.. code-block:: python

    from dask_gateway_server.options import Options, String

    def options_handler(options):
        # Fill in environment activation command template with the users
        # provided environment name. This command is then used as the setup
        # script for both the scheduler and workers.
        setup = "source ~/miniconda/bin/activate %s" % options.environment
        return {"scheduler_setup": setup, "worker_setup": setup}

    # Provide an option for users to specify the name or location of a
    # conda environment to use for both the scheduler and workers.
    # If not specified, the default environment of ``base`` is used.
    c.Backend.cluster_options = Options(
        String("environment", default="base", label="Conda Environment"),
        handler=options_handler,
    )


Additional configuration options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``dask-gateway-server`` has several additional configuration fields. See the
:doc:`api-server` docs (specifically :ref:`the jobqueue configuration docs
<jobqueue-config>`) for more information on all available options. At a minimum
you'll probably want to configure the worker resource limits.

.. code-block:: python

    # The resource limits for a worker
    c.JobQueueClusterConfig.worker_memory = '4 G'
    c.JobQueueClusterConfig.worker_cores = 2

If your cluster is under high load (and jobs may be slow to start), you may
also want to increase the cluster/worker timeout values:

.. code-block:: python

    # Increase startup timeouts to 5 min (600 seconds) each
    c.JobQueueClusterBackend.cluster_start_timeout = 600
    c.JobQueueClusterBackend.worker_start_timeout = 600


Example
~~~~~~~

In summary, an example ``dask_gateway_config.py`` configuration for PBS might
look like:

.. code-block:: python

    # Configure the gateway to use PBS as the backend
    c.DaskGateway.backend_class = "dask_gateway_server.backends.pbs.PBSBackend"

    # Configure the paths to the dask-scheduler/dask-worker CLIs
    c.PBSClusterConfig.scheduler_cmd = "~/miniconda/bin/dask-scheduler"
    c.PBSClusterConfig.worker_cmd = "~/miniconda/bin/dask-worker"

    # Limit resources for a single worker
    c.PBSClusterConfig.worker_memory = '4 G'
    c.PBSClusterConfig.worker_cores = 2

    # Specify the PBS queue to use
    c.PBSClusterConfig.queue = 'dask'

    # Increase startup timeouts to 5 min (600 seconds) each
    c.PBSClusterBackend.cluster_start_timeout = 600
    c.PBSClusterBackend.worker_start_timeout = 600


Open relevant ports
-------------------

For users to access the gateway server, they'll need access to the public
port(s) set in `Configure the server addresses (optional)`_ above (by default
this is port ``8000``). How to expose ports is system specific - cluster
administrators should determine how best to perform this task.


Start dask-gateway-server
-------------------------

At this point you should be able to start the gateway server as the ``dask``
user using your created configuration file. The ``dask-gateway-server`` process
will be a long running process - how you intend to manage it (``supervisord``,
etc...) is system specific. The requirements are:

- Start with ``dask`` as the user
- Start with ``/var/dask-gateway`` as the working directory
- Add ``/opt/dask-gateway/miniconda/bin`` to path
- Specify the configuration file location with ``-f /etc/dask-gateway/dask_gateway_config.py``

For ease, we recommend creating a small bash script stored at
``/opt/dask-gateway/start-dask-gateway`` to set this up:

.. code-block:: bash

    #!/usr/bin/env bash

    export PATH="/opt/dask-gateway/miniconda/bin:$PATH"
    cd /var/dask-gateway
    dask-gateway-server -f /etc/dask-gateway/dask_gateway_config.py

For *testing* here's how you might start ``dask-gateway-server`` manually:

.. code-block:: bash

    $ cd /var/dask-gateway
    $ sudo -iu dask /opt/dask-gateway/start-dask-gateway


Validate things are working
---------------------------

If the server started with no errors, you'll want to check that things are
working properly. The easiest way to do this is to try connecting as a user.

A user's environment requires the ``dask-gateway`` library be installed.

.. code-block:: shell

    # Install the dask-gateway client library
    $ conda create -n dask-gateway -c conda-forge dask-gateway

You can connect to the gateway by creating a :class:`dask_gateway.Gateway`
object, specifying the public address (note that if you configured
:data:`c.Proxy.tcp_address` you'll also need to specify the ``proxy_address``).

.. code-block:: python

    >>> from dask_gateway import Gateway
    >>> gateway = Gateway("http://public-address")

You should now be able to make API calls. Try
:meth:`dask_gateway.Gateway.list_clusters`, this should return an empty list.

.. code-block:: python

    >>> gateway.list_clusters()
    []

Next, see if you can create a cluster. This may take a few minutes.

.. code-block:: python

    >>> cluster = gateway.new_cluster()

The last thing you'll want to check is if you can successfully connect to your
newly created cluster.

.. code-block:: python

    >>> client = cluster.get_client()

If everything worked properly, you can shutdown your cluster with
:meth:`dask_gateway.GatewayCluster.shutdown`.

.. code-block:: python

    >>> cluster.shutdown()


.. _PBS: https://www.pbspro.org/
.. _Slurm: https://slurm.schedmd.com/
.. _miniconda: https://docs.conda.io/en/latest/miniconda.html
.. _sudo: https://en.wikipedia.org/wiki/Sudo
