Install on a Hadoop Cluster
===========================

Here we provide instructions for installing and configuring
``dask-gateway-server`` on a `Hadoop Cluster`_.


Create a user account
---------------------

Before installing anything, you'll need to create the user account which will
be used to run the ``dask-gateway-server`` process. The name of the user
doesn't matter, only the permissions they have. Here we'll use ``dask``:

.. code-block:: bash

    $ adduser dask


Enable proxy-user permissions
-----------------------------

Dask-Gateway makes full use of Hadoop's security model, and will start Dask
workers in containers with the requesting user's permissions (e.g. if ``alice``
creates a cluster, their dask workers will be running as user ``alice``).  To
accomplish this, the gateway server needs `proxy-user`_ permissions. This
allows the Dask-Gateway server to perform actions impersonating another user.

For ``dask-gateway-server`` to work properly, you'll need to enable
`proxy-user`_ permissions for the ``dask`` user account. The users ``dask`` has
permission to impersonate can be restricted to certain groups, and requests to
impersonate may be restricted to certain hosts. At a minimum, the ``dask`` user
will require permission to impersonate anyone using the gateway, with requests
allowed from at least the host running the ``dask-gateway-server``.

.. code-block:: xml

    <property>
      <name>hadoop.proxyuser.dask.hosts</name>
      <value>host-where-dask-gateway-is-running</value>
    </property>
    <property>
      <name>hadoop.proxyuser.dask.groups</name>
      <value>group1,group2</value>
    </property>

If looser restrictions are acceptable, you may also use the wildcard ``*``
to allow impersonation of any user or from any host.

.. code-block:: xml

    <property>
      <name>hadoop.proxyuser.dask.hosts</name>
      <value>*</value>
    </property>
    <property>
      <name>hadoop.proxyuser.dask.groups</name>
      <value>*</value>
    </property>

See the `proxy-user`_ documentation for more information.


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

    $ conda install -y -c conda-forge dask-gateway-server-yarn

If you want to use Kerberos for user-facing authentication, you'll also want to
install ``dask-gateway-server-kerberos``:

.. code-block:: bash

    $ conda install -y -c conda-forge dask-gateway-server-kerberos


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


Enable YARN integration
~~~~~~~~~~~~~~~~~~~~~~~

First you'll want to enable YARN as the cluster backend.

.. code-block:: python

    # Configure the gateway to use YARN as the backend
    c.DaskGateway.backend_class = (
        "dask_gateway_server.backends.yarn.YarnBackend"
    )


Enable kerberos security (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your cluster has Kerberos_ enabled, you'll also need to create a principal
and keytab for the ``dask`` user. You'll also need to create a ``HTTP`` service
principal for the host running ``dask-gateway-server`` (if one doesn't already
exist).  Keytabs can be created on the command-line as:

.. code-block:: shell

    # Create the dask principal
    $ kadmin -q "addprinc -randkey dask@YOUR_REALM.COM"

    # Create the HTTP principal (if not already created)
    $ kadmin -q "addprinc -randkey HTTP/FQDN"

    # Create a keytab
    $ kadmin -q "xst -norandkey -k /etc/dask-gateway/dask.keytab dask HTTP/FQDN"

where ``FQDN`` is the `fully qualified domain name`_ of the host running
``dask-gateway-server``.

Store the keytab file wherever you see fit (we recommend storing it along with
the other configuration in ``/etc/dask-gateway/``, as above). You'll also want
to make sure that ``dask.keytab`` is only readable by the ``dask`` user.

.. code-block:: shell

    $ chown dask /etc/dask-gateway/dask.keytab
    $ chmod 400 /etc/dask-gateway/dask.keytab

To configure ``dask-gateway-server`` to use this keytab file, you'll need to
add the following line to your ``dask_gateway_config.py``:

.. code-block:: python

    # Specify the location of the keytab file, and the principal name
    c.YarnBackend.keytab = "/etc/dask-gateway/dask.keytab"
    c.YarnBackend.principal = "dask"

    # Enable Kerberos for user-facing authentication
    c.DaskGateway.authenticator_class = "dask_gateway_server.auth.KerberosAuthenticator"
    c.KerberosAuthenticator.keytab = "/etc/dask-gateway/dask.keytab"


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

Since the Dask workers/schedulers will be running in their own YARN containers,
you'll need to provide a way for Python environments to be available to these
containers. You have a few options here:

- Install identical Python environments on every node
- Archive environments to be distributed to the container at runtime (recommended)

In either case, the Python environment requires at least the
``dask-gateway`` package be installed to work properly.


Using a local environment
^^^^^^^^^^^^^^^^^^^^^^^^^

If you've installed identical Python environments on every node, you only need
to configure ``dask-gateway-server`` to use the provided Python. This could be
done a few different ways:

.. code-block:: python

    # Configure the paths to the dask-scheduler/dask-worker CLIs
    c.YarnClusterConfig.scheduler_cmd = "/path/to/dask-scheduler"
    c.YarnClusterConfig.worker_cmd = "/path/to/dask-worker"

    # OR
    # Activate a local conda environment before startup
    c.YarnClusterConfig.scheduler_setup = 'source /path/to/miniconda/bin/activate /path/to/environment'
    c.YarnClusterConfig.worker_setup = 'source /path/to/miniconda/bin/activate /path/to/environment'

    # OR
    # Activate a virtual environment before startup
    c.YarnClusterConfig.scheduler_setup = 'source /path/to/your/environment/bin/activate'
    c.YarnClusterConfig.worker_setup = 'source /path/to/your/environment/bin/activate'


Using an archived environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

YARN also provides mechanisms to "localize" files/archives to a container
before starting the application. This can be used to distribute Python
environments at runtime. This approach is appealing in that it doesn't require
installing anything throughout the cluster, and allows for centrally managing
user's Python environments.

Packaging environments for distribution is usually accomplished using

- conda-pack_ for conda_ environments
- venv-pack_  for virtual environments (both venv_ and virtualenv_ supported)

Both are tools for taking an environment and creating an archive of it in a way
that (most) absolute paths in any libraries or scripts are altered to be
relocatable. This archive then can be distributed with your application, and
will be automatically extracted during `YARN resource localization`_

Below we demonstrate creating and packaging a Conda environment containing
``dask-gateway``, as well as ``pandas`` and ``scikit-learn``. Additional
packages could be added as needed.

**Packaging a conda environment with conda-pack**

.. code-block:: bash

    # Make a folder for storing the conda environments locally
    $ mkdir /opt/dask-gateway/envs

    # Create a new conda environment
    $ conda create -c conda-forge -y -p /opt/dask-gateway/envs/example
    ...

    # Activate the environment
    $ conda activate /opt/dask-gateway/envs/example

    # Install dask-gateway, along with any other packages
    $ conda install -c conda-forge -y dask-gateway pandas scikit-learn conda-pack

    # Package the environment into example.tar.gz
    $ conda pack -o example.tar.gz
    Collecting packages...
    Packing environment at '/opt/dask-gateway/envs/example' to 'example.tar.gz'
    [########################################] | 100% Completed | 17.9s


**Using the packaged environment**

It is recommended to upload the environments to some directory on HDFS
beforehand, to avoid repeating the upload cost for every user. This directory
should be readable by all users, but writable only by the admin user managing
Python environments (here we'll use the ``dask`` user, and create a
``/dask-gateway`` directory).

.. code-block:: shell

    $ hdfs dfs -mkdir -p /dask-gateway
    $ hdfs dfs -chown dask /dask-gateway
    $ hdfs dfs -chmod 755 /dask-gateway

Uploading our already packaged environment to hdfs:

.. code-block:: shell

    $ hdfs dfs -put /opt/dask-gateway/envs/example.tar.gz /dask-gateway/example.tar.gz

To use the packaged environment with ``dask-gateway-server``, you need to
include the archive in ``YarnClusterConfig.localize_files``, and activate the
environment in
``YarnClusterConfig.scheduler_setup``/``YarnClusterConfig.worker_setup``.

.. code-block:: python

    c.YarnClusterConfig.localize_files = {
        'environment': {
            'source': 'hdfs:///dask-gateway/example.tar.gz',
            'visibility': 'public'
        }
    }
    c.YarnClusterConfig.scheduler_setup = 'source environment/bin/activate'
    c.YarnClusterConfig.worker_setup = 'source environment/bin/activate'

Note that we set ``visibility`` to ``public`` for the environment, so that
multiple users can all share the same localized environment (reducing the cost
of moving the environments around).

For more information, see the `Skein documentation on distributing files`_.


Additional configuration options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``dask-gateway-server`` has several additional configuration fields. See the
:doc:`api-server` docs (specifically :ref:`the yarn configuration docs
<yarn-config>`) for more information on all available options. At a minimum
you'll probably want to configure the worker resource limits, as well as which
YARN queue to use.

.. code-block:: python

    # The resource limits for a worker
    c.YarnClusterConfig.worker_memory = '4 G'
    c.YarnClusterConfig.worker_cores = 2

    # The YARN queue to use
    c.YarnClusterConfig.queue = '...'

If your cluster is under high load (and jobs may be slow to start), you may
also want to increase the cluster/worker timeout values:

.. code-block:: python

    # Increase startup timeouts to 5 min (600 seconds) each
    c.YarnClusterConfig.cluster_start_timeout = 600
    c.YarnClusterConfig.worker_start_timeout = 600


Example
~~~~~~~

In summary, an example ``dask_gateway_config.py`` configuration might look
like:

.. code-block:: python

    # Configure the gateway to use YARN as the backend
    c.DaskGateway.backend_class = (
        "dask_gateway_server.backends.yarn.YarnBackend"
    )

    # Specify the location of the keytab file, and the principal name
    c.YarnBackend.keytab = "/etc/dask-gateway/dask.keytab"
    c.YarnBackend.principal = "dask"

    # Enable Kerberos for user-facing authentication
    c.DaskGateway.authenticator_class = "dask_gateway_server.auth.KerberosAuthenticator"
    c.KerberosAuthenticator.keytab = "/etc/dask-gateway/dask.keytab"

    # Specify location of the archived Python environment
    c.YarnClusterConfig.localize_files = {
        'environment': {
            'source': 'hdfs:///dask-gateway/example.tar.gz',
            'visibility': 'public'
        }
    }
    c.YarnClusterConfig.scheduler_setup = 'source environment/bin/activate'
    c.YarnClusterConfig.worker_setup = 'source environment/bin/activate'

    # Limit resources for a single worker
    c.YarnClusterConfig.worker_memory = '4 G'
    c.YarnClusterConfig.worker_cores = 2

    # Specify the YARN queue to use
    c.YarnClusterConfig.queue = 'dask'


Open relevant port(s)
---------------------

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

A user's environment requires the ``dask-gateway`` library be installed. If
your cluster is secured with kerberos, you'll also need to install
``dask-gateway-kerberos``.

.. code-block:: shell

    # Install the dask-gateway client library
    $ conda create -n dask-gateway -c conda-forge dask-gateway

    # If kerberos is enabled, also install dask-gateway-kerberos
    $ conda create -n dask-gateway -c conda-forge dask-gateway-kerberos

You can connect to the gateway by creating a :class:`dask_gateway.Gateway`
object, specifying the public address (note that if you configured
:data:`c.Proxy.tcp_address` you'll also need to specify the ``proxy_address``).

.. code-block:: python

    >>> from dask_gateway import Gateway

    # When running without kerberos
    >>> gateway = Gateway("http://public-address")

    # OR, if kerberos is enabled, you'll need to kinit and then do
    >>> gateway = Gateway("http://public-address", auth="kerberos")

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


.. _Hadoop Cluster: https://hadoop.apache.org/
.. _miniconda: https://docs.conda.io/en/latest/miniconda.html
.. _proxy-user: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html
.. _conda-pack: https://conda.github.io/conda-pack/
.. _conda: http://conda.io/
.. _venv:
.. _virtualenv: https://virtualenv.pypa.io/en/stable/
.. _venv-pack documentation:
.. _venv-pack: https://jcristharif.com/venv-pack/
.. _YARN resource localization: https://hortonworks.com/blog/resource-localization-in-yarn-deep-dive/
.. _Skein documentation on distributing files: https://jcristharif.com/skein/distributing-files.html
.. _Kerberos: https://web.mit.edu/kerberos/
.. _fully qualified domain name: https://en.wikipedia.org/wiki/Fully_qualified_domain_name
