Dask Gateway Installation
=========================

Install from a release
----------------------
Dask-gateway has two components -- the client (dask-gateway) and the server (dask-gateway-server).
Each of these has a few flavors with optional dependencies.
The conda-forge install command is `conda install -c conda-forge <name>`.
The pypi install command is `pip install <name>`

===================================  ===================================  ==================================================
conda-forge                          pypi                                 description
===================================  ===================================  ==================================================
``dask-gateway``                     ``dask-gateway``                     base client
``dask-gateway-kerberos``            ``dask-gateway[kerberos]``           client with Kerberos auth support
``dask-gateway-server``              ``dask-gateway-server``              base server
``dask-gateway-server-kerberos``     ``dask-gateway-server[kerberos]``    server with Kerberos auth support
``dask-gateway-server-kubernetes``   ``dask-gateway-server[kubernetes]``  server with support for Dask clusters on kubernetes
``dask-gateway-server-yarn``         ``dask-gateway-server[yarn]``        server with support for Dask clusters on Yarn
===================================  ===================================  ===================================================

Installing for Yarn
*******************
1. Install miniconda onto an edge node of your Hadoop cluster ::

   sudo su
   curl -X GET https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
   bash miniconda.sh -b -p /opt/miniconda
   /opt/miniconda/bin/conda init bash  # or insert your shell here if it is not bash
   # restart your shell or close / open your ssh connection again to activate your conda environment

2. Create the server conda environment ::

   sudo su
   # Now create your server environment and include the client so you can easily test your installation
   conda create -p /opt/dask-gateway-server \
       -c conda-forge \
       dask-gateway-server-yarn \
       dask-gateway-server-kerberos \
       dask-gateway-kerberos

   # Let's do a quick test to make sure our binaries are installed correctly
   conda activate -p /opt/dask-gateway-server
   dask-gateway-server --help
   # These next two imports should print out the same version and it should
   # be the version that you installed a few lines up
   python -c "import dask_gateway; print(dask_gateway.__version__)"
   python -c "import dask_gateway_server; print(dask_gateway_server.__version__)"
   # This next file needs to be importable if you're using Yarn
   python -c "from dask_gateway_server.managers import yarn; print(yarn.__file__)"

3. Make required folders and service users ::

   # Recommendation is to create a user without a home directory (-r)
   # and to set the shell to a non-existent one so no one can log in as
   # this user (-s /bin/false)
   useradd -r -s /bin/false dask
   mkdir -p /var/log/dask-gateway-server
   mkdir -p /etc/dask-gateway-server/conf


3. Configure the upstart script for dask-gateway-server ::

   vi /etc/init/dask-gateway-server.conf
   description "Starts Dask gateway server for Yarn"
   author "Eric Dill"
   start on runlevel [2345]
   stop on runlevel [016]

   start on started netfs
   start on started rsyslog

   start on stopping netfs
   start on stopping rsyslog

   respawn

   # respawn unlimited times with 5 seconds time interval
   respawn limit 0 5

   env SLEEP_TIME=10
   env DAEMON="dask-gateway-server"
   env DESC="Starts dask gateway server"
   env EXEC_PATH="/opt/dask-gateway-server/bin/dask-gateway-server
   env SVC_USER="dask"
   env DAEMON_FLAGS=""
   env CONF_DIR="/etc/dask-gateway-server/conf"

   TODO: FINISH THIS FILE


4. Set up dask-gateway's configuration file ::




Install for development
-----------------------
TBD
