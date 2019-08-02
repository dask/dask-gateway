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

   curl -X GET https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
   bash miniconda.sh -b -p ~/miniconda
   ~/miniconda/bin/conda init bash  # or insert your shell here if it is not bash
   # restart your shell or close / open your ssh connection again to activate your conda environment

2. Create the server conda environment ::

   sudo su
   # Now create your server environment and include the client so you can easily test your installation
   conda create -n dask-gateway-server \
       -c conda-forge \
       dask-gateway-server-yarn \
       dask-gateway-server-kerberos \
       dask-gateway-kerberos

   # Let's do a quick test to make sure our binaries are installed correctly
   conda activate dask-gateway-server
   dask-gateway-server --help
   # These next two imports should print out the same version and it should
   # be the version that you installed a few lines up
   python -c "import dask_gateway; print(dask_gateway.__version__)"
   python -c "import dask_gateway_server; print(dask_gateway_server.__version__)"
   # This next file needs to be importable if you're using Yarn
   python -c "from dask_gateway_server.managers import yarn; print(yarn.__file__)"

3. Set up dask-gateway's configuration file ::

4. Run dask-gateway and debug it :) ::


3. Make required folders and service users ::

   # Recommendation is to create a user without a home directory (-r)
   # and to set the shell to a non-existent one so no one can log in as
   # this user (-s /bin/false)
   useradd -r -s /bin/false dask
   mkdir -p /var/log/dask-gateway-server
   mkdir -p /etc/dask-gateway-server/conf



4. Configure the upstart script for dask-gateway-server ::

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






Install for development
-----------------------

1. Create your conda development environment

   conda create -n dask-gateway-dev go pip --file dask-gateway/requirements.txt --file dask-gateway-server/requirements.txt -c conda-forge

2. Install dask-gateway and dask-gateway-server in editable mode

   conda activate dask-gateway-dev
   cd dask-gateway
   pip install -e .
   cd dask-gateway-server
   pip install -e .

   # Note -- I'm missing something here because recreating the
   environment doesn't recompile the go piece. Where does this
   compiled go live?

3. You may need to install additional libraries depending on your use case

  * for dask-gateway-server:
      * Yarn: `conda install "skein>=0.7.3"`
      * Kerberos: `conda install pykerberos`
      * Kubernetes: `conda install "python-kubernetes>=9"`
  * for dask-gateway:
      * Kerberos: `conda install pykerberos` (or winkerberos on win)

Note: from here on I'm assuming Yarn cluster manager. PR's welcome to add instructions for other cluster managers

4. Create your conda env that gets distributed to HDFS

   conda create -n dask-gateway-hdfs-env dask-gateway-kerberos conda-pack -c conda-forge

5. Pack the env and upload it to hdfs

```
conda activate dask-gateway-hdfs-env
conda pack -d ./environment
hdfs dfs -put dask-gateway-hdfs-env.tar.gz
```
6. Update the dask-gateway-config.py to reflect where this hdfs env is located

```
c.YarnClusterManager.localize_files = {
    'environment':  {
        'source': 'hdfs:///user/edill/dask-gateway-hdfs-env.tar.gz',
        'visibility': 'public'
}}
c.YarnClusterManager.scheduler_setup = 'source environment/bin/activate'
c.YarnClusterManager.worker_setup = 'source environment/bin/activate'
```
