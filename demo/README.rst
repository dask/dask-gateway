Dask-Gateway Hadoop Demo
========================

Here we provide a docker setup for running a demo of Dask-Gateway on a Hadoop
cluster.


Starting the Demo Cluster
-------------------------

To run, first install docker_, following the instructions for your OS. You'll
also need to make sure that docker is started with sufficient resources - we
recommend having at least 8 GB allocated to your ``docker-machine``.

The demo cluster can then be started as follows:

.. code-block:: shell

    # Clone the repository
    $ git clone https://github.com/jcrist/dask-gateway.git

    # Enter the `dask-gateway` directory
    $ cd dask-gateway/

    # Start the demo cluster
    $ ./demo/start-demo.sh


This will take ~30-60 second to install and start the gateway server. Once
ready the gateway server will be available on port ``8787`` at your
docker-machine IP address. This IP address can be found at:

.. code-block:: shell

    $ docker-machine inspect --format {{.Driver.IPAddress}})

For ease of use you may want to add ``master.example.com`` to your
``/etc/hosts`` file, rather than using the IP address above.

.. code-block:: text

    # /etc/hosts

    <IP-address-from-above>    master.example.com

The Hadoop Web UI can be viewed at ``http://master.example.com:8088/cluster``
(or use the IP above instead of ``master.example.com``).


Using the Demo Cluster
----------------------

You can connect to the running gateway server with the ``dask-gateway``
library. To install:

.. code-block:: shell

    $ pip install -e ./dask-gateway

You can then connect to the gateway by creating a ``Gateway`` instance. Two
user accounts have been created, you can login as either ``alice`` or ``bob``.

.. code-block:: python
    
    from dask_gateway import Gateway, BasicAuth

    gateway = Gateway("http://master.example.com:8787",
                      auth=BasicAuth("alice"))

You can create a new Dask cluster via the ``new_cluster`` method. This will
take a couple seconds as YARN processes the application request.

.. code-block:: python

    # Create a new cluster
    cluster = gateway.new_cluster()

If working in a Jupyter Notebook with IPWidgets installed you can use the GUI
to scale up/down the cluster. The ``scale`` method can also be used for this
purpose.

.. code-block:: python

    # Scale up to 4 workers
    cluster.scale(4)

To get a ``dask.distributed.Client`` object for this cluster, use the
``get_client`` method:

.. code-block:: python

    # Get a Client
    client = cluster.get_client()

At this point you're free to use the dask cluster as normal.

.. code-block:: python

    # Create an array and do some work
    import dask.array as da
    x = da.random.normal(size=(5000, 5000), chunks=(500, 500))
    x.dot(x.T).sum().compute()

When you're done you can shutdown the cluster using the ``shutdown`` method.

.. code-block:: python

    # Shutdown the running cluster
    cluster.shutdown()

Alternatively you can leave the cluster running, and connect to it later via
``gateway.connect``:

.. code-block:: python

    # List all your running clusters
    clusters = gateway.list_clusters()

    # Connect to the first cluster
    cluster = gateway.connect(clusters[0].name)


Stopping the Demo Cluster
-------------------------

When you're done with the demo, you can shut it down via:

.. code-block:: shell

    $ ./demo/stop-demo.sh


.. _docker: https://www.docker.com/
