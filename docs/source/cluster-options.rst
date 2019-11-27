Exposing Cluster Options
========================

By default cluster configuration (e.g. worker memory, docker image, etc...) is
set statically by an administrator in the server configuration file. To allow
users to change certain parameters when creating a new cluster an administrator
must explicitly expose them in the configuration.

User Experience
---------------

On the user side, exposing options with allows users to:

- Query what options (if any) are available using
  :meth:`dask_gateway.Gateway.cluster_options`

.. code-block:: python

    >>> options = gateway.cluster_options()
    >>> options
    Options<worker_cores=1, worker_memory=1.0, environment='basic'>

- Specify configuration overrides for exposed fields as keyword
  arguments to :meth:`dask_gateway.Gateway.new_cluster` or
  :meth:`dask_gateway.GatewayCluster`.

.. code-block:: python

    # Using Gateway.new_cluster
    >>> cluster = gateway.new_cluster(worker_cores=2, environment="tensorflow")

    # Or using the GatewayCluster constructor
    >>> cluster = GatewayCluster(worker_cores=2, environment="tensorflow")


- If working in a notebook, use the ipywidgets_ based GUI to configure a
  cluster.

.. image:: /_images/options-widget.png
    :alt: Cluster options widget


See :ref:`user-cluster-options` for more information on the user experience.


Server Configuration
--------------------

Options are exposed to the user by setting
:data:`c.DaskGateway.cluster_manager_options`. This configuration field takes
a :class:`dask_gateway_server.options.Options` object, which describes what
options are exposed to end users, and how the gateway server should interpret
those options.

.. autosummary:: ~dask_gateway_server.options.Options

A :class:`dask_gateway_server.options.Options` object takes two arguments:

- ``*fields``: One or more :class:`dask_gateway_server.options.Field` objects,
  which provide a typed declarative specification of each user facing option.

- ``handler``: An optional handler function for translating the values set by
  those options into configuration values to set on the cluster manager.

``Field`` objects provide typed specifications for a user facing option. There
are several different ``Field`` classes available, each representing a
different common type:

.. autosummary::
    ~dask_gateway_server.options.Integer
    ~dask_gateway_server.options.Float
    ~dask_gateway_server.options.Bool
    ~dask_gateway_server.options.String
    ~dask_gateway_server.options.Select

Each field supports the following standard parameters:

- ``field``: The field name to use. Must be a valid Python identifier. This
  will be the keyword users use to set this field programmatically (e.g.
  ``"worker_cores"``).
- ``default``: The default value if the user doesn't specify this field.
- ``label``: A human readable label that will be used in GUI representations
  (e.g. ``"Worker Cores"``). Optional, if not provided ``field`` will be used.
- ``target``: The target key to set in the processed options dict. Must be a
  valid Python identifier. Optional, if not provided ``field`` will be used.

After validation (type, bounds, etc...), a dictionary of all options for a
requested cluster is passed to a ``handler`` function. Here any additional
validation can be done (errors raised in the handler are forwarded to the
user), as well as any conversion needed between the exposed option fields and
configuration fields on the backing cluster manager. The default ``handler``
returns the provided options unchanged.

Available options are cluster manager specific. For example, if running on
Kubernetes, an options handler can return overrides for any configuration
fields on :ref:`KubeClusterManager <kube-cluster-manager-config>`. See
:ref:`cluster-managers-reference` for information on what configuration fields
are available on your backend.

Examples
--------

Worker Cores and Memory
^^^^^^^^^^^^^^^^^^^^^^^

Here we expose options for users to configure
:data:`c.ClusterManager.worker_cores` and
:data:`c.ClusterManager.worker_memory`. We set bounds on each resource to
prevent users from requesting too large of a worker. The handler is used to
convert the user specified memory from GiB to bytes (as expected by
:data:`c.ClusterManager.worker_memory`).

.. code-block:: python

    from dask_gateway_server.options import Options, Integer, Float

    def options_handler(options):
        return {
            "worker_cores": options.worker_cores,
            "worker_memory": int(options.worker_memory * 2 ** 30),
        }

    c.DaskGateway.cluster_manager_options = Options(
        Integer("worker_cores", default=1, min=1, max=4, label="Worker Cores"),
        Float("worker_memory", default=1, min=1, max=8, label="Worker Memory (GiB)"),
        handler=options_handler,
    )


Cluster Profiles
^^^^^^^^^^^^^^^^

Instead of exposing individual options, you may instead wish to expose
"profiles" - user-friendly names for common groups of options. For example,
here we provide 3 cluster profiles (small, medium, and large) a user can select
from.

.. code-block:: python

    from dask_gateway_server.options import Options, Select

    # A mapping from profile name to configuration overrides
    profiles = {
        "small": {"worker_cores": 2, "worker_memory": "4 G"},
        "medium": {"worker_cores": 4, "worker_memory": "8 G"},
        "large": {"worker_cores": 8, "worker_memory": "16 G"},
    }

    # Expose `profile` as an option, valid values are 'small', 'medium', or
    # 'large'. A handler is used to convert the profile name to the
    # corresponding configuration overrides.
    c.DaskGateway.cluster_manager_options = Options(
        Select(
            "profile",
            ["small", "medium", "large"],
            default="medium",
            label="Cluster Profile",
        )
        handler=lambda options: profiles[options.profile],
    )


.. _ipywidgets: https://ipywidgets.readthedocs.io/en/latest/
