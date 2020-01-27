from traitlets import Instance, Integer, Dict, Union, Unicode
from traitlets.config import LoggingConfigurable, Configurable

from ..options import Options
from ..traitlets import MemoryLimit, Type, Callable
from ..utils import awaitable


class Backend(LoggingConfigurable):
    cluster_options = Union(
        [Callable(), Instance(Options, args=())],
        help="""
        User options for configuring an individual cluster.

        Allows users to specify configuration overrides when creating a new
        cluster. See the documentation for more information:

        :doc:`cluster-options`.
        """,
        config=True,
    )

    cluster_config_class = Type(
        "dask_gateway_server.backends.base.ClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    # Forwarded from the main application
    api_url = Unicode()

    async def process_cluster_options(self, user, request):
        if callable(self.cluster_options):
            options = await awaitable(self.cluster_options(user))
        else:
            options = self.cluster_options

        cluster_options = options.parse_options(request)
        overrides = options.get_configuration(cluster_options)
        config = self.cluster_config_class(**overrides).to_dict()
        return cluster_options, config

    async def setup(self):
        """Called when the server is starting up.

        Do any setup tasks in this method"""
        pass

    async def cleanup(self):
        """Called when the server is shutting down.

        Do any cleanup tasks in this method"""
        pass

    async def list_clusters(self, user=None, statuses=None):
        """List known clusters.

        Parameters
        ----------
        user : str, optional
            A user name to filter on. If not provided, defaults to
            all users.
        statuses : list, optional
            A list of statuses to filter on. If not provided, defaults to all
            running and pending clusters.

        Returns
        -------
        clusters : List[Cluster]
        """
        raise NotImplementedError

    async def get_cluster(self, cluster_id):
        """Get information about a cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.

        Returns
        -------
        cluster : Cluster
        """
        raise NotImplementedError

    async def start_cluster(self, user, cluster_options):
        """Start a new cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_options : dict
            Any additional options provided by the user.

        Returns
        -------
        cluster_id : str
        """
        raise NotImplementedError

    async def stop_cluster(self, user, cluster_id):
        """Stop a cluster.

        No-op if the cluster is already stopped.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        """
        raise NotImplementedError

    async def scale_cluster(self, user, cluster_id, n):
        """Scale a cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        n : int
            The number of workers to scale to.
        """
        raise NotImplementedError

    async def adapt_cluster(
        self, user, cluster_id, minimum=None, maximum=None, active=True
    ):
        """Adaptively scale a cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        minimum : int, optional
            The minimum number of workers to adaptively scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to adaptively scale to. Defaults to infinity.
        active : bool, optional
            Set to False to disable adaptive scaling.
        """
        raise NotImplementedError


class ClusterConfig(Configurable):
    scheduler_cmd = Unicode(
        "dask-gateway-scheduler",
        help="Shell command to start a dask-gateway scheduler.",
        config=True,
    )

    worker_cmd = Unicode(
        "dask-gateway-worker",
        help="Shell command to start a dask-gateway worker.",
        config=True,
    )

    environment = Dict(
        help="""
        Environment variables to set for both the worker and scheduler processes.
        """,
        config=True,
    )

    worker_memory = MemoryLimit(
        "2 G",
        help="""
        Number of bytes available for a dask worker. Allows the following
        suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True,
    )

    worker_cores = Integer(
        1,
        min=1,
        help="""
        Number of cpu-cores available for a dask worker.
        """,
        config=True,
    )

    scheduler_memory = MemoryLimit(
        "2 G",
        help="""
        Number of bytes available for a dask scheduler. Allows the following
        suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True,
    )

    scheduler_cores = Integer(
        1,
        min=1,
        help="""
        Number of cpu-cores available for a dask scheduler.
        """,
        config=True,
    )

    def to_dict(self):
        return {
            k: getattr(self, k)
            for k in self.trait_names()
            if k not in {"parent", "config"}
        }
