from traitlets import Unicode, Float, Integer, Dict, Bool
from traitlets.config import LoggingConfigurable

from .utils import MemoryLimit


class ClusterManager(LoggingConfigurable):
    """Base class for dask cluster managers"""

    environment = Dict(
        help="""
        Environment variables to set for both the worker and scheduler processes.
        """,
        config=True,
    )

    cluster_start_timeout = Float(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask cluster.

        Note that this is the time for ``start_cluster`` to run, not the time
        for the cluster to actually respond.
        """,
        config=True,
    )

    cluster_connect_timeout = Float(
        30,
        help="""
        Timeout (in seconds) for a started dask cluster to connect to the gateway.

        This is the time between ``start_cluster`` completing and the scheduler
        connecting to the gateway.
        """,
        config=True,
    )

    worker_start_timeout = Float(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask worker.
        """,
        config=True,
    )

    worker_connect_timeout = Float(
        30,
        help="""
        Timeout (in seconds) for a started dask worker to connect to the scheduler.

        This is the time between ``start_worker`` completing and the worker
        connecting to the scheduler.
        """,
        config=True,
    )

    worker_memory = MemoryLimit(
        "2 G",
        help="""
        Maximum number of bytes a dask worker is allowed to use. Allows the
        following suffixes:

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
        Maximum number of cpu-cores a dask worker is allowed to use.
        """,
        config=True,
    )

    scheduler_memory = MemoryLimit(
        "2 G",
        help="""
        Maximum number of bytes a dask scheduler is allowed to use. Allows the
        following suffixes:

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
        Maximum number of cpu-cores a dask scheduler is allowed to use.
        """,
        config=True,
    )

    worker_cmd = Unicode(
        "dask-gateway-worker",
        help="Shell command to start a dask-gateway worker.",
        config=True,
    )

    scheduler_cmd = Unicode(
        "dask-gateway-scheduler",
        help="Shell command to start a dask-gateway scheduler.",
        config=True,
    )

    # Parameters forwarded by gateway application
    api_url = Unicode()
    temp_dir = Unicode()

    def get_tls_paths(self, cluster_info):
        """Get the absolute paths to the tls cert and key files."""
        return "dask.crt", "dask.pem"

    def get_env(self, cluster_info):
        """Get a dict of environment variables to set for the process"""
        out = dict(self.environment)
        tls_cert_path, tls_key_path = self.get_tls_paths(cluster_info)
        # Set values that dask-gateway needs to run
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_CLUSTER_NAME": cluster_info.cluster_name,
                "DASK_GATEWAY_API_TOKEN": cluster_info.api_token,
                "DASK_GATEWAY_TLS_CERT": tls_cert_path,
                "DASK_GATEWAY_TLS_KEY": tls_key_path,
            }
        )
        return out

    supports_bulk_shutdown = Bool(
        False,
        help="""
        Whether ``stop_cluster`` implements a bulk shutdown method.

        If ``False`` (default), on shutdown ``stop_worker`` will be
        individually called for each worker. Otherwise ``stop_cluster`` will be
        called once and must stop all the workers in one action. This option
        makes sense for cluster backends where all processes are in a group and
        can be killed efficiently in one action.
        """,
    )

    async def start_cluster(self, cluster_info):
        """Start a new cluster.

        This should do any initialization for the whole dask cluster
        application, and then start the scheduler.

        Parameters
        ----------
        cluster_info : ClusterInfo
            Information about the cluster to be started.

        Yields
        ------
        cluster_state : dict
            Any state needed for further interactions with this cluster. This
            should be serializable using ``json.dumps``. If startup occurs in
            multiple stages, can iteratively yield state updates to be
            checkpointed. If an error occurs at any time, the last yielded
            state will be used when calling ``stop_cluster``.
        """
        raise NotImplementedError

    async def stop_cluster(self, cluster_info, cluster_state):
        """Stop the cluster.

        Parameters
        ----------
        cluster_info : ClusterInfo
            Information about the cluster.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
        """
        raise NotImplementedError

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        """Start a new worker.

        Parameters
        ----------
        worker_name : str
            The worker name, should be passed to ``dask-gateway-worker`` via
            the ``--name`` flag, or set as the ``DASK_GATEWAY_WORKER_NAME``
            environment variable.
        cluster_info : ClusterInfo
            Information about the cluster.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.

        Returns
        -------
        worker_state : dict
            Any state needed for further interactions with this worker. This
            should be serializable using ``json.dumps``. If startup occurs in
            multiple stages, can iteratively yield state updates to be
            checkpointed. If an error occurs at any time, the last yielded
            state will be used when calling ``stop_worker``.
        """
        raise NotImplementedError

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        """Remove a worker.

        Parameters
        ---------
        worker_name : str
            The worker name.
        worker_state : dict
            Any additional worker state returned from ``start_worker``.
        cluster_info : ClusterInfo
            Information about the cluster.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
        """
        raise NotImplementedError
