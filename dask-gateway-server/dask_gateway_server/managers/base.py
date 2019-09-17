from traitlets import Bytes, Unicode, Float, Integer, Dict, Bool, Instance
from traitlets.config import LoggingConfigurable

from ..utils import MemoryLimit, TaskPool


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
        """,
        config=True,
    )

    cluster_status_period = Float(
        30,
        help="""
        Time (in seconds) between cluster status checks.

        A smaller period will detect failed clusters sooner, but will use more
        resources. A larger period will provide slower feedback in the presence
        of failures.
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

    worker_status_period = Float(
        30,
        help="""
        Time (in seconds) between worker status checks.

        A smaller period will detect failed workers sooner, but will use more
        resources. A larger period will provide slower feedback in the presence
        of failures.
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

    # Cluster-specific parameters forwarded by gateway application
    username = Unicode()
    cluster_name = Unicode()
    api_token = Unicode()
    tls_cert = Bytes()
    tls_key = Bytes()

    # Common parameters forwarded by gateway application
    task_pool = Instance(TaskPool, args=())
    api_url = Unicode()
    temp_dir = Unicode()

    def get_tls_paths(self):
        """Get the absolute paths to the tls cert and key files."""
        return "dask.crt", "dask.pem"

    def get_env(self):
        """Get a dict of environment variables to set for the process"""
        out = dict(self.environment)
        tls_cert_path, tls_key_path = self.get_tls_paths()
        # Set values that dask-gateway needs to run
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_CLUSTER_NAME": self.cluster_name,
                "DASK_GATEWAY_API_TOKEN": self.api_token,
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

    async def start_cluster(self):
        """Start a new cluster.

        This should do any initialization for the whole dask cluster
        application, and then start the scheduler.

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

    async def cluster_status(self, cluster_state):
        """Check the status of a cluster.

        Called periodically to check the status of a cluster.

        Parameters
        ----------
        cluster_state : dict
            Any additional state returned from ``start_cluster``.

        Returns
        -------
        running : bool
            Whether the cluster is running.
        msg : str, optional
            If not running, an optional message describing the exit condition.
        """
        raise NotImplementedError

    async def stop_cluster(self, cluster_state):
        """Stop the cluster.

        Parameters
        ----------
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
        """
        raise NotImplementedError

    async def start_worker(self, worker_name, cluster_state):
        """Start a new worker.

        Parameters
        ----------
        worker_name : str
            The worker name, should be passed to ``dask-gateway-worker`` via
            the ``--name`` flag, or set as the ``DASK_GATEWAY_WORKER_NAME``
            environment variable.
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

    async def worker_status(self, worker_name, worker_state, cluster_state):
        """Check the status of a worker.

        Called periodically to check the status of a worker. Once a worker is
        running this method will no longer be called.

        Parameters
        ---------
        worker_name : str
            The worker name.
        worker_state : dict
            Any additional worker state returned from ``start_worker``.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.

        Returns
        -------
        running : bool
            Whether the worker is running.
        msg : str, optional
            If not running, an optional message describing the exit condition.
        """
        raise NotImplementedError

    def on_worker_running(self, worker_name, worker_state, cluster_state):
        """Called when a worker is marked as running.

        Optional callback, useful for cluster managers that track worker state
        in background tasks.

        Parameters
        ---------
        worker_name : str
            The worker name.
        worker_state : dict
            Any additional worker state returned from ``start_worker``.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
        """
        pass

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        """Remove a worker.

        Parameters
        ---------
        worker_name : str
            The worker name.
        worker_state : dict
            Any additional worker state returned from ``start_worker``.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
        """
        raise NotImplementedError
