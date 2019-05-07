from traitlets import Unicode, Integer, Dict
from traitlets.config import LoggingConfigurable

from .utils import MemoryLimit


class ClusterManager(LoggingConfigurable):
    """Base class for dask cluster managers"""

    environment = Dict(
        help="""
        Environment variables to set for both the worker and scheduler processes.
        """,
        config=True
    )

    start_timeout = Integer(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask cluster.
        """
    )

    worker_memory = MemoryLimit(
        '2 G',
        help="""
        Maximum number of bytes a dask worker is allowed to use. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True
    )

    worker_cores = Integer(
        1,
        min=1,
        help="""
        Maximum number of cpu-cores a dask worker is allowed to use.
        """,
        config=True
    )

    scheduler_memory = MemoryLimit(
        '2 G',
        help="""
        Maximum number of bytes a dask scheduler is allowed to use. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True
    )

    scheduler_cores = Integer(
        1,
        min=1,
        help="""
        Maximum number of cpu-cores a dask scheduler is allowed to use.
        """,
        config=True
    )

    worker_cmd = Unicode(
        'dask-gateway-worker',
        help='Shell command to start a dask-gateway worker.',
        config=True
    )

    scheduler_cmd = Unicode(
        'dask-gateway-scheduler',
        help='Shell command to start a dask-gateway scheduler.',
        config=True
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
        out.update({'DASK_GATEWAY_API_URL': self.api_url,
                    'DASK_GATEWAY_CLUSTER_NAME': cluster_info.cluster_name,
                    'DASK_GATEWAY_API_TOKEN': cluster_info.api_token,
                    'DASK_GATEWAY_TLS_CERT': tls_cert_path,
                    'DASK_GATEWAY_TLS_KEY': tls_key_path})
        return out

    async def start_cluster(self, cluster_info):
        """Start a new cluster.

        This should do any initialization for the whole dask cluster
        application, and then kickoff starting up the scheduler. The scheduler
        does not need to have started before returning from this routine.

        Parameters
        ----------
        cluster_info : ClusterInfo
            Information about the cluster to be started.

        Returns
        -------
        cluster_state : dict
            Any state needed for further interactions with this cluster. This
            should be serializable using ``json.dumps``.
        """
        raise NotImplementedError

    async def is_cluster_running(self, cluster_info, cluster_state):
        """Check if the cluster is running.

        Returns ``True`` if running, ``False`` otherwise.

        Parameters
        ----------
        cluster_info : ClusterInfo
            Information about the cluster.
        cluster_state : dict
            Any additional state returned from ``start_cluster``.
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
            Any additional information about this worker needed to remove it in
            the future.
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
