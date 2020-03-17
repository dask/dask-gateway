import asyncio

import aiohttp
from traitlets import Instance, Integer, Float, Dict, Union, Unicode, default
from traitlets.config import LoggingConfigurable, Configurable

from .. import models
from ..options import Options
from ..traitlets import MemoryLimit, Type, Callable
from ..utils import awaitable


__all__ = ("Backend", "ClusterConfig")


class PublicException(Exception):
    """An exception that can be reported to the user"""

    pass


class Backend(LoggingConfigurable):
    """Base class for defining dask-gateway backends.

    Subclasses should implement the following methods:

    - ``setup``
    - ``cleanup``
    - ``start_cluster``
    - ``stop_cluster``
    - ``on_cluster_heartbeat``
    """

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

    scheduler_api_retries = Integer(
        3,
        min=0,
        help="""
        The number of attempts to make when contacting a scheduler api endpoint.

        If failures occur after the max number of retries, the dask cluster will
        be marked as failed and will be cleaned up.
        """,
    )

    api_url = Unicode(
        help="""
        The address that internal components (e.g. dask clusters)
        will use when contacting the gateway.
        """,
        config=True,
    )

    # Forwarded from the main application
    gateway_address = Unicode()

    @default("gateway_address")
    def _gateway_address_default(self):
        return self.parent.address

    async def get_cluster_options(self, user):
        if callable(self.cluster_options):
            return await awaitable(self.cluster_options(user))
        return self.cluster_options

    async def process_cluster_options(self, user, request):
        try:
            cluster_options = await self.get_cluster_options(user)
            requested_options = cluster_options.parse_options(request)
            overrides = cluster_options.get_configuration(requested_options)
            config = self.cluster_config_class(parent=self, **overrides)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PublicException(str(exc))
        return requested_options, config

    async def forward_message_to_scheduler(self, cluster, msg):
        if cluster.status != models.ClusterStatus.RUNNING:
            raise PublicException(f"cluster {cluster.name} is not running")
        attempt = 1
        t = 0.1
        while True:
            try:
                await self.session.post(
                    cluster.api_address + "/api/comm",
                    json=msg,
                    headers={"Authorization": "token %s" % cluster.token},
                    raise_for_status=True,
                )
                return
            except Exception:
                if attempt < self.scheduler_api_retries:
                    self.log.warning(
                        f"Failed to message cluster {cluster.name} on attempt "
                        f"{attempt}, retrying in {t} s",
                        exc_info=True,
                    )
                    await asyncio.sleep(t)
                    attempt += 1
                    t = min(t * 2, 5)
                else:
                    break
        self.log.warning(
            f"Failed to message cluster {cluster.name} on attempt "
            f"{attempt}, marking cluster as failed"
        )
        await self.stop_cluster(cluster.name, failed=True)
        raise PublicException(f"cluster {cluster.name} is not running")

    async def setup(self, app):
        """Called when the server is starting up.

        Do any initialization here.

        Parameters
        ----------
        app : aiohttp.web.Application
            The aiohttp application. Can be used to add additional routes if
            needed.
        """
        self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Called when the server is shutting down.

        Do any cleanup tasks in this method"""
        await self.session.close()

    async def list_clusters(self, username=None, statuses=None):
        """List known clusters.

        Parameters
        ----------
        username : str, optional
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

    async def get_cluster(self, cluster_name, wait=False):
        """Get information about a cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        wait : bool, optional
            If True, wait until the cluster has started or failed before
            returning. If waiting is not possible (or waiting for a long period
            of time would be expensive) it is valid to return early with a
            Cluster object in a state prior to RUNNING (the client will retry
            in this case). Default is False.

        Returns
        -------
        cluster : Cluster
        """
        raise NotImplementedError

    async def start_cluster(self, user, cluster_options):
        """Submit a new cluster.

        Parameters
        ----------
        user : User
            The user making the request.
        cluster_options : dict
            Any additional options provided by the user.

        Returns
        -------
        cluster_name : str
        """
        raise NotImplementedError

    async def stop_cluster(self, cluster_name, failed=False):
        """Stop a cluster.

        No-op if the cluster is already stopped.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        failed : bool, optional
            If True, the cluster should be marked as FAILED after stopping. If
            False (default) it should be marked as STOPPED.
        """
        raise NotImplementedError

    async def on_cluster_heartbeat(self, cluster_name, msg):
        """Handle a cluster heartbeat.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        msg : dict
            The heartbeat message.
        """
        raise NotImplementedError


class ClusterConfig(Configurable):
    """Base class for holding individual Dask cluster configurations"""

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

    adaptive_period = Float(
        3,
        min=0,
        help="""
        Time (in seconds) between adaptive scaling checks.

        A smaller period will decrease scale up/down latency when responding to
        cluster load changes, but may also result in higher load on the gateway
        server.
        """,
        config=True,
    )

    idle_timeout = Float(
        0,
        min=0,
        help="""
        Time (in seconds) before an idle cluster is automatically shutdown.

        Set to 0 (default) for no idle timeout.
        """,
        config=True,
    )

    def to_dict(self):
        return {
            k: getattr(self, k)
            for k in self.trait_names()
            if k not in {"parent", "config"}
        }
