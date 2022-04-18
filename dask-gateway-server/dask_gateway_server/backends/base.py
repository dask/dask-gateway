import asyncio

import aiohttp
from traitlets import (
    Dict,
    Float,
    Instance,
    Integer,
    Unicode,
    Union,
    default,
    observe,
    validate,
)
from traitlets.config import Configurable, LoggingConfigurable

from .. import models
from ..options import Options
from ..traitlets import Callable, Command, MemoryLimit, Type
from ..utils import awaitable, format_bytes

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
            overrides = cluster_options.get_configuration(requested_options, user)
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

    scheduler_cmd = Command(
        "dask-scheduler", help="Shell command to start a dask scheduler.", config=True
    )

    worker_cmd = Command(
        "dask-worker", help="Shell command to start a dask worker.", config=True
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

    # Number of threads per worker. Defaults to the number of cores
    worker_threads = Integer(
        help="""
        Number of threads available for a dask worker.

        Defaults to ``worker_cores``.
        """,
        min=1,
        config=True,
        allow_none=True,
    )

    @default("worker_threads")
    def _default_worker_threads(self):
        return max(int(self.worker_cores), 1)

    @validate("worker_threads")
    def _validate_worker_threads(self, proposal):
        if not proposal.value:
            return self._default_worker_threads()
        return proposal.value

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

    cluster_max_memory = MemoryLimit(
        None,
        help="""
        The maximum amount of memory (in bytes) available to this cluster.
        Allows the following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Set to ``None`` for no memory limit (default).
        """,
        min=0,
        allow_none=True,
        config=True,
    )

    cluster_max_cores = Float(
        None,
        help="""
        The maximum number of cores available to this cluster.

        Set to ``None`` for no cores limit (default).
        """,
        min=0.0,
        allow_none=True,
        config=True,
    )

    cluster_max_workers = Integer(
        help="""
        The maximum number of workers available to this cluster.

        Note that this will be combined with ``cluster_max_cores`` and
        ``cluster_max_memory`` at runtime to determine the actual maximum
        number of workers available to this cluster.
        """,
        allow_none=True,
        min=0,
        config=True,
    )

    def _check_scheduler_memory(self, scheduler_memory, cluster_max_memory):
        if cluster_max_memory is not None and scheduler_memory > cluster_max_memory:
            memory = format_bytes(scheduler_memory)
            limit = format_bytes(cluster_max_memory)
            raise ValueError(
                f"Scheduler memory request of {memory} exceeds cluster memory "
                f"limit of {limit}"
            )

    def _check_scheduler_cores(self, scheduler_cores, cluster_max_cores):
        if cluster_max_cores is not None and scheduler_cores > cluster_max_cores:
            raise ValueError(
                f"Scheduler cores request of {scheduler_cores} exceeds cluster "
                f"cores limit of {cluster_max_cores}"
            )

    def _worker_limit_from_resources(self):
        inf = max_workers = float("inf")
        if self.cluster_max_memory is not None:
            max_workers = min(
                (self.cluster_max_memory - self.scheduler_memory) // self.worker_memory,
                max_workers,
            )
        if self.cluster_max_cores is not None:
            max_workers = min(
                (self.cluster_max_cores - self.scheduler_cores) // self.worker_cores,
                max_workers,
            )

        if max_workers == inf:
            return None
        return max(0, int(max_workers))

    @validate("scheduler_memory")
    def _validate_scheduler_memory(self, proposal):
        self._check_scheduler_memory(proposal.value, self.cluster_max_memory)
        return proposal.value

    @validate("scheduler_cores")
    def _validate_scheduler_cores(self, proposal):
        self._check_scheduler_cores(proposal.value, self.cluster_max_cores)
        return proposal.value

    @validate("cluster_max_memory")
    def _validate_cluster_max_memory(self, proposal):
        self._check_scheduler_memory(self.scheduler_memory, proposal.value)
        return proposal.value

    @validate("cluster_max_cores")
    def _validate_cluster_max_cores(self, proposal):
        self._check_scheduler_cores(self.scheduler_cores, proposal.value)
        return proposal.value

    @validate("cluster_max_workers")
    def _validate_cluster_max_workers(self, proposal):
        lim = self._worker_limit_from_resources()
        if lim is None:
            return proposal.value
        if proposal.value is None:
            return lim
        return min(proposal.value, lim)

    @observe("cluster_max_workers")
    def _observe_cluster_max_workers(self, change):
        # This shouldn't be needed, but traitlet validators don't run
        # if a value is `None` and `allow_none` is true, so we need to
        # add an observer to handle the event of an *explicit* `None`
        # set for `cluster_max_workers`
        if change.new is None:
            lim = self._worker_limit_from_resources()
            if lim is not None:
                self.cluster_max_workers = lim

    @default("cluster_max_workers")
    def _default_cluster_max_workers(self):
        return self._worker_limit_from_resources()

    def to_dict(self):
        return {
            k: getattr(self, k)
            for k in self.trait_names()
            if k not in {"parent", "config"}
        }
