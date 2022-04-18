import asyncio
import atexit
import enum
import logging
import os
import ssl
import sys
import tempfile
import traceback
import warnings
import weakref
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
import dask
import yarl
from dask.utils import format_bytes
from distributed import Client
from distributed.core import rpc
from distributed.security import Security
from distributed.utils import LoopRunner

# Register gateway protocol
from . import comm
from .auth import get_auth
from .options import Options
from .utils import cancel_task, format_template

del comm

__all__ = ("Gateway", "GatewayCluster", "GatewayClusterError", "GatewayServerError")


logger = logging.getLogger("distributed.client")


class GatewayClusterError(Exception):
    """Exception related to starting/stopping/scaling of a gateway cluster"""


class GatewayServerError(Exception):
    """Exception related to the operation of the gateway server.

    Indicates an internal error in the gateway server.
    """


class GatewayWarning(UserWarning):
    """Warnings raised by the Gateway client"""


class GatewaySecurity(Security):
    """A security implementation that temporarily stores credentials on disk.

    The normal ``Security`` class assumes credentials already exist on disk,
    but our credentials exist only in memory. Since Python's SSLContext doesn't
    support directly loading credentials from memory, we write them temporarily
    to disk when creating the context, then delete them immediately."""

    def __init__(self, tls_key, tls_cert):
        self.tls_key = tls_key
        self.tls_cert = tls_cert

    def __repr__(self):
        return "GatewaySecurity<...>"

    def get_connection_args(self, role):
        with tempfile.TemporaryDirectory() as tempdir:
            key_path = os.path.join(tempdir, "dask.pem")
            cert_path = os.path.join(tempdir, "dask.crt")
            with open(key_path, "w") as f:
                f.write(self.tls_key)
            with open(cert_path, "w") as f:
                f.write(self.tls_cert)
            ctx = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH, cafile=cert_path
            )
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = False
            ctx.load_cert_chain(cert_path, key_path)
        return {"ssl_context": ctx, "require_encryption": True}


class _IntEnum(enum.IntEnum):
    @classmethod
    def _create(cls, x):
        return x if isinstance(x, cls) else cls.from_name(x)

    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError(f"{name!r} is not a valid {cls.__name__}")


class ClusterStatus(_IntEnum):
    """The cluster's status.

    Attributes
    ----------
    PENDING : ClusterStatus
        The cluster is pending start.
    RUNNING : ClusterStatus
        The cluster is running.
    STOPPING : ClusterStatus
        The cluster is stopping, but not fully stopped.
    STOPPED : ClusterStatus
        The cluster was shutdown by user or admin request.
    FAILED : ClusterStatus
        A failure occurred during any of the above states, see the logs for
        more information.
    """

    PENDING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    FAILED = 5


class ClusterReport:
    """A report on a cluster's state.

    Attributes
    ----------
    name : str
        The cluster's name, a unique id.
    options : dict
        User provided configuration for this cluster.
    status : ClusterStatus
        The cluster's status.
    scheduler_address : str or None
        The address of the scheduler, or None if not currently running.
    dashboard_link : str or None
        A link to the dashboard, or None if not currently running.
    start_time : datetime.datetime
        The time the cluster was started.
    stop_time : datetime.datetime or None
        The time the cluster was stopped, or None if currently running.
    tls_cert : str or None
        The tls certificate. None if not currently running or a full report
        wasn't requested.
    tls_key : str or None
        The tls key. None if not currently running or a full report wasn't
        requested.
    """

    __slots__ = (
        "name",
        "options",
        "status",
        "scheduler_address",
        "dashboard_link",
        "start_time",
        "stop_time",
        "tls_cert",
        "tls_key",
    )

    def __init__(
        self,
        name,
        options,
        status,
        scheduler_address,
        dashboard_link,
        start_time,
        stop_time,
        tls_cert=None,
        tls_key=None,
    ):
        self.name = name
        self.options = options
        self.status = status
        self.scheduler_address = scheduler_address
        self.dashboard_link = dashboard_link
        self.start_time = start_time
        self.stop_time = stop_time
        self.tls_cert = tls_cert
        self.tls_key = tls_key

    def __repr__(self):
        return f"ClusterReport<name={self.name}, status={self.status.name}>"

    @property
    def security(self):
        """A security object for this cluster, if present, else None"""
        return (
            None
            if self.tls_key is None
            else GatewaySecurity(tls_key=self.tls_key, tls_cert=self.tls_cert)
        )

    @classmethod
    def _from_json(cls, public_address, proxy_address, msg):
        start_time = datetime.fromtimestamp(msg.pop("start_time") / 1000)
        stop_time = msg.pop("stop_time")
        if stop_time is not None:
            stop_time = datetime.fromtimestamp(stop_time / 1000)

        name = msg.pop("name")
        status = ClusterStatus._create(msg.pop("status"))
        scheduler_address = (
            f"{proxy_address}/{name}" if status == ClusterStatus.RUNNING else None
        )

        dashboard_route = msg.pop("dashboard_route")
        dashboard_link = (
            f"{public_address}{dashboard_route}" if dashboard_route else None
        )

        return cls(
            start_time=start_time,
            stop_time=stop_time,
            name=name,
            status=status,
            scheduler_address=scheduler_address,
            dashboard_link=dashboard_link,
            **msg,
        )


def _get_default_request_kwargs(scheme):
    proxy = proxy_auth = None

    http_proxy = format_template(dask.config.get("gateway.http-client.proxy"))
    if http_proxy is True:
        proxies = aiohttp.helpers.proxies_from_env()
        info = proxies.get(scheme)
        if info is not None:
            proxy = info.proxy
            proxy_auth = info.proxy_auth
    elif isinstance(http_proxy, str):
        url = yarl.URL(http_proxy)
        proxy, proxy_auth = aiohttp.helpers.strip_auth_from_url(url)

    return {"proxy": proxy, "proxy_auth": proxy_auth}


class Gateway:
    """A client for a Dask Gateway Server.

    Parameters
    ----------
    address : str, optional
        The address to the gateway server.
    proxy_address : str, int, optional
        The address of the scheduler proxy server. Defaults to `address` if not
        provided. If an int, it's used as the port, with the host/ip taken from
        ``address``. Provide a full address if a different host/ip should be
        used.
    public_address : str, optional
        The address to the gateway server, as accessible from a web browser.
        This will be used as the root of all browser-facing links (e.g. the
        dask dashboard).  Defaults to ``address`` if not provided.
    auth : GatewayAuth, optional
        The authentication method to use.
    asynchronous : bool, optional
        If true, starts the client in asynchronous mode, where it can be used
        in other async code.
    loop : IOLoop, optional
        The IOLoop instance to use. Defaults to the current loop in
        asynchronous mode, otherwise a background loop is started.
    """

    def __init__(
        self,
        address=None,
        proxy_address=None,
        public_address=None,
        auth=None,
        asynchronous=False,
        loop=None,
    ):
        if address is None:
            address = format_template(dask.config.get("gateway.address"))
        if address is None:
            raise ValueError(
                "No dask-gateway address provided or found in configuration"
            )
        address = address.rstrip("/")

        if public_address is None:
            public_address = format_template(dask.config.get("gateway.public-address"))
        if public_address is None:
            public_address = address
        else:
            public_address = public_address.rstrip("/")

        if proxy_address is None:
            proxy_address = format_template(dask.config.get("gateway.proxy-address"))
        if proxy_address is None:
            parsed = urlparse(address)
            if parsed.netloc:
                if parsed.port is None:
                    proxy_port = {"http": 80, "https": 443}.get(parsed.scheme, 8786)
                else:
                    proxy_port = parsed.port
                proxy_netloc = "%s:%d" % (parsed.hostname, proxy_port)
            else:
                proxy_netloc = proxy_address
        elif isinstance(proxy_address, int):
            parsed = urlparse(address)
            proxy_netloc = "%s:%d" % (parsed.hostname, proxy_address)
        elif isinstance(proxy_address, str):
            parsed = urlparse(proxy_address)
            proxy_netloc = parsed.netloc if parsed.netloc else proxy_address
        proxy_address = "gateway://%s" % proxy_netloc

        scheme = urlparse(address).scheme
        self._request_kwargs = _get_default_request_kwargs(scheme)

        self.address = address
        self._public_address = public_address
        self.proxy_address = proxy_address

        self.auth = get_auth(auth)
        self._session = None

        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self._loop_runner.start()

    @property
    def loop(self):
        return self._loop_runner.loop

    @property
    def asynchronous(self):
        return self._asynchronous

    def sync(self, func, *args, **kwargs):
        if self.asynchronous:
            return func(*args, **kwargs)
        else:
            future = asyncio.run_coroutine_threadsafe(
                func(*args, **kwargs), self.loop.asyncio_loop
            )
            try:
                return future.result()
            except BaseException:
                future.cancel()
                raise

    def close(self):
        """Close the gateway client"""
        if self.asynchronous:
            return self._cleanup()
        elif self.loop.asyncio_loop.is_running():
            self.sync(self._cleanup)
        self._loop_runner.stop()

    async def _cleanup(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        await self._cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        if hasattr(self, "asynchronous") and (
            not self.asynchronous
            and hasattr(self, "_loop_runner")
            and not sys.is_finalizing()
        ):
            self.close()

    def __repr__(self):
        return "Gateway<%s>" % self.address

    async def _request(self, method, url, json=None):
        if self._session is None:
            # "unsafe" allows cookies to be set for ip addresses, which can
            # commonly serve dask-gateway deployments. Since this client is
            # only ever used with a single endpoint, there is no danger of
            # leaking cookies to a different server that happens to have the
            # same ip.
            self._session = aiohttp.ClientSession(
                cookie_jar=aiohttp.CookieJar(unsafe=True)
            )
        session = self._session

        resp = await session.request(method, url, json=json, **self._request_kwargs)

        if resp.status == 401:
            headers, context = self.auth.pre_request(resp)
            resp = await session.request(
                method, url, json=json, headers=headers, **self._request_kwargs
            )
            self.auth.post_response(resp, context)

        if resp.status >= 400:
            try:
                msg = await resp.json()
                msg = msg["error"]
            except Exception:
                msg = await resp.text()

            if resp.status in {404, 422}:
                raise ValueError(msg)
            elif resp.status == 409:
                raise GatewayClusterError(msg)
            elif resp.status == 500:
                raise GatewayServerError(msg)
            else:
                resp.raise_for_status()
        else:
            return resp

    async def _clusters(self, status=None):
        if status is not None:
            if isinstance(status, (str, ClusterStatus)):
                status = [ClusterStatus._create(status)]
            else:
                status = [ClusterStatus._create(s) for s in status]
            query = "?status=" + ",".join(s.name for s in status)
        else:
            query = ""

        url = f"{self.address}/api/v1/clusters/{query}"
        resp = await self._request("GET", url)
        data = await resp.json()
        return [
            ClusterReport._from_json(self._public_address, self.proxy_address, r)
            for r in data.values()
        ]

    def list_clusters(self, status=None, **kwargs):
        """List clusters for this user.

        Parameters
        ----------
        status : ClusterStatus, str, or list, optional
            The cluster status (or statuses) to select. Valid options are
            'pending', 'running', 'stopping', 'stopped', 'failed'.
            By default selects active clusters ('pending', 'running').

        Returns
        -------
        clusters : list of ClusterReport
        """
        return self.sync(self._clusters, status=status, **kwargs)

    def get_cluster(self, cluster_name, **kwargs):
        """Get information about a specific cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.

        Returns
        -------
        report : ClusterReport
        """
        return self.sync(self._cluster_report, cluster_name, **kwargs)

    async def _get_versions(self):
        url = "%s/api/version" % self.address
        resp = await self._request("GET", url)
        server_info = await resp.json()
        from . import __version__

        return {
            "server": server_info,
            "client": {"version": __version__},
        }

    def get_versions(self):
        """Return version info for the server and client

        Returns
        -------
        version_info : dict
        """
        return self.sync(self._get_versions)

    def _config_cluster_options(self):
        opts = dask.config.get("gateway.cluster.options")
        return {k: format_template(v) for k, v in opts.items()}

    async def _cluster_options(self, use_local_defaults=True):
        url = "%s/api/v1/options" % self.address
        resp = await self._request("GET", url)
        data = await resp.json()
        options = Options._from_spec(data["cluster_options"])
        if use_local_defaults:
            options.update(self._config_cluster_options())
        return options

    def cluster_options(self, use_local_defaults=True, **kwargs):
        """Get the available cluster configuration options.

        Parameters
        ----------
        use_local_defaults : bool, optional
            Whether to use any default options from the local configuration.
            Default is True, set to False to use only the server-side defaults.

        Returns
        -------
        cluster_options : dask_gateway.options.Options
            A dict of cluster options.
        """
        return self.sync(
            self._cluster_options, use_local_defaults=use_local_defaults, **kwargs
        )

    async def _submit(self, cluster_options=None, **kwargs):
        url = "%s/api/v1/clusters/" % self.address
        if cluster_options is not None:
            if not isinstance(cluster_options, Options):
                raise TypeError(
                    "cluster_options must be an `Options`, got %r"
                    % type(cluster_options).__name__
                )
            options = dict(cluster_options)
            options.update(kwargs)
        else:
            options = self._config_cluster_options()
            options.update(kwargs)
        resp = await self._request("POST", url, json={"cluster_options": options})
        data = await resp.json()
        return data["name"]

    def submit(self, cluster_options=None, **kwargs):
        """Submit a new cluster to be started.

        This returns quickly with a ``cluster_name``, which can later be used
        to connect to the cluster.

        Parameters
        ----------
        cluster_options : dask_gateway.options.Options, optional
            An ``Options`` object describing the desired cluster configuration.
        **kwargs :
            Additional cluster configuration options. If ``cluster_options`` is
            provided, these are applied afterwards as overrides. Available
            options are specific to each deployment of dask-gateway, see
            ``cluster_options`` for more information.

        Returns
        -------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._submit, cluster_options=cluster_options, **kwargs)

    async def _cluster_report(self, cluster_name, wait=False):
        params = "?wait" if wait else ""
        url = f"{self.address}/api/v1/clusters/{cluster_name}{params}"
        resp = await self._request("GET", url)
        data = await resp.json()
        return ClusterReport._from_json(self._public_address, self.proxy_address, data)

    async def _wait_for_start(self, cluster_name):
        while True:
            try:
                report = await self._cluster_report(cluster_name, wait=True)
            except TimeoutError:
                # Timeout, ignore
                pass
            else:
                if report.status is ClusterStatus.RUNNING:
                    return report
                elif report.status is ClusterStatus.FAILED:
                    raise GatewayClusterError(
                        "Cluster %r failed to start, see logs for "
                        "more information" % cluster_name
                    )
                elif report.status is ClusterStatus.STOPPED:
                    raise GatewayClusterError(
                        "Cluster %r is already stopped" % cluster_name
                    )
            # Not started yet, try again later
            await asyncio.sleep(0.5)

    def connect(self, cluster_name, shutdown_on_close=False):
        """Connect to a submitted cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster to connect to.
        shutdown_on_close : bool, optional
            If True, the cluster will be automatically shutdown on close.
            Default is False.

        Returns
        -------
        cluster : GatewayCluster
        """
        return GatewayCluster.from_name(
            cluster_name,
            shutdown_on_close=shutdown_on_close,
            address=self.address,
            proxy_address=self.proxy_address,
            auth=self.auth,
            asynchronous=self.asynchronous,
            loop=self.loop,
        )

    def new_cluster(self, cluster_options=None, shutdown_on_close=True, **kwargs):
        """Submit a new cluster to the gateway, and wait for it to be started.

        Same as calling ``submit`` and ``connect`` in one go.

        Parameters
        ----------
        cluster_options : dask_gateway.options.Options, optional
            An ``Options`` object describing the desired cluster configuration.
        shutdown_on_close : bool, optional
            If True (default), the cluster will be automatically shutdown on
            close. Set to False to have cluster persist until explicitly
            shutdown.
        **kwargs :
            Additional cluster configuration options. If ``cluster_options`` is
            provided, these are applied afterwards as overrides. Available
            options are specific to each deployment of dask-gateway, see
            ``cluster_options`` for more information.

        Returns
        -------
        cluster : GatewayCluster
        """
        return GatewayCluster(
            address=self.address,
            proxy_address=self.proxy_address,
            public_address=self._public_address,
            auth=self.auth,
            asynchronous=self.asynchronous,
            loop=self.loop,
            cluster_options=cluster_options,
            shutdown_on_close=shutdown_on_close,
            **kwargs,
        )

    async def _stop_cluster(self, cluster_name):
        url = f"{self.address}/api/v1/clusters/{cluster_name}"
        await self._request("DELETE", url)

    def stop_cluster(self, cluster_name, **kwargs):
        """Stop a cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._stop_cluster, cluster_name, **kwargs)

    async def _scale_cluster(self, cluster_name, n):
        url = f"{self.address}/api/v1/clusters/{cluster_name}/scale"
        resp = await self._request("POST", url, json={"count": n})
        try:
            msg = await resp.json()
        except Exception:
            msg = {}
        if not msg.get("ok", True) and msg.get("msg"):
            warnings.warn(GatewayWarning(msg["msg"]))

    def scale_cluster(self, cluster_name, n, **kwargs):
        """Scale a cluster to n workers.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
        return self.sync(self._scale_cluster, cluster_name, n, **kwargs)

    async def _adapt_cluster(
        self, cluster_name, minimum=None, maximum=None, active=True
    ):
        resp = await self._request(
            "POST",
            f"{self.address}/api/v1/clusters/{cluster_name}/adapt",
            json={"minimum": minimum, "maximum": maximum, "active": active},
        )
        try:
            msg = await resp.json()
        except Exception:
            msg = {}
        if not msg.get("ok", True) and msg.get("msg"):
            warnings.warn(GatewayWarning(msg["msg"]))

    def adapt_cluster(
        self, cluster_name, minimum=None, maximum=None, active=True, **kwargs
    ):
        """Configure adaptive scaling for a cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        minimum : int, optional
            The minimum number of workers to scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to scale to. Defaults to infinity.
        active : bool, optional
            If ``True`` (default), adaptive scaling is activated. Set to
            ``False`` to deactivate adaptive scaling.
        """
        return self.sync(
            self._adapt_cluster,
            cluster_name,
            minimum=minimum,
            maximum=maximum,
            active=active,
            **kwargs,
        )


_widget_status_template = """
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table style="text-align: right;">
    <tr><th>Workers</th> <td>%d</td></tr>
    <tr><th>Threads</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
</table>
</div>
"""


@atexit.register
def cleanup_lingering_clusters():
    for o in list(GatewayCluster._instances):
        try:
            if not o.asynchronous:
                o.close()
        except Exception as exc:
            lines = ["Exception ignored when closing %r:\n" % o]
            lines.extend(traceback.format_exception(type(exc), exc, exc.__traceback__))
            warnings.warn("".join(lines))


class GatewayCluster:
    """A dask-gateway cluster.

    Parameters
    ----------
    address : str, optional
        The address to the gateway server.
    proxy_address : str, int, optional
        The address of the scheduler proxy server. If an int, it's used as the
        port, with the host/ip taken from ``address``. Provide a full address
        if a different host/ip should be used.
    public_address : str, optional
        The address to the gateway server, as accessible from a web browser.
        This will be used as the root of all browser-facing links (e.g. the
        dask dashboard).  Defaults to ``address`` if not provided.
    auth : GatewayAuth, optional
        The authentication method to use.
    cluster_options : mapping, optional
        A mapping of cluster options to use to start the cluster.
    shutdown_on_close : bool, optional
        If True (default), the cluster will be automatically shutdown on
        close.
    asynchronous : bool, optional
        If true, starts the cluster in asynchronous mode, where it can be used
        in other async code.
    loop : IOLoop, optional
        The IOLoop instance to use. Defaults to the current loop in
        asynchronous mode, otherwise a background loop is started.
    **kwargs :
        Additional cluster configuration options. If ``cluster_options`` is
        provided, these are applied afterwards as overrides. Available options
        are specific to each deployment of dask-gateway, see
        ``Gateway.cluster_options`` for more information.
    """

    _instances = weakref.WeakSet()

    def __init__(
        self,
        address=None,
        proxy_address=None,
        public_address=None,
        auth=None,
        cluster_options=None,
        shutdown_on_close=True,
        asynchronous=False,
        loop=None,
        **kwargs,
    ):
        self._init_internal(
            address=address,
            proxy_address=proxy_address,
            public_address=public_address,
            auth=auth,
            cluster_options=cluster_options,
            cluster_kwargs=kwargs,
            shutdown_on_close=shutdown_on_close,
            asynchronous=asynchronous,
            loop=loop,
        )

    @classmethod
    def from_name(
        cls,
        cluster_name,
        shutdown_on_close=False,
        address=None,
        proxy_address=None,
        public_address=None,
        auth=None,
        asynchronous=False,
        loop=None,
    ):
        """Connect to a submitted cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster to connect to.
        shutdown_on_close : bool, optional
            If True, the cluster will be automatically shutdown on close.
            Default is False.
        **kwargs :
            Additional parameters to pass to the ``GatewayCluster``
            constructor. See the docstring for more information.

        Returns
        -------
        cluster : GatewayCluster
        """
        self = object.__new__(cls)
        self._init_internal(
            address=address,
            proxy_address=proxy_address,
            public_address=public_address,
            auth=auth,
            asynchronous=asynchronous,
            loop=loop,
            shutdown_on_close=shutdown_on_close,
            name=cluster_name,
        )
        return self

    def _init_internal(
        self,
        address=None,
        proxy_address=None,
        public_address=None,
        auth=None,
        cluster_options=None,
        cluster_kwargs=None,
        shutdown_on_close=None,
        asynchronous=False,
        loop=None,
        name=None,
    ):
        self.status = "created"

        self.shutdown_on_close = shutdown_on_close

        self._instances.add(self)

        self.gateway = Gateway(
            address=address,
            proxy_address=proxy_address,
            public_address=public_address,
            auth=auth,
            asynchronous=asynchronous,
            loop=loop,
        )

        # Internals
        self.scheduler_info = {}
        self.scheduler_comm = None
        self._watch_worker_status_task = None

        # XXX: Ensure client is closed. Currently disconnected clients try to
        # reconnect forever, which spams the schedulerproxy. Once this bug is
        # fixed, we should be able to remove this.
        self._clients = weakref.WeakSet()

        self._cluster_options = cluster_options
        self._cluster_kwargs = cluster_kwargs or {}
        self._start_task = None
        self._stop_task = None

        self.name = name
        self.scheduler_address = None
        self.dashboard_link = None
        self.security = None

        if name is not None:
            self.status = "starting"
        if not self.asynchronous:
            self.gateway.sync(self._start_internal)

    @property
    def loop(self):
        return self.gateway.loop

    @property
    def asynchronous(self):
        return self.gateway.asynchronous

    async def _start_internal(self):
        if self._start_task is None:
            self._start_task = asyncio.ensure_future(self._start_async())
        try:
            await self._start_task
        except BaseException:
            # On exception, cleanup
            await self._stop_internal()
            raise
        return self

    async def _start_async(self):
        if self.status in ("closing", "closed"):
            return
        # Start cluster if not already started
        if self.status == "created":
            self.status = "starting"
            self.name = await self.gateway._submit(
                cluster_options=self._cluster_options, **self._cluster_kwargs
            )
        # Connect to cluster
        try:
            report = await self.gateway._wait_for_start(self.name)
        except GatewayClusterError:
            raise
        self.scheduler_address = report.scheduler_address
        self.dashboard_link = report.dashboard_link
        self.security = report.security
        # Initialize scheduler comms and other internals
        await self._init_internals()
        self.status = "running"

    async def _init_internals(self):
        self.scheduler_comm = rpc(
            self.scheduler_address,
            connection_args=self.security.get_connection_args("client"),
        )
        comm = None
        try:
            comm = await self.scheduler_comm.live_comm()
            await comm.write({"op": "subscribe_worker_status"})
            self.scheduler_info = await comm.read()
            self._watch_worker_status_task = asyncio.ensure_future(
                self._watch_worker_status(comm)
            )
        except Exception:
            if comm is not None:
                await comm.close()

    async def _stop_internal(self, shutdown=None):
        self._instances.discard(self)
        if self._stop_task is None:
            self._stop_task = asyncio.ensure_future(self._stop_async())
        try:
            await self._stop_task
        except Exception:
            logger.warning("Exception raised while closing clients", exc_info=True)

        if shutdown is None:
            shutdown = self.shutdown_on_close

        if shutdown and self.name is not None:
            try:
                await self.gateway._stop_cluster(self.name)
            except Exception:
                logger.error(
                    "Exception raised while shutting down cluster %s",
                    self.name,
                    exc_info=True,
                )

        await self.gateway._cleanup()

    async def _stop_async(self):
        if self._start_task is not None:
            if not self._start_task.done():
                # We're still starting, cancel task
                await cancel_task(self._start_task)

        self.status = "closing"

        if self._watch_worker_status_task is not None:
            await cancel_task(self._watch_worker_status_task)
        if self.scheduler_comm is not None:
            self.scheduler_comm.close_rpc()
        for client in list(self._clients):
            await client._close()
            self._clients.discard(client)

        self.status = "closed"

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        return await self._start_internal()

    async def __aexit__(self, typ, value, traceback):
        await self._stop_internal()

    def close(self, shutdown=None):
        """Close the cluster object.

        Parameters
        ----------
        shutdown : bool, optional
            Whether to shutdown the cluster. If True, the cluster will be
            shutdown, if False only the local state will be cleaned up.
            Defaults to the value of ``shutdown_on_close``, set to True or
            False to override.
        """
        if self.asynchronous:
            return self._stop_internal(shutdown=shutdown)
        if self.status == "closed":
            return
        if self.loop.asyncio_loop.is_running():
            self.gateway.sync(self._stop_internal, shutdown=shutdown)
        self.gateway.close()

    def shutdown(self):
        """Shutdown this cluster. Alias for ``close(shutdown=True)``."""
        return self.close(shutdown=True)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if not self.asynchronous:
            self.close()

    def __del__(self):
        if not hasattr(self, "gateway"):
            return
        if self.asynchronous:
            # No del for async mode
            return
        if not sys.is_finalizing():
            self.close()

    def __repr__(self):
        return f"GatewayCluster<{self.name}, status={self.status}>"

    def get_client(self, set_as_default=True):
        """Get a ``Client`` for this cluster.

        Returns
        -------
        client : dask.distributed.Client
        """
        client = Client(
            self,
            security=self.security,
            set_as_default=set_as_default,
            asynchronous=self.asynchronous,
            loop=self.loop,
        )
        if not self.asynchronous:
            self._clients.add(client)
        return client

    def scale(self, n, **kwargs):
        """Scale the cluster to ``n`` workers.

        Parameters
        ----------
        n : int
            The number of workers to scale to.
        """
        return self.gateway.scale_cluster(self.name, n, **kwargs)

    def adapt(self, minimum=None, maximum=None, active=True, **kwargs):
        """Configure adaptive scaling for the cluster.

        Parameters
        ----------
        minimum : int, optional
            The minimum number of workers to scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to scale to. Defaults to infinity.
        active : bool, optional
            If ``True`` (default), adaptive scaling is activated. Set to
            ``False`` to deactivate adaptive scaling.
        """
        return self.gateway.adapt_cluster(
            self.name, minimum=minimum, maximum=maximum, active=active, **kwargs
        )

    async def _watch_worker_status(self, comm):
        # We don't want to hold on to a ref to self, otherwise this will
        # leave a dangling reference and prevent garbage collection.
        ref_self = weakref.ref(self)
        self = None
        try:
            while True:
                try:
                    msgs = await comm.read()
                except OSError:
                    break
                try:
                    self = ref_self()
                    if self is None:
                        break
                    for op, msg in msgs:
                        if op == "add":
                            workers = msg.pop("workers")
                            self.scheduler_info["workers"].update(workers)
                            self.scheduler_info.update(msg)
                        elif op == "remove":
                            del self.scheduler_info["workers"][msg]
                    if hasattr(self, "_status_widget"):
                        self._status_widget.value = self._widget_status()
                finally:
                    self = None
        finally:
            await comm.close()

    def _widget_status(self):
        try:
            workers = self.scheduler_info["workers"]
        except KeyError:
            return None
        else:
            n_workers = len(workers)
            threads = sum(w["nthreads"] for w in workers.values())
            memory = sum(w["memory_limit"] for w in workers.values())

            return _widget_status_template % (n_workers, threads, format_bytes(memory))

    def _widget(self):
        """Create IPython widget for display within a notebook"""
        try:
            return self._cached_widget
        except AttributeError:
            pass

        if self.asynchronous:
            return None

        try:
            from ipywidgets import HTML, Accordion, Button, HBox, IntText, Layout, VBox
        except ImportError:
            self._cached_widget = None
            return None

        layout = Layout(width="150px")

        title = HTML("<h2>GatewayCluster</h2>")

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        request = IntText(0, description="Workers", layout=layout)
        scale = Button(description="Scale", layout=layout)

        minimum = IntText(0, description="Minimum", layout=layout)
        maximum = IntText(0, description="Maximum", layout=layout)
        adapt = Button(description="Adapt", layout=layout)

        accordion = Accordion(
            [HBox([request, scale]), HBox([minimum, maximum, adapt])],
            layout=Layout(min_width="500px"),
        )
        accordion.selected_index = None
        accordion.set_title(0, "Manual Scaling")
        accordion.set_title(1, "Adaptive Scaling")

        @scale.on_click
        def scale_cb(b):
            try:
                self.scale(request.value)
            except Exception as e:
                logger.exception(e)

        @adapt.on_click
        def adapt_cb(b):
            self.adapt(minimum=minimum.value, maximum=maximum.value)

        name = HTML(f"<p><b>Name: </b>{self.name}</p>")

        elements = [title, HBox([status, accordion]), name]

        if self.dashboard_link is not None:
            link = HTML(
                '<p><b>Dashboard: </b><a href="{0}" target="_blank">{0}'
                "</a></p>\n".format(self.dashboard_link)
            )
            elements.append(link)

        self._cached_widget = box = VBox(elements)
        self._status_widget = status

        return box

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        else:
            from IPython.display import display

            data = {"text/plain": repr(self), "text/html": self._repr_html_()}
            display(data, raw=True)

    def _repr_html_(self):
        if self.dashboard_link is not None:
            dashboard = "<a href='{0}' target='_blank'>{0}</a>".format(
                self.dashboard_link
            )
        else:
            dashboard = "Not Available"
        return (
            "<div style='background-color: #f2f2f2; display: inline-block; "
            "padding: 10px; border: 1px solid #999999;'>\n"
            "  <h3>GatewayCluster</h3>\n"
            "  <ul>\n"
            "    <li><b>Name: </b>{name}\n"
            "    <li><b>Dashboard: </b>{dashboard}\n"
            "  </ul>\n"
            "</div>\n"
        ).format(name=self.name, dashboard=dashboard)
