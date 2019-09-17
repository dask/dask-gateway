import enum
import json
import os
import ssl
import tempfile
import warnings
import weakref
from datetime import timedelta, datetime
from threading import get_ident
from urllib.parse import urlparse

import dask
from distributed import Client
from distributed.security import Security
from distributed.utils import LoopRunner, sync, thread_state, format_bytes, log_errors
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from tornado.httputil import HTTPHeaders

from .cookiejar import CookieJar

# Register gateway protocol
from . import comm
from .auth import get_auth
from .options import Options

del comm

__all__ = ("Gateway", "GatewayCluster", "GatewayClusterError", "GatewayServerError")


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
        raise ValueError("%r is not a valid %s" % (name, cls.__name__))


class ClusterStatus(_IntEnum):
    """The cluster's status.

    Attributes
    ----------
    STARTING : ClusterStatus
        The cluster is starting.
    STARTED : ClusterStatus
        The cluster has started, but hasn't connected to the gateway.
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

    STARTING = 1
    STARTED = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    FAILED = 6


class ClusterReport(object):
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
        return "ClusterReport<name=%s, status=%s>" % (self.name, self.status.name)

    @property
    def security(self):
        """A security object for this cluster, if present, else None"""
        return (
            None
            if self.tls_key is None
            else GatewaySecurity(tls_key=self.tls_key, tls_cert=self.tls_cert)
        )

    @classmethod
    def _from_json(cls, gateway_address, proxy_address, msg):
        start_time = datetime.fromtimestamp(msg.pop("start_time") / 1000)
        stop_time = msg.pop("stop_time")
        if stop_time is not None:
            stop_time = datetime.fromtimestamp(stop_time / 1000)

        name = msg.pop("name")
        status = ClusterStatus._create(msg.pop("status"))
        scheduler_address = (
            "%s/%s" % (proxy_address, name) if status == ClusterStatus.RUNNING else None
        )

        dashboard_route = msg.pop("dashboard_route")
        dashboard_link = (
            "%s%s" % (gateway_address, dashboard_route) if dashboard_route else None
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


class Gateway(object):
    """A client for a Dask Gateway Server.

    Parameters
    ----------
    address : str, optional
        The address to the gateway server.
    proxy_address : str, int, optional
        The address of the scheduler proxy server. If an int, it's used as the
        port, with the host/ip taken from ``address``. Provide a full address
        if a different host/ip should be used.
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
        self, address=None, proxy_address=None, auth=None, asynchronous=False, loop=None
    ):
        if address is None:
            address = dask.config.get("gateway.address")
        if address is None:
            raise ValueError(
                "No dask-gateway address provided or found in configuration"
            )
        address = address.rstrip("/")

        if proxy_address is None:
            proxy_address = dask.config.get("gateway.proxy-address")
        if proxy_address is None:
            raise ValueError(
                "No dask-gateway proxy address provided or found in configuration"
            )
        if isinstance(proxy_address, int):
            parsed = urlparse(address)
            proxy_netloc = "%s:%d" % (parsed.hostname, proxy_address)
        elif isinstance(proxy_address, str):
            parsed = urlparse(proxy_address)
            proxy_netloc = parsed.netloc if parsed.netloc else proxy_address
        proxy_address = "gateway://%s" % proxy_netloc

        self.address = address
        self.proxy_address = proxy_address

        self._auth = get_auth(auth)
        self._cookie_jar = CookieJar()

        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self._loop_runner.start()
        if self._asynchronous:
            self._started = self._start()
        else:
            self.sync(self._start)

    async def _start(self):
        self._http_client = AsyncHTTPClient()

    def close(self):
        """Close this gateway client"""
        if not self.asynchronous:
            self._loop_runner.stop()

    def __del__(self):
        # __del__ is still called, even if __init__ failed. Only close if init
        # actually succeeded.
        if hasattr(self, "_started"):
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __await__(self):
        return self._started.__await__()

    async def __aenter__(self):
        await self._started
        return self

    async def __aexit__(self, typ, value, traceback):
        pass

    def __repr__(self):
        return "Gateway<%s>" % self.address

    @property
    def asynchronous(self):
        return (
            self._asynchronous
            or getattr(thread_state, "asynchronous", False)
            or (
                hasattr(self.loop, "_thread_identity")
                and self.loop._thread_identity == get_ident()
            )
        )

    def sync(self, func, *args, **kwargs):
        if kwargs.pop("asynchronous", None) or self.asynchronous:
            callback_timeout = kwargs.pop("callback_timeout", None)
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = gen.with_timeout(timedelta(seconds=callback_timeout), future)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    async def _fetch(self, req):
        try:
            self._cookie_jar.pre_request(req)
            resp = await self._http_client.fetch(req, raise_error=False)
            if resp.code == 401:
                context = self._auth.pre_request(req, resp)
                resp = await self._http_client.fetch(req, raise_error=False)
                self._auth.post_response(req, resp, context)
            self._cookie_jar.post_response(resp)
            if resp.error:
                if resp.code == 599:
                    raise TimeoutError("Request timed out")
                else:
                    try:
                        msg = json.loads(resp.body)["error"]
                    except Exception:
                        msg = resp.body.decode()

                    if resp.code in {404, 422}:
                        raise ValueError(msg)
                    elif resp.code == 409:
                        raise GatewayClusterError(msg)
                    elif resp.code == 500:
                        raise GatewayServerError(msg)
                    else:
                        resp.rethrow()
        except HTTPError as exc:
            # Tornado 6 still raises these above with raise_error=False
            if exc.code == 599:
                raise TimeoutError("Request timed out")
            # Should never get here!
            raise
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

        url = "%s/gateway/api/clusters/%s" % (self.address, query)
        req = HTTPRequest(url=url)
        resp = await self._fetch(req)
        return [
            ClusterReport._from_json(self.address, self.proxy_address, r)
            for r in json.loads(resp.body).values()
        ]

    def list_clusters(self, status=None, **kwargs):
        """List clusters for this user.

        Parameters
        ----------
        status : ClusterStatus, str, or list, optional
            The cluster status (or statuses) to select. Valid options are
            'starting', 'started', 'running', 'stopping', 'stopped', 'failed'.
            By default selects active clusters ('starting', 'started',
            'running').

        Returns
        -------
        clusters : list of ClusterReport
        """
        return self.sync(self._clusters, status=status, **kwargs)

    async def _cluster_options(self):
        url = "%s/gateway/api/clusters/options" % self.address
        req = HTTPRequest(url=url, method="GET")
        resp = await self._fetch(req)
        data = json.loads(resp.body)
        return Options._from_spec(data["cluster_options"])

    def cluster_options(self, **kwargs):
        """Get the available cluster configuration options.

        Returns
        -------
        cluster_options : Options
            A dict of cluster options.
        """
        return self.sync(self._cluster_options, **kwargs)

    async def _submit(self, cluster_options=None, **kwargs):
        url = "%s/gateway/api/clusters/" % self.address
        if cluster_options is not None:
            if not isinstance(cluster_options, Options):
                raise TypeError(
                    "cluster_options must be an `Options`, got %r"
                    % type(cluster_options).__name__
                )
            options = dict(cluster_options)
            options.update(kwargs)
        else:
            options = kwargs
        req = HTTPRequest(
            url=url,
            method="POST",
            body=json.dumps({"cluster_options": options}),
            headers=HTTPHeaders({"Content-type": "application/json"}),
        )
        resp = await self._fetch(req)
        data = json.loads(resp.body)
        return data["name"]

    def submit(self, cluster_options=None, **kwargs):
        """Submit a new cluster to be started.

        This returns quickly with a ``cluster_name``, which can later be used
        to connect to the cluster.

        Parameters
        ----------
        cluster_options : Options, optional
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
        return self.sync(self._submit, **kwargs)

    async def _cluster_report(self, cluster_name, wait=False):
        params = "?wait" if wait else ""
        url = "%s/gateway/api/clusters/%s%s" % (self.address, cluster_name, params)
        req = HTTPRequest(url=url)
        resp = await self._fetch(req)
        return ClusterReport._from_json(
            self.address, self.proxy_address, json.loads(resp.body)
        )

    async def _connect(self, cluster_name):
        while True:
            try:
                report = await self._cluster_report(cluster_name, wait=True)
            except TimeoutError:
                # Timeout, ignore
                pass
            else:
                if report.status is ClusterStatus.RUNNING:
                    return GatewayCluster(
                        gateway=self,
                        name=report.name,
                        scheduler_address=report.scheduler_address,
                        dashboard_link=report.dashboard_link,
                        security=report.security,
                    )
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
            await gen.sleep(0.5)

    def connect(self, cluster_name, **kwargs):
        """Connect to a submitted cluster.

        Returns
        -------
        cluster : GatewayCluster
        """
        return self.sync(self._connect, cluster_name, **kwargs)

    async def _new_cluster(self, **kwargs):
        cluster_name = await self._submit(**kwargs)
        try:
            return await self._connect(cluster_name)
        except GatewayClusterError:
            raise
        except BaseException:
            # Ensure cluster is stopped on error
            await self._stop_cluster(cluster_name)
            raise

    def new_cluster(self, cluster_options=None, **kwargs):
        """Submit a new cluster to the gateway, and wait for it to be started.

        Same as calling ``submit`` and ``connect`` in one go.

        Parameters
        ----------
        cluster_options : Options, optional
            An ``Options`` object describing the desired cluster configuration.
        **kwargs :
            Additional cluster configuration options. If ``cluster_options`` is
            provided, these are applied afterwards as overrides. Available
            options are specific to each deployment of dask-gateway, see
            ``cluster_options`` for more information.

        Returns
        -------
        cluster : GatewayCluster
        """
        return self.sync(self._new_cluster, cluster_options=cluster_options, **kwargs)

    async def _stop_cluster(self, cluster_name):
        url = "%s/gateway/api/clusters/%s" % (self.address, cluster_name)
        req = HTTPRequest(url=url, method="DELETE")
        await self._fetch(req)

    def stop_cluster(self, cluster_name, **kwargs):
        """Stop a cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._stop_cluster, cluster_name, **kwargs)

    async def _scale_cluster(self, cluster_name, n):
        url = "%s/gateway/api/clusters/%s/workers" % (self.address, cluster_name)
        req = HTTPRequest(
            url=url,
            method="PUT",
            body=json.dumps({"worker_count": n}),
            headers=HTTPHeaders({"Content-type": "application/json"}),
        )
        resp = await self._fetch(req)
        msg = json.loads(resp.body)
        if msg["message"]:
            warnings.warn(GatewayWarning(msg["message"]))

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
    <tr><th>Cores</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
</table>
</div>
"""


class GatewayCluster(object):
    """A dask-gateway cluster.

    Users should use ``Gateway.new_cluster`` or ``Gateway.connect`` instead of
    calling this class directly.
    """

    def __init__(self, gateway, name, scheduler_address, dashboard_link, security):
        self._gateway = gateway
        self.name = name
        self.scheduler_address = scheduler_address
        self.dashboard_link = dashboard_link
        self.security = security
        self._internal_client = None
        # XXX: Ensure client is closed. Currently disconnected clients try to
        # reconnect forever, which spams the schedulerproxy. Once this bug is
        # fixed, we should be able to remove this.
        self._clients = weakref.WeakSet()

    def __repr__(self):
        return "GatewayCluster<%s>" % self.name

    async def __aenter__(self):
        return self

    async def __aexit__(self):
        await self.shutdown()

    def __enter__(self):
        return self

    def __exit__(self):
        self.shutdown()

    def shutdown(self, **kwargs):
        """Shutdown this cluster."""
        if self._internal_client is not None:
            self._internal_client.close()
            self._internal_client = None
        for client in list(self._clients):
            client.close()
            self._clients.discard(client)
        return self._gateway.stop_cluster(self.name, **kwargs)

    def close(self, **kwargs):
        """Shutdown this cluster, alias for ``shutdown``"""
        return self.shutdown(**kwargs)

    def get_client(self, set_as_default=True):
        """Get a ``Client`` for this cluster.

        Returns
        -------
        client : dask.distributed.Client
        """
        client = Client(
            self.scheduler_address,
            security=self.security,
            set_as_default=set_as_default,
            asynchronous=self._gateway.asynchronous,
            loop=self._gateway.loop,
        )
        if not self._gateway.asynchronous:
            self._clients.add(client)
        return client

    def scale(self, n, **kwargs):
        """Scale the cluster to ``n`` workers.

        Parameters
        ----------
        n : int
            The number of workers to scale to.
        """
        return self._gateway.scale_cluster(self.name, n, **kwargs)

    def _widget_status(self):
        if self._internal_client is None:
            return None
        try:
            workers = self._internal_client._scheduler_identity["workers"]
        except KeyError:
            if self._internal_client.status in ("closing", "closed"):
                return None
        else:
            n_workers = len(workers)
            cores = sum(w["nthreads"] for w in workers.values())
            memory = sum(w["memory_limit"] for w in workers.values())

            return _widget_status_template % (n_workers, cores, format_bytes(memory))

    async def _widget_updater(self, widget):
        while True:
            value = self._widget_status()
            if value is None:
                break
            widget.value = value
            await gen.sleep(2)

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        if self._gateway.asynchronous:
            return None

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML
        except ImportError:
            self._cached_widget = None
            return None

        try:
            self._internal_client = self.get_client(set_as_default=False)
        except Exception:
            return None

        layout = Layout(width="150px")

        title = HTML("<h2>GatewayCluster</h2>")

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        request = IntText(0, description="Workers", layout=layout)
        scale = Button(description="Scale", layout=layout)

        @scale.on_click
        def scale_cb(b):
            with log_errors():
                self.scale(request.value)

        elements = [title, HBox([status, request, scale])]

        if self.dashboard_link is not None:
            link = HTML(
                '<p><b>Dashboard: </b><a href="{0}" target="_blank">{0}'
                "</a></p>\n".format(self.dashboard_link)
            )
            elements.append(link)

        self._cached_widget = box = VBox(elements)

        self._internal_client.loop.add_callback(self._widget_updater, status)

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
