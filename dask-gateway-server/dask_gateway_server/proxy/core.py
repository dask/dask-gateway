import os
import json
import socket
import subprocess
import uuid
from urllib.parse import urlparse

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from traitlets.config import LoggingConfigurable, Application, catch_config_error
from traitlets import default, Unicode, CaselessStrEnum, Float, Bool

from .. import __version__ as VERSION
from ..utils import random_port


__all__ = ("SchedulerProxy", "WebProxy")


_PROXY_EXE = os.path.join(
    os.path.abspath(os.path.dirname(os.path.relpath(__file__))), "dask-gateway-proxy"
)


class ProxyBase(LoggingConfigurable):
    """Base class for dask proxies"""

    log_level = CaselessStrEnum(
        ["error", "warn", "info", "debug"],
        default_value="warn",
        help="The proxy log-level.",
        config=True,
    )

    # Forwarded by the main application on initialization
    public_url = Unicode()

    api_url = Unicode(
        help="""
        The address for configuring the Proxy.

        This is the address that the Dask Gateway will connect to when
        adding/removing routes. This must be reachable from the Dask Gateway
        server, but shouldn't be publicly accessible (if possible). Default's
        to ``127.0.0.1:{random-port}``.
        """,
        config=True,
    )

    auth_token = Unicode(
        help="""
        The Proxy auth token

        Loaded from the DASK_GATEWAY_PROXY_TOKEN env variable by default.
        """,
        config=True,
    )

    connect_timeout = Float(
        10.0,
        help="""
        Timeout (in seconds) from init until the proxy process is connected.
        """,
        config=True,
    )

    externally_managed = Bool(
        False,
        help="""
        Whether the proxy process is externally managed.

        If False (default), the proxy process will be started and stopped by
        the gateway process. Set to True if the proxy will be started via some
        external manager (e.g. supervisord).
        """,
        config=True,
    )

    @default("api_url")
    def _default_api_url(self):
        return "http://127.0.0.1:%d" % random_port()

    @default("auth_token")
    def _auth_token_default(self):
        token = os.environ.get("DASK_GATEWAY_PROXY_TOKEN", "")
        if not token:
            self.log.info("Generating new auth token for %s proxy", self._subcommand)
            token = uuid.uuid4().hex
        return token

    async def start(self):
        """Start the proxy."""
        if not self.externally_managed:
            address = urlparse(self.public_url).netloc
            api_address = urlparse(self.api_url).netloc
            command = [
                _PROXY_EXE,
                self._subcommand,
                "-address",
                address,
                "-api-address",
                api_address,
                "-log-level",
                self.log_level,
                "-is-child-process",
            ]

            env = os.environ.copy()
            env["DASK_GATEWAY_PROXY_TOKEN"] = self.auth_token
            self.log.info("Starting the Dask gateway %s proxy...", self._subcommand)
            proc = subprocess.Popen(
                command,
                env=env,
                stdin=subprocess.PIPE,
                stdout=None,
                stderr=None,
                start_new_session=True,
            )
            self.proxy_process = proc
            self.log.info(
                "Dask gateway %s proxy running at %r, api at %r",
                self._subcommand,
                self.public_url,
                self.api_url,
            )
        else:
            self.log.info(
                "Connecting to dask gateway %s proxy at %r, api at %r",
                self._subcommand,
                self.public_url,
                self.api_url,
            )

        await self.wait_until_up()

    async def wait_until_up(self):
        loop = gen.IOLoop.current()
        deadline = loop.time() + self.connect_timeout
        dt = 0.1
        while True:
            try:
                await self.get_all_routes()
                return
            except HTTPError as e:
                if e.code == 403:
                    raise RuntimeError(
                        "Failed to connect to %s proxy api at %s due to "
                        "authentication failure, please ensure that "
                        "DASK_GATEWAY_PROXY_TOKEN is the same between "
                        "the proxy process and the gateway"
                        % (self._subcommand, self.api_url)
                    )
                else:
                    raise RuntimeError(
                        "Error while connecting to %s proxy api at %s: %s"
                        % (self._subcommand, self.api_url, e)
                    )
            except (OSError, socket.error):
                # Failed to connect, see if the process erred out
                exitcode = (
                    self.proxy_process.poll()
                    if hasattr(self, "proxy_process")
                    else None
                )
                if exitcode is not None:
                    raise RuntimeError(
                        "Failed to start %s proxy, exit code %i"
                        % (self._subcommand, exitcode)
                    )
            remaining = deadline - loop.time()
            if remaining < 0:
                break
            # Exponential backoff
            dt = min(dt * 2, 5, remaining)
            await gen.sleep(dt)
        raise RuntimeError(
            "Failed to connect to %s proxy at %s in %d secs"
            % (self._subcommand, self.api_url, self.connect_timeout)
        )

    def stop(self):
        """Stop the proxy."""
        if hasattr(self, "proxy_process"):
            self.log.info("Stopping the Dask gateway %s proxy", self._subcommand)
            self.proxy_process.terminate()

    async def _api_request(self, url, method="GET", body=None):
        client = AsyncHTTPClient()
        if isinstance(body, dict):
            body = json.dumps(body)
        req = HTTPRequest(
            url,
            method=method,
            headers={"Authorization": "token %s" % self.auth_token},
            body=body,
        )
        return await client.fetch(req)

    async def add_route(self, route, target):
        """Add a route to the proxy.

        Parameters
        ----------
        route : string
            The route to add.
        target : string
            The ip:port to map this route to.
        """
        self.log.debug("Adding route %r -> %r", route, target)
        await self._api_request(
            url="%s/api/routes%s" % (self.api_url, route),
            method="PUT",
            body={"target": target},
        )

    async def delete_route(self, route):
        """Delete a route from the proxy.

        Idempotent, no error is raised if the route doesn't exist.

        Parameters
        ----------
        route : string
            The route to delete.
        """
        self.log.debug("Removing route %r", route)
        await self._api_request(
            url="%s/api/routes%s" % (self.api_url, route), method="DELETE"
        )

    async def get_all_routes(self):
        """Get the proxies current routing table.

        Returns
        -------
        routes : dict
            A dict of route -> target for all routes in the proxy.
        """
        resp = await self._api_request(url="%s/api/routes" % self.api_url, method="GET")
        return json.loads(resp.body.decode("utf8", "replace"))


class SchedulerProxy(ProxyBase):
    """A proxy for connecting Dask clients to schedulers behind a firewall."""

    _subcommand = "scheduler"


class WebProxy(ProxyBase):
    """A proxy for proxying out the dashboards from behind a firewall"""

    _subcommand = "web"


class ProxyApp(Application):
    """Start a proxy application"""

    version = VERSION

    config_file = Unicode(
        "dask_gateway_config.py", help="The config file to load", config=True
    )

    aliases = {"f": "ProxyApp.config_file", "config": "ProxyApp.config_file"}

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        self.parent.load_config_file(self.config_file)

    def exec_args(self, proxy):
        """Start the proxy."""
        address = urlparse(proxy.public_url).netloc
        api_address = urlparse(proxy.api_url).netloc
        env = os.environ.copy()
        env["DASK_GATEWAY_PROXY_TOKEN"] = proxy.auth_token

        return (
            _PROXY_EXE,
            "dask-gateway-proxy",
            proxy._subcommand,
            "-address",
            address,
            "-api-address",
            api_address,
            "-log-level",
            proxy.log_level,
            env,
        )

    def start(self):
        public_url = getattr(self.parent, self.public_url_attr)
        proxy = self.proxy_class(
            parent=self.parent, log=self.parent.log, public_url=public_url
        )
        args = self.exec_args(proxy)
        os.execle(*args)


class SchedulerProxyApp(ProxyApp):
    """Start the scheduler proxy"""

    name = "dask-gateway scheduler-proxy"
    description = "Start the scheduler proxy"

    examples = """

        dask-gateway scheduler-proxy
    """
    proxy_class = SchedulerProxy
    public_url_attr = "gateway_url"


class WebProxyApp(ProxyApp):
    """Start the web proxy"""

    name = "dask-gateway scheduler-proxy"
    description = "Start the web proxy"

    examples = """

        dask-gateway web-proxy
    """
    proxy_class = WebProxy
    public_url_attr = "public_url"
