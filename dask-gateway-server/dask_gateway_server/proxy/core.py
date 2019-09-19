import os
import json
import socket
import subprocess
import uuid

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from traitlets.config import LoggingConfigurable, Application, catch_config_error
from traitlets import default, Unicode, CaselessStrEnum, Float, Bool, Instance

from .. import __version__ as VERSION
from ..utils import ServerUrls


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
    # or
    public_urls = Instance(ServerUrls)

    @default("public_urls")
    def _default_public_urls(self):
        return ServerUrls(self.public_url)

    api_url = Unicode(
        "http://127.0.0.1:0",
        help="""
        The address for configuring the Proxy.

        This is the address that the Dask Gateway will connect to when
        adding/removing routes. This must be reachable from the Dask Gateway
        server, but shouldn't be publicly accessible (if possible). Defaults
        to ``127.0.0.1:{random-port}``.
        """,
        config=True,
    )

    api_connect_url = Unicode(
        help="""
        The address that the api URL can be connected to.

        Useful if the address the api server should listen at is different than
        the address it's reachable at (by e.g. the gateway).

        Defaults to ``api_url``.
        """,
        config=True,
    )

    api_urls = Instance(ServerUrls)

    @default("api_urls")
    def _default_api_urls(self):
        return ServerUrls(self.api_url, self.api_connect_url)

    auth_token = Unicode(
        help="""
        The Proxy auth token

        Loaded from the DASK_GATEWAY_PROXY_TOKEN env variable by default.
        """,
        config=True,
    )

    @default("auth_token")
    def _auth_token_default(self):
        token = os.environ.get("DASK_GATEWAY_PROXY_TOKEN", "")
        if not token:
            self.log.info("Generating new auth token for %s proxy", self._subcommand)
            token = uuid.uuid4().hex
        return token

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

    def get_start_command(self, is_child_process=True):
        address = self.public_urls.bind.netloc
        api_address = self.api_urls.bind.netloc
        out = [
            _PROXY_EXE,
            self._subcommand,
            "-address",
            address,
            "-api-address",
            api_address,
            "-log-level",
            self.log_level,
        ]
        if is_child_process:
            out.append("-is-child-process")
        return out

    def get_start_env(self):
        env = os.environ.copy()
        env["DASK_GATEWAY_PROXY_TOKEN"] = self.auth_token
        return env

    async def start(self):
        """Start the proxy."""
        if not self.externally_managed:
            command = self.get_start_command()
            env = self.get_start_env()
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
                "Dask gateway %s proxy started at %r, api at %r",
                self._subcommand,
                self.public_urls.connect_url,
                self.api_urls.connect_url,
            )
        else:
            self.log.info(
                "Connecting to dask gateway %s proxy at %r, api at %r",
                self._subcommand,
                self.public_urls.connect_url,
                self.api_urls.connect_url,
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
                        % (self._subcommand, self.api_urls.connect_url)
                    )
                elif e.code != 599:
                    raise RuntimeError(
                        "Error while connecting to %s proxy api at %s: %s"
                        % (self._subcommand, self.api_urls.connect_url, e)
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
            % (self._subcommand, self.api_urls.connect_url, self.connect_timeout)
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
            url="%s/api/routes%s" % (self.api_urls.connect_url, route),
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
            url="%s/api/routes%s" % (self.api_urls.connect_url, route), method="DELETE"
        )

    async def get_all_routes(self):
        """Get the proxies current routing table.

        Returns
        -------
        routes : dict
            A dict of route -> target for all routes in the proxy.
        """
        resp = await self._api_request(
            url="%s/api/routes" % self.api_urls.connect_url, method="GET"
        )
        return json.loads(resp.body.decode("utf8", "replace"))


class SchedulerProxy(ProxyBase):
    """A proxy for connecting Dask clients to schedulers behind a firewall."""

    _subcommand = "scheduler"


class WebProxy(ProxyBase):
    """A proxy for proxying out the dashboards from behind a firewall"""

    _subcommand = "web"

    # Forwarded by the main application
    tls_cert = Unicode()
    tls_key = Unicode()

    def get_start_command(self, is_child_process=True):
        out = super().get_start_command(is_child_process=is_child_process)
        if bool(self.tls_cert) != bool(self.tls_key):
            raise ValueError("Must set both tls_cert and tls_key")
        if self.tls_cert:
            if self.public_urls.bind.scheme != "https":
                raise ValueError(
                    "tls_cert & tls_key are set, but public_url doesn't have an "
                    "https scheme"
                )
            out.extend(["-tls-cert", self.tls_cert, "-tls-key", self.tls_key])
        return out


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

    def start(self):
        proxy = self.make_proxy()
        command = proxy.get_start_command(is_child_process=False)
        env = proxy.get_start_env()
        exe = command[0]
        args = command[1:]
        os.execle(exe, "dask-gateway-proxy", *args, env)


class SchedulerProxyApp(ProxyApp):
    """Start the scheduler proxy"""

    name = "dask-gateway-server scheduler-proxy"
    description = "Start the scheduler proxy"

    examples = """

        dask-gateway-server scheduler-proxy
    """

    def make_proxy(self):
        return SchedulerProxy(
            parent=self.parent, log=self.parent.log, public_url=self.parent.gateway_url
        )


class WebProxyApp(ProxyApp):
    """Start the web proxy"""

    name = "dask-gateway-server scheduler-proxy"
    description = "Start the web proxy"

    examples = """

        dask-gateway-server web-proxy
    """

    def make_proxy(self):
        return WebProxy(
            parent=self.parent,
            log=self.parent.log,
            public_url=self.parent.public_url,
            tls_cert=self.parent.tls_cert,
            tls_key=self.parent.tls_key,
        )
