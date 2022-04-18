import asyncio
import json
import os
import subprocess
import uuid

from aiohttp import web
from traitlets import (
    Bool,
    CaselessStrEnum,
    Float,
    Integer,
    List,
    Unicode,
    default,
    validate,
)
from traitlets.config import Application, LoggingConfigurable, catch_config_error

from .. import __version__ as VERSION
from ..utils import CancelGroup, Flag, TaskPool, normalize_address

__all__ = ("Proxy", "ProxyApp")


_PROXY_EXE = os.path.join(
    os.path.abspath(os.path.dirname(os.path.relpath(__file__))), "dask-gateway-proxy"
)


class Proxy(LoggingConfigurable):
    """The dask-gateway-server proxy"""

    log_level = CaselessStrEnum(
        ["error", "warn", "info", "debug"],
        default_value="warn",
        help="The proxy log-level.",
        config=True,
    )

    address = Unicode(
        ":8000",
        help="""
        The address the HTTP/HTTPS proxy should *listen* at.

        Should be of the form ``{hostname}:{port}``.

        Where:

        - ``hostname`` sets the hostname to *listen* at. Set to ``""`` or
          ``"0.0.0.0"`` to listen on all interfaces.
        - ``port`` sets the port to *listen* at.
        """,
        config=True,
    )

    tcp_address = Unicode(
        help="""
        The address the TCP (scheduler) proxy should *listen* at.

        Should be of the form ``{hostname}:{port}``

        Where:

        - ``hostname`` sets the hostname to *listen* at. Set to ``""`` or
          ``"0.0.0.0"`` to listen on all interfaces.
        - ``port`` sets the port to *listen* at.

        If not specified, will default to `address`.
        """,
        config=True,
    )

    @default("tcp_address")
    def _default_tcp_address(self):
        return self.address

    @validate("address", "tcp_address")
    def _validate_addresses(self, proposal):
        return normalize_address(proposal.value)

    prefix = Unicode(
        "",
        help="""
        The path prefix the HTTP/HTTPS proxy should serve under.

        This prefix will be prepended to all routes registered with the proxy.
        """,
        config=True,
    )

    @validate("prefix")
    def _validate_prefix(self, proposal):
        prefix = proposal.value

        if prefix == "":
            return prefix

        if not prefix.startswith("/"):
            raise ValueError("invalid prefix, must start with ``/``")

        return prefix.rstrip("/")

    gateway_url = Unicode(
        help="The base URL the proxy should use for connecting to the gateway server",
        config=True,
    )

    @default("gateway_url")
    def _gateway_url_default(self):
        address = normalize_address(self.gateway_address, resolve_host=True)
        return f"http://{address}"

    api_token = Unicode(
        help="""
        The Proxy api token

        A 32 byte hex-encoded random string. Commonly created with the
        ``openssl`` CLI:

        .. code-block:: shell

            $ openssl rand -hex 32

        Loaded from the DASK_GATEWAY_PROXY_TOKEN env variable by default.
        """,
        config=True,
    )

    @default("api_token")
    def _api_token_default(self):
        token = os.environ.get("DASK_GATEWAY_PROXY_TOKEN", "")
        if not token:
            self.log.info("Generating new api token for proxy")
            token = uuid.uuid4().hex
        return token

    max_events = Integer(
        100,
        help="""
        The maximum number of events (proxy changes) to retain.

        A proxy server that lags by more than this number will have to do a
        full refesh.
        """,
        config=True,
    )

    tls_key = Unicode(
        "",
        help="""Path to TLS key file for the public url of the HTTP proxy.

        When setting this, you should also set tls_cert.
        """,
        config=True,
    )

    tls_cert = Unicode(
        "",
        help="""Path to TLS certificate file for the public url of the HTTP proxy.

        When setting this, you should also set tls_key.
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

    proxy_status_period = Float(
        30,
        help="""
        Time (in seconds) between proxy status checks.

        Only applies when ``externally_managed`` is False.
        """,
        config=True,
    )

    # forwarded from parent
    gateway_address = Unicode()

    @default("gateway_address")
    def _gateway_address_default(self):
        return self.parent.gateway_address

    def get_start_command(self, is_child_process=True):
        out = [
            _PROXY_EXE,
            "-address",
            self.address,
            "-tcp-address",
            self.tcp_address,
            "-api-url",
            self.gateway_url + "/api/v1/routes",
            "-log-level",
            self.log_level,
        ]
        if is_child_process:
            out.append("-is-child-process")
        if bool(self.tls_cert) != bool(self.tls_key):
            raise ValueError("Must set both tls_cert and tls_key")
        if self.tls_cert:
            out.extend(["-tls-cert", self.tls_cert, "-tls-key", self.tls_key])
        return out

    def get_start_env(self):
        env = os.environ.copy()
        env["DASK_GATEWAY_PROXY_TOKEN"] = self.api_token
        return env

    def start_proxy_process(self):
        command = self.get_start_command()
        env = self.get_start_env()
        self.log.info("Starting the Dask gateway proxy...")
        proc = subprocess.Popen(
            command,
            env=env,
            stdin=subprocess.PIPE,
            stdout=None,
            stderr=None,
            start_new_session=True,
        )
        self.proxy_process = proc
        self.log.info("Dask gateway proxy started")
        self.log.info(
            "- %s routes listening at %s://%s",
            "HTTPS" if self.tls_cert else "HTTP",
            "https" if self.tls_cert else "http",
            self.address,
        )
        self.log.info("- Scheduler routes listening at gateway://%s", self.tcp_address)

    async def monitor_proxy_process(self):
        backoff = default_backoff = 0.5
        while True:
            retcode = self.proxy_process.poll()
            if retcode is not None:
                self.log.warning(
                    "Proxy process exited unexpectedly with return code %d, restarting",
                    retcode,
                )
                try:
                    self.start_proxy_process()
                except Exception:
                    self.log.error(
                        "Failed starting the proxy process, retrying in %.1f seconds...",
                        backoff,
                        exc_info=True,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 15)
                    continue

            backoff = default_backoff
            await asyncio.sleep(self.proxy_status_period)

    async def setup(self, app):
        """Start the proxy."""
        self.cg = CancelGroup()
        self.task_pool = TaskPool()
        # Exposed for testing
        self._proxy_contacted = Flag()
        if not self.externally_managed:
            self.start_proxy_process()
            self.task_pool.spawn(self.monitor_proxy_process())

        # Internal state
        self.routes = {}
        self.offset = 0
        self.events = []
        self._watchers = set()
        self._next_id = 1

        app.on_shutdown.append(self._on_shutdown)

        app.add_routes([web.get("/api/v1/routes", self.routes_handler)])

        # Proxy through the gateway application
        await self.add_route(kind="PATH", path="/", target=self.gateway_url)

    async def _on_shutdown(self, app):
        await self.task_pool.close()
        await self.cg.cancel()

    async def cleanup(self):
        """Stop the proxy."""
        await self.task_pool.close()
        if hasattr(self, "proxy_process"):
            self.log.info("Stopping the Dask gateway proxy")
            self.proxy_process.terminate()

    def _get_id(self):
        o = self._next_id
        self._next_id += 1
        return o

    def _append_event(self, kind, route):
        event = {"id": self._get_id(), "type": kind, "route": route}

        if len(self.events) >= self.max_events:
            n_dropped = self.max_events // 2
            self.events = self.events[n_dropped:]
            self.offset += n_dropped

        self.events.append(event)

        for q in self._watchers:
            q.put_nowait([event])

    async def add_route(self, kind=None, sni=None, path=None, target=None):
        if target is None:
            raise ValueError("must specify `target`")

        if kind == "SNI":
            if sni is None:
                raise ValueError("must specify `sni`")
            route = {"kind": kind, "sni": sni, "target": target}
            self.routes[(kind, sni)] = route
        elif kind == "PATH":
            if path is None:
                raise ValueError("must specify `path`")
            path = self.prefix + path
            route = {"kind": kind, "path": path, "target": target}
            self.routes[(kind, path)] = route
        else:
            raise ValueError(f"Unknown route kind {kind}")
        self._append_event("PUT", route)

    async def remove_route(self, kind=None, sni=None, path=None):
        if kind == "SNI":
            if sni is None:
                raise ValueError("must specify `sni`")
            route = self.routes.pop((kind, sni), None)
            if route is not None:
                route = {"kind": kind, "sni": sni}
        elif kind == "PATH":
            if path is None:
                raise ValueError("must specify `path`")
            path = self.prefix + path
            route = self.routes.pop((kind, path), None)
            if route is not None:
                route = {"kind": kind, "path": path}
        else:
            raise ValueError(f"Unknown route kind {kind}")

        if route is not None:
            self._append_event("DELETE", route)

    def _events_after(self, last_id=0):
        index = last_id - self.offset
        if index >= 0:
            events = self.events[index:]
            return events
        else:
            # We've since dropped these events, client needs to reset
            return None

    async def routes_handler(self, request):
        self._proxy_contacted.set()
        # Authenticate the api request
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        auth_type, auth_token = auth_header.split(" ", 1)
        if auth_type != "token" or auth_token != self.api_token:
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": "token"})

        watch = request.query.get("watch", False)

        if watch:
            last_id = request.query.get("last_id", 0)
            try:
                last_id = int(last_id)
            except Exception:
                raise web.HTTPUnprocessableEntity(
                    reason="invalid `last_id`: %s" % last_id
                )

            events = self._events_after(last_id)
            if events is None:
                raise web.HTTPGone(reason="`last_id` is too old, full refresh required")

            queue = asyncio.Queue()
            if events:
                queue.put_nowait(events)
            self._watchers.add(queue)
            try:
                response = web.StreamResponse(
                    headers={"Content-Type": "application/json"}
                )
                await response.prepare(request)

                async with self.cg.cancellable():
                    while True:
                        events = await queue.get()
                        msg = {"events": events}
                        await response.write(json.dumps(msg).encode() + b"\n")
            finally:
                self._watchers.discard(queue)

            await response.write_eof()
            return response

        else:
            msg = {"routes": list(self.routes.values()), "id": self._next_id - 1}
            return web.json_response(msg)


class ProxyApp(Application):
    """Start a proxy application"""

    name = "dask-gateway-server proxy"
    description = "Start the proxy"
    examples = """

        dask-gateway-server proxy -f config.py
    """
    version = VERSION

    config_file = Unicode(
        "dask_gateway_config.py", help="The config file to load", config=True
    )

    aliases = {
        "f": "ProxyApp.config_file",
        "config": "ProxyApp.config_file",
        "log-level": "Proxy.log_level",
    }

    classes = List([Proxy])

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        self.parent.load_config_file(self.config_file)
        self.parent.config.merge(self.config)

    def start(self):
        proxy = Proxy(
            parent=self.parent, log=self.parent.log, gateway_address=self.parent.address
        )
        command = proxy.get_start_command(is_child_process=False)
        env = proxy.get_start_env()
        exe = command[0]
        args = command[1:]
        os.execle(exe, "dask-gateway-proxy", *args, env)
