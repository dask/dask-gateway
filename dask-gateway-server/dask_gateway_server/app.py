import asyncio
import logging
import os
import signal
import sys
from urllib.parse import urlparse

from aiohttp import web
from colorlog import ColoredFormatter
from traitlets import Unicode, Bool, Bytes, Float, List, default, validate
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from .auth import Authenticator
from .backends import Backend
from .proxy import SchedulerProxy, WebProxy
from .routes import default_routes
from .utils import classname, TaskPool, Type, ServerUrls


class LogFormatter(ColoredFormatter):
    def __init__(self, fmt=None, datefmt=None):
        super().__init__(
            fmt=fmt,
            datefmt=datefmt,
            reset=False,
            log_colors={
                "DEBUG": "blue",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )


# Override default values for logging
Application.log_level.default_value = "INFO"
Application.log_format.default_value = (
    "%(log_color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(reset)s %(message)s"
)


class GenerateConfig(Application):
    """Generate and write a default configuration file"""

    name = "dask-gateway-server generate-config"
    version = VERSION
    description = "Generate and write a default configuration file"

    examples = """

        dask-gateway-server generate-config
    """

    output = Unicode(
        "dask_gateway_config.py", help="The path to write the config file", config=True
    )

    force = Bool(False, help="If true, will overwrite file if it exists.", config=True)

    aliases = {"output": "GenerateConfig.output"}

    flags = {
        "force": (
            {"GenerateConfig": {"force": True}},
            "Overwrite config file if it exists",
        )
    }

    def start(self):
        config_file_dir = os.path.dirname(os.path.abspath(self.output))
        if not os.path.isdir(config_file_dir):
            self.exit(
                "%r does not exist. The destination directory must exist "
                "before generating config file." % config_file_dir
            )
        if os.path.exists(self.output) and not self.force:
            self.exit("Config file already exists, use `--force` to overwrite")

        config_text = DaskGateway.instance().generate_config_file()
        if isinstance(config_text, bytes):
            config_text = config_text.decode("utf8")
        print("Writing default config to: %s" % self.output)
        with open(self.output, mode="w") as f:
            f.write(config_text)


class DaskGateway(Application):
    """A gateway for managing dask clusters across multiple users"""

    name = "dask-gateway-server"
    version = VERSION

    description = """Start a Dask Gateway server"""

    examples = """

    Start the server with config file ``config.py``

        dask-gateway-server -f config.py
    """

    subcommands = {
        "generate-config": (
            "dask_gateway_server.app.GenerateConfig",
            "Generate a default config file",
        ),
        "scheduler-proxy": (
            "dask_gateway_server.proxy.core.SchedulerProxyApp",
            "Start the scheduler proxy",
        ),
        "web-proxy": (
            "dask_gateway_server.proxy.core.WebProxyApp",
            "Start the web proxy",
        ),
    }

    aliases = {
        "log-level": "DaskGateway.log_level",
        "f": "DaskGateway.config_file",
        "config": "DaskGateway.config_file",
    }

    config_file = Unicode(
        "dask_gateway_config.py", help="The config file to load", config=True
    )

    # Fail if the config file errors
    raise_config_file_errors = True

    scheduler_proxy_class = Type(
        "dask_gateway_server.proxy.SchedulerProxy",
        help="The gateway scheduler proxy class to use",
    )

    web_proxy_class = Type(
        "dask_gateway_server.proxy.WebProxy", help="The gateway web proxy class to use"
    )

    authenticator_class = Type(
        "dask_gateway_server.auth.DummyAuthenticator",
        klass="dask_gateway_server.auth.Authenticator",
        help="The gateway authenticator class to use",
        config=True,
    )

    backend_class = Type(
        "dask_gateway_server.backends.local.UnsafeLocalBackend",
        klass="dask_gateway_server.backends.Backend",
        help="The gateway backend class to use",
        config=True,
    )

    public_url = Unicode(
        "http://:8000",
        help="The public facing URL of the whole Dask Gateway application",
        config=True,
    )

    public_connect_url = Unicode(
        help="""
        The address that the public URL can be connected to.

        Useful if the address the web proxy should listen at is different than
        the address it's reachable at (by e.g. the scheduler/workers).

        Defaults to ``public_url``.
        """,
        config=True,
    )

    gateway_url = Unicode(help="The URL that Dask clients will connect to", config=True)

    private_url = Unicode(
        "http://127.0.0.1:0",
        help="""
        The gateway's private URL, used for internal communication.

        This must be reachable from the web proxy, but shouldn't be publicly
        accessible (if possible). Default is ``http://127.0.0.1:{random-port}``.
        """,
        config=True,
    )

    private_connect_url = Unicode(
        help="""
        The address that the private URL can be connected to.

        Useful if the address the gateway should listen at is different than
        the address it's reachable at (by e.g. the web proxy).

        Defaults to ``private_url``.
        """,
        config=True,
    )

    @validate(
        "public_url",
        "public_connect_url",
        "gateway_url",
        "private_url",
        "private_connect_url",
    )
    def _validate_url(self, proposal):
        url = proposal.value
        name = proposal.trait.name
        scheme = urlparse(url).scheme
        if name.startswith("gateway"):
            if scheme != "tls":
                raise ValueError("'gateway_url' must be a tls url, got %s" % url)
        else:
            if scheme not in {"http", "https"}:
                raise ValueError("%r must be an http/https url, got %s" % (name, url))
        return url

    tls_key = Unicode(
        "",
        help="""Path to TLS key file for the public url of the web proxy.

        When setting this, you should also set tls_cert.
        """,
        config=True,
    )

    tls_cert = Unicode(
        "",
        help="""Path to TLS certificate file for the public url of the web proxy.

        When setting this, you should also set tls_key.
        """,
        config=True,
    )

    cookie_secret = Bytes(
        help="""The cookie secret to use to encrypt cookies.

        A 32 byte hex-encoded random string. Commonly created with the
        ``openssl`` CLI:

        .. code-block:: shell

            $ openssl rand -hex 32

        Loaded from the DASK_GATEWAY_COOKIE_SECRET environment variable by
        default.
        """,
        config=True,
    )

    @default("cookie_secret")
    def _cookie_secret_default(self):
        secret = os.environb.get(b"DASK_GATEWAY_COOKIE_SECRET", b"")
        if not secret:
            self.log.debug("Generating new cookie secret")
            secret = os.urandom(32)
        return secret

    @validate("cookie_secret")
    def _cookie_secret_validate(self, proposal):
        if len(proposal["value"]) != 32:
            raise ValueError(
                "Cookie secret is %d bytes, it must be "
                "32 bytes" % len(proposal["value"])
            )
        return proposal["value"]

    cookie_max_age_days = Float(
        7,
        help="""Number of days for a login cookie to be valid.
        Default is one week.
        """,
        config=True,
    )

    temp_dir = Unicode(
        None,
        help="""
        The directory to use when creating temporary runtime files.

        Defaults to the platform's temporary directory, see
        https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir for
        more information.
        """,
        config=True,
        allow_none=True,
    )

    _log_formatter_cls = LogFormatter

    classes = List([Backend, Authenticator, WebProxy, SchedulerProxy])

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return

        # Setup logging
        self.log.propagate = False
        for name in ["aiohttp.access", "aiohttp.server"]:
            l = logging.getLogger(name)
            l.handlers[:] = []
            l.propagate = True
            l.parent = self.log
            l.setLevel(self.log_level)

        self.log.info("Starting dask-gateway-server - version %s", VERSION)

        # Load configuration
        self.load_config_file(self.config_file)

        # Initialize URLs from configuration
        self.public_urls = ServerUrls(self.public_url, self.public_connect_url)
        self.private_urls = ServerUrls(self.private_url, self.private_connect_url)
        if not self.gateway_url:
            gateway_url = f"tls://{self.public_urls.bind_host}:8786"
        else:
            gateway_url = self.gateway_url
        self.gateway_urls = ServerUrls(gateway_url)
        self.api_url = self.public_urls.connect_url + "/gateway/api"

        # Initialize proxies
        self.scheduler_proxy = self.scheduler_proxy_class(
            parent=self, log=self.log, public_urls=self.gateway_urls
        )
        self.web_proxy = self.web_proxy_class(
            parent=self,
            log=self.log,
            public_urls=self.public_urls,
            tls_cert=self.tls_cert,
            tls_key=self.tls_key,
        )

        # Initialize task pool
        self.task_pool = TaskPool()

        # Initialize authenticator and backend
        self.log.info("Authenticator: %r", classname(self.authenticator_class))
        self.authenticator = self.authenticator_class(parent=self, log=self.log)
        self.log.info("Backend: %r", classname(self.backend_class))
        self.backend = self.backend_class(parent=self, log=self.log)

        # Initialize aiohttp application
        self.app = web.Application(logger=self.log)
        self.app.add_routes(default_routes)
        self.app["backend"] = self.backend
        self.app["authenticator"] = self.authenticator

    def handle_shutdown_signal(self, sig):
        self.log.warning("Received signal %s, initiating shutdown...", sig.name)
        raise web.GracefulExit

    async def start_async(self):
        # Register signal handlers
        loop = asyncio.get_event_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.handle_shutdown_signal, s)

        # Start the aiohttp application
        self.runner = web.AppRunner(self.app, handle_signals=False)
        await self.runner.setup()

        site = web.TCPSite(
            self.runner,
            self.private_urls.bind.hostname,
            self.private_urls.bind.port,
            shutdown_timeout=60.0,
            backlog=128,
        )
        await site.start()
        self.log.info("Gateway private API serving at %s", self.private_urls.bind_url)

        # Start the proxies
        await self.scheduler_proxy.start()
        await self.web_proxy.start()

        # Start the backend
        await self.backend.on_startup()

        # Add the private url to the proxy
        await self.web_proxy.add_route(
            self.public_urls.connect.path + "/gateway/", self.private_urls.connect_url
        )

        # Log routes
        self.log.info("Dask-Gateway started successfully!")
        for name, urls in [
            ("Public address", self.public_urls._to_log),
            ("Proxy address", self.gateway_urls._to_log),
        ]:
            if len(urls) == 2:
                self.log.info("- %s at %s or %s", name, *urls)
            else:
                self.log.info("- %s at %s", name, *urls)

    async def stop_async(self):
        # Shutdown the aiohttp application
        if hasattr(self, "runner"):
            await self.runner.cleanup()

        # Shutdown the proxies
        if hasattr(self, "scheduler_proxy"):
            self.scheduler_proxy.stop()
        if hasattr(self, "web_proxy"):
            self.web_proxy.stop()

        # Shutdown backend
        if hasattr(self, "backend"):
            await self.backend.on_shutdown()

        # Close task pool
        if hasattr(self, "task_pool"):
            await self.task_pool.close(timeout=10)

    async def run_app(self):
        try:
            await self.start_async()
        except Exception:
            self.log.critical("Failed to start gateway, shutting down", exc_info=True)
            sys.exit(1)

        while True:
            await asyncio.sleep(3600)

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()
        loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(self.run_app())
        except (KeyboardInterrupt, web.GracefulExit):
            pass
        finally:
            try:
                loop.run_until_complete(self.stop_async())
            except Exception:
                self.log.error("Error while shutting down:", exc_info=True)
            loop.close()


main = DaskGateway.launch_instance
