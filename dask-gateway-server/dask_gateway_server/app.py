import asyncio
import logging
import os
import signal
import sys

from aiohttp import web
from traitlets import Bool, List, Unicode, default, validate
from traitlets.config import catch_config_error

from . import __version__ as VERSION
from .auth import Authenticator
from .backends import Backend
from .routes import default_routes
from .traitlets import Application, Type
from .utils import AccessLogger, LogFormatter, classname, normalize_address, run_main


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
        "proxy": ("dask_gateway_server.proxy.core.ProxyApp", "Start the proxy"),
        "kube-controller": (
            "dask_gateway_server.backends.kubernetes.controller.KubeController",
            "Start the k8s controller",
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

    authenticator_class = Type(
        "dask_gateway_server.auth.SimpleAuthenticator",
        klass="dask_gateway_server.auth.Authenticator",
        help="The gateway authenticator class to use",
        config=True,
    )

    backend_class = Type(
        klass="dask_gateway_server.backends.Backend",
        help="The gateway backend class to use",
        config=True,
    )

    @default("backend_class")
    def _default_backend_class(self):
        # Separate the default out to avoid traitlets auto-importing it,
        # even if it's unused :(.
        return "dask_gateway_server.backends.local.UnsafeLocalBackend"

    address = Unicode(
        help="""
        The address the private api server should *listen* at.

        Should be of the form ``{hostname}:{port}``

        Where:

        - ``hostname`` sets the hostname to *listen* at. Set to ``""`` or
          ``"0.0.0.0"`` to listen on all interfaces.
        - ``port`` sets the port to *listen* at.

        Defaults to ``127.0.0.1:0``.
        """,
        config=True,
    )

    @default("address")
    def _default_address(self):
        return normalize_address("127.0.0.1:0")

    @validate("address")
    def _validate_address(self, proposal):
        return normalize_address(proposal.value)

    _log_formatter_cls = LogFormatter

    classes = List([Backend, Authenticator])

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

        # Initialize authenticator and backend
        self.log.info("Authenticator: %r", classname(self.authenticator_class))
        self.authenticator = self.authenticator_class(parent=self, log=self.log)
        self.log.info("Backend: %r", classname(self.backend_class))
        self.backend = self.backend_class(parent=self, log=self.log)

        # Initialize aiohttp application
        self.app = web.Application(logger=self.log)
        self.app.add_routes(default_routes)
        self.app["gateway"] = self
        self.app["backend"] = self.backend
        self.app["authenticator"] = self.authenticator
        self.app["log"] = self.log

    def handle_shutdown_signal(self, sig):
        self.log.warning("Received signal %s, initiating shutdown...", sig.name)
        raise web.GracefulExit

    async def setup(self):
        # Register signal handlers
        loop = asyncio.get_event_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.handle_shutdown_signal, s)

        # Start the authenticator
        await self.authenticator.setup(self.app)

        # Start the backend
        await self.backend.setup(self.app)

        # Start the aiohttp application
        self.runner = web.AppRunner(
            self.app,
            handle_signals=False,
            access_log_class=AccessLogger,
            access_log=self.log,
        )
        await self.runner.setup()

        host, port = self.address.split(":")
        port = int(port)
        site = web.TCPSite(self.runner, host, port, shutdown_timeout=15.0, backlog=128)
        await site.start()
        self.log.info("Dask-Gateway server started")
        self.log.info("- Private API server listening at http://%s", self.address)

    async def cleanup(self):
        # Shutdown the aiohttp application
        if hasattr(self, "runner"):
            await self.runner.cleanup()

        # Shutdown backend
        if hasattr(self, "backend"):
            await self.backend.cleanup()

        # Shutdown authenticator
        if hasattr(self, "authenticator"):
            await self.authenticator.cleanup()

        self.log.info("Stopped successfully")

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()

        try:
            run_main(self.main())
        except (KeyboardInterrupt, web.GracefulExit):
            pass

    async def main(self):
        try:
            try:
                await self.setup()
            except Exception:
                self.log.critical(
                    "Failed to start gateway, shutting down", exc_info=True
                )
                sys.exit(1)

            while True:
                await asyncio.sleep(3600)
        finally:
            try:
                await self.cleanup()
            except Exception:
                self.log.error("Error while shutting down:", exc_info=True)

    async def health(self):
        # TODO: add runtime checks here
        return {"status": "pass"}

    def version_info(self):
        """Handles the /api/version route"""
        return {
            "version": VERSION,
        }


main = DaskGateway.launch_instance
