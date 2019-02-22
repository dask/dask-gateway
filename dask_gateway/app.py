import os
import logging
from urllib.parse import urlparse

from tornado import web
from tornado.log import LogFormatter
from tornado.gen import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop
from traitlets import Unicode, Bool, Type, Bytes, Float, default, validate
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from . import handlers


# Override default values for logging
Application.log_level.default_value = 'INFO'
Application.log_format.default_value = (
    "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(end_color)s %(message)s"
)


class GenerateConfig(Application):
    """Generate and write a default configuration file"""

    name = 'dask-gateway generate-config'
    version = VERSION
    description = "Generate and write a default configuration file"

    examples = """

        dask-gateway generate-config
    """

    output = Unicode(
        "dask_gateway_config.py",
        help="The path to write the config file",
        config=True
    )

    force = Bool(
        False,
        help="If true, will overwrite file if it exists.",
        config=True
    )

    aliases = {
        'output': 'GenerateConfig.output',
    }

    flags = {
        'force': ({'GenerateConfig': {'force': True}},
                  "Overwrite config file if it exists")
    }

    def start(self):
        config_file_dir = os.path.dirname(os.path.abspath(self.output))
        if not os.path.isdir(config_file_dir):
            self.exit("%r does not exist. The destination directory must exist "
                      "before generating config file." % config_file_dir)
        if os.path.exists(self.output) and not self.force:
            self.exit("Config file already exists, use `--force` to overwrite")

        config_text = DaskGateway.instance().generate_config_file()
        if isinstance(config_text, bytes):
            config_text = config_text.decode('utf8')
        print("Writing default config to: %s" % self.output)
        with open(self.output, mode='w') as f:
            f.write(config_text)


class DaskGateway(Application):
    """A gateway for managing dask clusters across multiple users"""
    name = 'dask-gateway'
    version = VERSION

    description = """Start a Dask Gateway server"""

    examples = """

    Start the server on 10.0.1.2:8080:

        dask-gateway --public-url 10.0.1.2:8080
    """

    subcommands = {
        'generate-config': (
            'dask_gateway.app.GenerateConfig',
            'Generate a default config file'
        )
    }

    aliases = {
        'log-level': 'DaskGateway.log_level',
        'f': 'DaskGateway.config_file',
        'config': 'DaskGateway.config_file'
    }

    config_file = Unicode(
        'dask_gateway_config.py',
        help="The config file to load",
        config=True
    )

    scheduler_proxy_class = Type(
        'dask_gateway.proxy.SchedulerProxy',
        help="The gateway scheduler proxy class to use"
    )

    web_proxy_class = Type(
        'dask_gateway.proxy.WebProxy',
        help="The gateway web proxy class to use"
    )

    authenticator_class = Type(
        'dask_gateway.auth.KerberosAuthenticator',
        klass='dask_gateway.auth.Authenticator',
        help="The gateway authenticator class to use"
    )

    public_url = Unicode(
        "http://:8000",
        help="The public facing URL of the whole Dask Gateway application",
        config=True
    )

    gateway_url = Unicode(
        "tls://:8786",
        help="The URL that Dask clients will connect to",
        config=True
    )

    private_url = Unicode(
        "http://127.0.0.1:8081",
        help="The gateway's private URL used for internal communication",
        config=True
    )

    cookie_secret = Bytes(
        help="""The cookie secret to use to encrypt cookies.

        Loaded from the DASK_GATEWAY_COOKIE_SECRET environment variable by
        default.
        """,
        config=True
    )

    cookie_max_age_days = Float(
        7,
        help="""Number of days for a login cookie to be valid.
        Default is one week.
        """,
        config=True
    )

    @default('cookie_secret')
    def _cookie_secret_default(self):
        secret = os.environb.get(b'DASK_GATEWAY_COOKIE_SECRET', b'')
        if not secret:
            self.log.info("Generating new cookie secret")
            secret = os.urandom(32)
        return secret

    @validate('cookie_secret')
    def _cookie_secret_validate(self, proposal):
        if len(proposal['value']) != 32:
            raise ValueError("Cookie secret is %d bytes, it must be "
                             "32 bytes" % len(proposal['value']))
        return proposal['value']

    _log_formatter_cls = LogFormatter

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return
        self.load_config_file(self.config_file)
        self.init_logging()
        self.init_scheduler_proxy()
        self.init_web_proxy()
        self.init_authenticator()
        self.init_tornado_application()

    def init_logging(self):
        # Prevent double log messages from tornado
        self.log.propagate = False

        # hook up tornado's loggers to our app handlers
        from tornado.log import app_log, access_log, gen_log
        for log in (app_log, access_log, gen_log):
            log.name = self.log.name
        logger = logging.getLogger('tornado')
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log.level)

    def init_scheduler_proxy(self):
        self.scheduler_proxy = self.scheduler_proxy_class(
            parent=self,
            log=self.log,
            public_url=self.gateway_url
        )

    def init_web_proxy(self):
        self.web_proxy = self.web_proxy_class(
            parent=self,
            log=self.log,
            public_url=self.public_url
        )

    def init_authenticator(self):
        self.authenticator = self.authenticator_class(
            parent=self,
            log=self.log
        )

    def init_tornado_application(self):
        self.handlers = list(handlers.default_handlers)
        self.tornado_application = web.Application(
            self.handlers,
            log=self.log,
            authenticator=self.authenticator,
            cookie_secret=self.cookie_secret,
            cookie_max_age_days=self.cookie_max_age_days
        )

    async def start_async(self):
        self.start_scheduler_proxy()
        self.start_web_proxy()
        await self.start_tornado_application()

    def start_scheduler_proxy(self):
        try:
            self.scheduler_proxy.start()
        except Exception:
            self.log.critical("Failed to start scheduler proxy", exc_info=True)
            self.exit(1)

    def start_web_proxy(self):
        try:
            self.web_proxy.start()
        except Exception:
            self.log.critical("Failed to start web proxy", exc_info=True)
            self.exit(1)

    async def start_tornado_application(self):
        private_url = urlparse(self.private_url)
        self.http_server = self.tornado_application.listen(
            private_url.port, address=private_url.hostname
        )
        self.log.info("Gateway API listening on %s", self.private_url)
        await self.web_proxy.add_route("/gateway/", self.private_url)

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()
        AsyncIOMainLoop().install()
        loop = IOLoop.current()
        loop.add_callback(self.start_async)
        try:
            loop.start()
        except KeyboardInterrupt:
            print("\nInterrupted")


main = DaskGateway.launch_instance


if __name__ == "__main__":
    main()
