import os
import logging

from tornado.log import LogFormatter
from tornado.gen import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop
from traitlets import Unicode, Bool, Type
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION


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
        self.scheduler_proxy = self.scheduler_proxy_class(log=self.log)

    def init_web_proxy(self):
        self.web_proxy = self.web_proxy_class(log=self.log)

    async def start_async(self):
        self.start_scheduler_proxy()
        self.start_web_proxy()

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
