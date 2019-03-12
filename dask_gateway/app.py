import asyncio
import json
import logging
import os
import stat
import tempfile
import uuid
import weakref
from urllib.parse import urlparse

from tornado import web
from tornado.log import LogFormatter
from tornado.gen import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop
from traitlets import Unicode, Bool, Type, Bytes, Float, default, validate
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from . import handlers, objects
from .tls import new_keypair
from .utils import cleanup_tmpdir


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
        help="The gateway authenticator class to use",
        config=True
    )

    cluster_class = Type(
        'dask_gateway.cluster.ClusterManager',
        klass='dask_gateway.cluster.ClusterManager',
        help="The gateway cluster manager class to use",
        config=True
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

    db_url = Unicode(
        'sqlite:///dask_gateway.sqlite',
        help="The URL for the database.",
        config=True
    )

    db_debug = Bool(
        False,
        help="If True, all database operations will be logged",
        config=True
    )

    temp_dir = Unicode(
        help="""
        Path to a directory to use to store temporary runtime files.

        The permissions on this directory must be restricted to ``0o700``. If
        the directory doesn't already exist, it will be created on startup and
        removed on shutdown.

        The default is to create a temporary directory ``"dask-gateway"`` in
        the system tmpdir default location.
        """,
        config=True
    )

    @default('temp_dir')
    def _temp_dir_default(self):
        return os.path.join(tempfile.gettempdir(), "dask-gateway")

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

    @property
    def api_url(self):
        return self.public_url + "/gateway/api"

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return
        self.load_config_file(self.config_file)
        self.init_logging()
        self.init_database()
        self.init_tempdir()
        self.init_scheduler_proxy()
        self.init_web_proxy()
        self.init_authenticator()
        self.init_tornado_application()
        self.init_state()

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

    def init_database(self):
        self.db = objects.make_engine(url=self.db_url, echo=self.db_debug)

    def init_tempdir(self):
        if os.path.exists(self.temp_dir):
            perm = stat.S_IMODE(os.stat(self.temp_dir).st_mode)
            if perm & (stat.S_IRWXO | stat.S_IRWXG):
                raise ValueError("Temporary directory %s has excessive permissions "
                                 "%r, should be at '0o700'" % (self.temp_dir, oct(perm)))
        else:
            self.log.debug("Creating temporary directory %r", self.temp_dir)
            os.mkdir(self.temp_dir, mode=0o700)
            weakref.finalize(self, cleanup_tmpdir, self.log, self.temp_dir)

    def init_state(self):
        self.username_to_user = {}
        self.cookie_to_user = {}
        self.clusters = {}
        self.token_to_cluster = {}

        # Temporary hashtable for loading
        id_to_user = {}

        # Load all existing users into memory
        for u in self.db.execute(objects.users.select()):
            user = objects.User(id=u.id, name=u.name, cookie=u.cookie)
            self.username_to_user[user.name] = user
            self.cookie_to_user[user.cookie] = user
            id_to_user[user.id] = user

        # Next load all existing clusters into memory
        for c in self.db.execute(objects.clusters.select()):
            user = id_to_user[c.user_id]
            state = json.loads(c.state)
            manager = self._new_cluster_manager(
                c.cluster_id, c.token, c.tls_cert, c.tls_key
            )
            manager.load_state(state)
            cluster = objects.Cluster(
                id=c.id,
                cluster_id=c.cluster_id,
                user=user,
                token=c.token,
                manager=manager,
                scheduler_address=c.scheduler_address,
                dashboard_address=c.dashboard_address,
                tls_cert=c.tls_cert,
                tls_key=c.tls_key
            )
            self.clusters[cluster.cluster_id] = cluster
            user.clusters[cluster.cluster_id] = cluster
            self.token_to_cluster[cluster.token] = cluster

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
            gateway=self,
            authenticator=self.authenticator,
            cookie_secret=self.cookie_secret,
            cookie_max_age_days=self.cookie_max_age_days
        )

    async def start_async(self):
        await self.start_scheduler_proxy()
        await self.start_web_proxy()
        await self.start_tornado_application()

    async def start_scheduler_proxy(self):
        try:
            await self.scheduler_proxy.start()
        except Exception:
            self.log.critical("Failed to start scheduler proxy", exc_info=True)
            self.exit(1)

    async def start_web_proxy(self):
        try:
            await self.web_proxy.start()
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

    def user_from_cookie(self, cookie):
        return self.cookie_to_user.get(cookie)

    def get_or_create_user(self, username):
        user = self.username_to_user.get(username)
        if user is None:
            cookie = uuid.uuid4().hex
            res = self.db.execute(
                objects.users.insert().values(name=username, cookie=cookie)
            )
            user = objects.User(
                id=res.inserted_primary_key[0],
                name=username,
                cookie=cookie
            )
            self.cookie_to_user[cookie] = user
            self.username_to_user[username] = user
        return user

    def cluster_from_token(self, token):
        return self.token_to_cluster.get(token)

    def _new_cluster_manager(self, cluster_id, token, tls_cert, tls_key):
        return self.cluster_class(
            cluster_id=cluster_id,
            api_token=token,
            api_url=self.api_url,
            temp_dir=self.temp_dir,
            tls_cert=tls_cert,
            tls_key=tls_key
        )

    def create_cluster(self, user):
        cluster_id = uuid.uuid4().hex
        token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_id)
        manager = self._new_cluster_manager(cluster_id, token, tls_cert, tls_key)
        state = json.dumps(manager.get_state()).encode('utf-8')

        res = self.db.execute(
            objects.clusters.insert().values(
                cluster_id=cluster_id,
                user_id=user.id,
                token=token,
                state=state,
                scheduler_address='',
                dashboard_address='',
                tls_cert=tls_cert,
                tls_key=tls_key
            )
        )
        cluster = objects.Cluster(
            id=res.inserted_primary_key[0],
            cluster_id=cluster_id,
            user=user,
            token=token,
            manager=manager,
            scheduler_address='',
            dashboard_address='',
            tls_cert=tls_cert,
            tls_key=tls_key
        )
        user.clusters[cluster_id] = cluster
        self.clusters[cluster_id] = cluster
        self.token_to_cluster[token] = cluster
        return cluster

    def start_cluster(self, cluster):
        async def start_cluster():
            self.log.debug("Starting cluster %s", cluster.cluster_id)
            await cluster.manager.start()
            self.log.debug("Cluster %s started", cluster.cluster_id)

            # Cluster has started, update state
            state = json.dumps(cluster.manager.get_state()).encode('utf-8')
            self.db.execute(
                objects.clusters
                .update()
                .where(objects.clusters.c.id == cluster.id)
                .values(state=state)
            )
        asyncio.ensure_future(start_cluster())

    def stop_cluster(self, cluster):
        async def stop_cluster():
            self.log.debug("Stopping cluster %s", cluster.cluster_id)
            # Remove routes from proxies if already set
            if cluster.scheduler_address:
                await self.web_proxy.delete_route(
                    "/gateway/clusters/" + cluster.cluster_id
                )
                await self.scheduler_proxy.delete_route(
                    "/" + cluster.cluster_id,
                )
            # Stop the cluster
            await cluster.manager.stop()
            self.log.debug("Cluster %s stopped", cluster.cluster_id)

            # Cluster has stopped, delete record
            self.db.execute(
                objects.clusters
                .delete()
                .where(objects.clusters.c.id == cluster.id)
            )
            del self.clusters[cluster.cluster_id]
            del self.token_to_cluster[cluster.token]
            del cluster.user.clusters[cluster.cluster_id]
        asyncio.ensure_future(stop_cluster())

    async def register_cluster(self, cluster, scheduler_address, dashboard_address):
        self.log.debug("Registering cluster %s", cluster.cluster_id)
        cluster.scheduler_address = scheduler_address
        cluster.dashboard_address = dashboard_address
        self.db.execute(
            objects.clusters
            .update()
            .where(objects.clusters.c.id == cluster.id)
            .values(scheduler_address=scheduler_address,
                    dashboard_address=dashboard_address)
        )
        await self.web_proxy.add_route(
            "/gateway/clusters/" + cluster.cluster_id,
            dashboard_address
        )
        await self.scheduler_proxy.add_route(
            "/" + cluster.cluster_id,
            scheduler_address
        )


main = DaskGateway.launch_instance


if __name__ == "__main__":
    main()
