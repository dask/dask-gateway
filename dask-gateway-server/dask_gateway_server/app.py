import asyncio
import json
import logging
import os
import signal
import stat
import tempfile
import weakref
from functools import partial
from urllib.parse import urlparse

from tornado import web
from tornado.log import LogFormatter
from tornado.gen import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from traitlets import Unicode, Bool, Bytes, Float, List, Instance, default, validate
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from . import handlers
from .managers import ClusterManager
from .auth import Authenticator
from .objects import (
    DataManager,
    WorkerStatus,
    ClusterStatus,
    timestamp,
    normalize_encrypt_key,
    is_in_memory_db,
)
from .limits import UserLimits
from .options import Options
from .proxy import SchedulerProxy, WebProxy
from .utils import (
    classname,
    cleanup_tmpdir,
    cancel_task,
    TaskPool,
    Type,
    timeout,
    ServerUrls,
)


# Override default values for logging
Application.log_level.default_value = "INFO"
Application.log_format.default_value = (
    "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(end_color)s %(message)s"
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

    cluster_manager_class = Type(
        "dask_gateway_server.managers.local.UnsafeLocalClusterManager",
        klass="dask_gateway_server.managers.ClusterManager",
        help="The gateway cluster manager class to use",
        config=True,
    )

    cluster_manager_options = Instance(
        Options,
        args=(),
        help="""
        User options for configuring the cluster manager.

        Allows users to specify configuration overrides when creating a new
        cluster manager. See the documentation for more information.
        """,
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

    stop_clusters_on_shutdown = Bool(
        True,
        help="""
        Whether to stop active clusters on gateway shutdown.

        If true, all active clusters will be stopped before shutting down the
        gateway. Set to False to leave active clusters running.
        """,
        config=True,
    )

    @validate("stop_clusters_on_shutdown")
    def _stop_clusters_on_shutdown_validate(self, proposal):
        if not proposal.value and is_in_memory_db(self.db_url):
            raise ValueError(
                "When using an in-memory database, `stop_clusters_on_shutdown` "
                "must be True"
            )
        return proposal.value

    check_cluster_timeout = Float(
        10,
        help="""
        Timeout (in seconds) before deciding a cluster is no longer active.

        When the gateway restarts, any clusters still marked as active have
        their status checked. This timeout sets the max time we allocate for
        checking a cluster's status before deciding that the cluster is no
        longer active.
        """,
    )

    db_url = Unicode(
        "sqlite:///:memory:",
        help="""
        The URL for the database. Default is in-memory only.

        If not in-memory, ``db_encrypt_keys`` must also be set.
        """,
        config=True,
    )

    db_encrypt_keys = List(
        help="""
        A list of keys to use to encrypt private data in the database. Can also
        be set by the environment variable ``DASK_GATEWAY_ENCRYPT_KEYS``, where
        the value is a ``;`` delimited string of encryption keys.

        Each key should be a base64-encoded 32 byte value, and should be
        cryptographically random. Lacking other options, openssl can be used to
        generate a single key via:

        .. code-block:: shell

            $ openssl rand -base64 32

        A single key is valid, multiple keys can be used to support key rotation.
        """,
        config=True,
    )

    @default("db_encrypt_keys")
    def _db_encrypt_keys_default(self):
        keys = os.environb.get(b"DASK_GATEWAY_ENCRYPT_KEYS", b"").strip()
        if not keys:
            return []
        return [normalize_encrypt_key(k) for k in keys.split(b";") if k.strip()]

    @validate("db_encrypt_keys")
    def _db_encrypt_keys_validate(self, proposal):
        if not proposal.value and not is_in_memory_db(self.db_url):
            raise ValueError(
                "Must configure `db_encrypt_keys`/`DASK_GATEWAY_ENCRYPT_KEYS` "
                "when not using an in-memory database"
            )
        return [normalize_encrypt_key(k) for k in proposal.value]

    db_debug = Bool(
        False, help="If True, all database operations will be logged", config=True
    )

    db_cleanup_period = Float(
        600,
        help="""
        Time (in seconds) between database cleanup tasks.

        This sets how frequently old records are removed from the database.
        This shouldn't be too small (to keep the overhead low), but should be
        smaller than ``db_record_max_age`` (probably by an order of magnitude).
        """,
        config=True,
    )

    db_cluster_max_age = Float(
        3600 * 24,
        help="""
        Max time (in seconds) to keep around records of completed clusters.

        Every ``db_cleanup_period``, completed clusters older than
        ``db_cluster_max_age`` are removed from the database.
        """,
        config=True,
    )

    temp_dir = Unicode(
        help="""
        Path to a directory to use to store temporary runtime files.

        The permissions on this directory must be restricted to ``0o700``. If
        the directory doesn't already exist, it will be created on startup and
        removed on shutdown.

        The default is to create a temporary directory
        ``"dask-gateway-<UUID>"`` in the system tmpdir default location.
        """,
        config=True,
    )

    @default("temp_dir")
    def _temp_dir_default(self):
        temp_dir = tempfile.mkdtemp(prefix="dask-gateway-")
        self.log.debug("Creating temporary directory %r", temp_dir)
        weakref.finalize(self, cleanup_tmpdir, self.log, temp_dir)
        return temp_dir

    _log_formatter_cls = LogFormatter

    classes = List([ClusterManager, Authenticator, WebProxy, SchedulerProxy])

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return
        self.log.info("Starting dask-gateway-server - version %s", VERSION)
        self.load_config_file(self.config_file)
        self.log.info("Cluster manager: %r", classname(self.cluster_manager_class))
        self.log.info("Authenticator: %r", classname(self.authenticator_class))
        self.init_logging()
        self.init_tempdir()
        self.init_asyncio()
        self.init_server_urls()
        self.init_scheduler_proxy()
        self.init_web_proxy()
        self.init_authenticator()
        self.init_user_limits()
        self.init_database()
        self.init_tornado_application()

    def init_logging(self):
        # Prevent double log messages from tornado
        self.log.propagate = False

        # hook up tornado's loggers to our app handlers
        from tornado.log import app_log, access_log, gen_log

        for log in (app_log, access_log, gen_log):
            log.name = self.log.name
            log.handlers[:] = []
        logger = logging.getLogger("tornado")
        logger.handlers[:] = []
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log.level)

    def init_tempdir(self):
        if os.path.exists(self.temp_dir):
            perm = stat.S_IMODE(os.stat(self.temp_dir).st_mode)
            if perm & (stat.S_IRWXO | stat.S_IRWXG):
                raise ValueError(
                    "Temporary directory %s has excessive permissions "
                    "%r, should be at '0o700'" % (self.temp_dir, oct(perm))
                )
        else:
            self.log.debug("Creating temporary directory %r", self.temp_dir)
            os.mkdir(self.temp_dir, mode=0o700)
            weakref.finalize(self, cleanup_tmpdir, self.log, self.temp_dir)

    def init_asyncio(self):
        self.task_pool = TaskPool()

    def init_server_urls(self):
        """Initialize addresses from configuration"""
        self.public_urls = ServerUrls(self.public_url, self.public_connect_url)
        self.private_urls = ServerUrls(self.private_url, self.private_connect_url)
        if not self.gateway_url:
            gateway_url = f"tls://{self.public_urls.bind_host}:8786"
        else:
            gateway_url = self.gateway_url
        self.gateway_urls = ServerUrls(gateway_url)
        # Additional common url
        self.api_url = self.public_urls.connect_url + "/gateway/api"

    def init_scheduler_proxy(self):
        self.scheduler_proxy = self.scheduler_proxy_class(
            parent=self, log=self.log, public_urls=self.gateway_urls
        )

    def init_web_proxy(self):
        self.web_proxy = self.web_proxy_class(
            parent=self,
            log=self.log,
            public_urls=self.public_urls,
            tls_cert=self.tls_cert,
            tls_key=self.tls_key,
        )

    def init_authenticator(self):
        self.authenticator = self.authenticator_class(parent=self, log=self.log)

    def init_user_limits(self):
        self.user_limits = UserLimits(parent=self, log=self.log)

    def init_database(self):
        self.db = DataManager(
            url=self.db_url, echo=self.db_debug, encrypt_keys=self.db_encrypt_keys
        )

    def init_tornado_application(self):
        self.handlers = list(handlers.default_handlers)
        self.tornado_application = web.Application(
            self.handlers,
            log=self.log,
            gateway=self,
            authenticator=self.authenticator,
            cookie_secret=self.cookie_secret,
            cookie_max_age_days=self.cookie_max_age_days,
        )

    async def start_async(self):
        self.init_signal()
        await self.start_scheduler_proxy()
        await self.start_web_proxy()
        await self.load_database_state()
        await self.start_tornado_application()

    async def start_scheduler_proxy(self):
        await self.scheduler_proxy.start()

    async def start_web_proxy(self):
        await self.web_proxy.start()

    def create_cluster_manager(self, options):
        config = self.cluster_manager_options.get_configuration(options)
        return self.cluster_manager_class(
            parent=self,
            log=self.log,
            task_pool=self.task_pool,
            temp_dir=self.temp_dir,
            api_url=self.api_url,
            **config,
        )

    def init_cluster_manager(self, manager, cluster):
        manager.username = cluster.user.name
        manager.cluster_name = cluster.name
        manager.api_token = cluster.token
        manager.tls_cert = cluster.tls_cert
        manager.tls_key = cluster.tls_key

    async def load_database_state(self):
        self.db.load_database_state()

        active_clusters = list(self.db.active_clusters())
        if active_clusters:
            self.log.info(
                "Gateway was stopped with %d active clusters, "
                "checking cluster status...",
                len(active_clusters),
            )

            tasks = (self.check_cluster(c) for c in active_clusters)
            results = await asyncio.gather(*tasks, return_exceptions=True)

            n_clusters = 0
            for c, r in zip(active_clusters, results):
                if isinstance(r, Exception):
                    self.log.error(
                        "Error while checking status of cluster %s", c.name, exc_info=r
                    )
                elif r:
                    n_clusters += 1

            self.log.info(
                "All clusters have been checked, there are %d active clusters",
                n_clusters,
            )

        self.task_pool.create_background_task(self.cleanup_database())

    async def cleanup_database(self):
        while True:
            try:
                n = self.db.cleanup_expired(self.db_cluster_max_age)
            except Exception as exc:
                self.log.error(
                    "Error while cleaning expired database records", exc_info=exc
                )
            else:
                self.log.debug("Removed %d expired clusters from the database", n)
            await asyncio.sleep(self.db_cleanup_period)

    async def check_cluster(self, cluster):
        cluster.manager = self.create_cluster_manager(cluster.options)
        self.init_cluster_manager(cluster.manager, cluster)

        if cluster.status == ClusterStatus.RUNNING:
            client = AsyncHTTPClient()
            url = "%s/api/status" % cluster.api_address
            req = HTTPRequest(
                url, method="GET", headers={"Authorization": "token %s" % cluster.token}
            )
            try:
                resp = await asyncio.wait_for(
                    client.fetch(req), timeout=self.check_cluster_timeout
                )
                workers = json.loads(resp.body.decode("utf8", "replace"))["workers"]
                running = True
            except asyncio.CancelledError:
                raise
            except Exception:
                running = False
                workers = []
        else:
            # Gateway was stopped before cluster fully started.
            running = False
            workers = []

        if running:
            # Cluster is running, update our state to match
            await self.add_cluster_to_proxies(cluster)

            # Update our set of workers to match
            actual_workers = set(workers)
            to_stop = []
            for w in cluster.active_workers():
                if w.name in actual_workers:
                    self.mark_worker_running(cluster, w)
                else:
                    to_stop.append(w)

            tasks = (self.stop_worker(cluster, w, failed=True) for w in to_stop)
            await asyncio.gather(*tasks, return_exceptions=False)

            # Start the periodic monitor
            self.start_cluster_status_monitor(cluster)
        else:
            # Cluster is not available, shut it down
            await self.stop_cluster(cluster, failed=True)

        return running

    async def start_tornado_application(self):
        self.http_server = self.tornado_application.listen(
            self.private_urls.bind_port, address=self.private_urls.bind_host
        )
        self.log.info("Gateway private API serving at %s", self.private_urls.bind_url)
        await self.web_proxy.add_route(
            self.public_urls.connect.path + "/gateway/", self.private_urls.connect_url
        )
        self.log.info("Dask-Gateway started successfully!")
        for name, urls in [
            ("Public address", self.public_urls._to_log),
            ("Proxy address", self.gateway_urls._to_log),
        ]:
            if len(urls) == 2:
                self.log.info("- %s at %s or %s", name, *urls)
            else:
                self.log.info("- %s at %s", name, *urls)

    async def start_or_exit(self):
        try:
            await self.start_async()
        except Exception:
            self.log.critical("Failed to start gateway, shutting down", exc_info=True)
            await self.stop_async(stop_event_loop=False)
            self.exit(1)

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()
        AsyncIOMainLoop().install()
        loop = IOLoop.current()
        loop.add_callback(self.start_or_exit)
        try:
            loop.start()
        except KeyboardInterrupt:
            print("\nInterrupted")

    def init_signal(self):
        loop = asyncio.get_event_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.handle_shutdown_signal, s)

    def handle_shutdown_signal(self, sig):
        self.log.info("Received signal %s, initiating shutdown...", sig.name)
        asyncio.ensure_future(self.stop_async())

    async def _stop_async(self, timeout=5):
        # Stop the server to prevent new requests
        if hasattr(self, "http_server"):
            self.http_server.stop()

        # If requested, shutdown any active clusters
        if self.stop_clusters_on_shutdown:
            tasks = {
                asyncio.ensure_future(self.stop_cluster(c, failed=True)): c
                for c in self.db.active_clusters()
            }
            if tasks:
                self.log.info("Stopping all active clusters...")
                done, pending = await asyncio.wait(tasks.keys())
                for f in done:
                    try:
                        await f
                    except Exception as exc:
                        cluster = tasks[f]
                        self.log.error(
                            "Failed to stop cluster %s for user %s",
                            cluster.name,
                            cluster.user.name,
                            exc_info=exc,
                        )
        else:
            self.log.info("Leaving any active clusters running")

        if hasattr(self, "task_pool"):
            await self.task_pool.close(timeout=timeout)

        # Shutdown the proxies
        if hasattr(self, "scheduler_proxy"):
            self.scheduler_proxy.stop()
        if hasattr(self, "web_proxy"):
            self.web_proxy.stop()

    async def stop_async(self, timeout=5, stop_event_loop=True):
        try:
            await self._stop_async(timeout=timeout)
        except Exception:
            self.log.error("Error while shutting down:", exc_info=True)
        # Stop the event loop
        if stop_event_loop:
            IOLoop.current().stop()

    def start_cluster_status_monitor(self, cluster):
        cluster._status_monitor = self.task_pool.create_background_task(
            self._cluster_status_monitor(cluster)
        )

    def stop_cluster_status_monitor(self, cluster):
        if cluster._status_monitor is not None:
            cluster._status_monitor.cancel()
            cluster._status_monitor = None

    async def _cluster_status_monitor(self, cluster):
        while True:
            try:
                res = await cluster.manager.cluster_status(cluster.state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error while checking cluster %s status", cluster.name, exc_info=exc
                )
            else:
                running, msg = res if isinstance(res, tuple) else (res, None)
                if not running:
                    if msg:
                        self.log.warning(
                            "Cluster %s stopped unexpectedly: %s", cluster.name, msg
                        )
                    else:
                        self.log.warning(
                            "Cluster %s stopped unexpectedly", cluster.name
                        )
                    self.schedule_stop_cluster(cluster, failed=True)
                    return
            await asyncio.sleep(cluster.manager.cluster_status_period)

    async def _start_cluster(self, cluster):
        self.log.info(
            "Starting cluster %s for user %s...", cluster.name, cluster.user.name
        )

        # Walk through the startup process, saving state as updates occur
        async for state in cluster.manager.start_cluster():
            self.log.debug("State update for cluster %s", cluster.name)
            self.db.update_cluster(cluster, state=state)

        # Move cluster to started
        self.db.update_cluster(cluster, status=ClusterStatus.STARTED)

    async def start_cluster(self, cluster):
        """Start the cluster.

        Returns True if successfully started, False otherwise.
        """
        try:
            async with timeout(cluster.manager.cluster_start_timeout):
                await self._start_cluster(cluster)
                self.log.info(
                    "Cluster %s has started, waiting for connection", cluster.name
                )
                self.start_cluster_status_monitor(cluster)
                addresses = await cluster._connect_future
        except asyncio.TimeoutError:
            self.log.warning(
                "Cluster %s startup timed out after %.1f seconds",
                cluster.name,
                cluster.manager.cluster_start_timeout,
            )
            return False
        except asyncio.CancelledError:
            # Catch separately to avoid in generic handler below
            raise
        except Exception as exc:
            self.log.error(
                "Error while starting cluster %s", cluster.name, exc_info=exc
            )
            return False

        scheduler_address, dashboard_address, api_address = addresses
        self.log.info("Cluster %s connected at %s", cluster.name, scheduler_address)

        # Mark cluster as running
        self.db.update_cluster(
            cluster,
            scheduler_address=scheduler_address,
            dashboard_address=dashboard_address,
            api_address=api_address,
            status=ClusterStatus.RUNNING,
        )

        # Register routes with proxies
        await self.add_cluster_to_proxies(cluster)

        return True

    async def add_cluster_to_proxies(self, cluster):
        if cluster.dashboard_address:
            await self.web_proxy.add_route(
                self.public_urls.connect.path + "/gateway/clusters/" + cluster.name,
                cluster.dashboard_address,
            )
        await self.scheduler_proxy.add_route(
            "/" + cluster.name, cluster.scheduler_address
        )

    def start_new_cluster(self, user, request):
        # Process the user provided options
        options = self.cluster_manager_options.parse_options(request)
        manager = self.create_cluster_manager(options)

        # Check if allowed to create cluster
        allowed, msg = self.user_limits.check_cluster_limits(
            user, manager.scheduler_memory, manager.scheduler_cores
        )
        if not allowed:
            raise Exception(msg)

        # Finish initializing the object states
        cluster = self.db.create_cluster(
            user, options, manager.scheduler_memory, manager.scheduler_cores
        )
        cluster.manager = manager
        self.init_cluster_manager(cluster.manager, cluster)

        # Launch the cluster startup task
        f = self.task_pool.create_task(self.start_cluster(cluster))
        f.add_done_callback(partial(self._monitor_start_cluster, cluster=cluster))
        cluster._start_future = f

        return cluster

    def _monitor_start_cluster(self, future, cluster=None):
        try:
            if future.result():
                # Startup succeeded, nothing to do
                return
        except asyncio.CancelledError:
            # Startup cancelled, cleanup is handled separately
            return
        except Exception as exc:
            self.log.error(
                "Unexpected error while starting cluster %s", cluster.name, exc_info=exc
            )

        self.schedule_stop_cluster(cluster, failed=True)

    async def stop_cluster(self, cluster, failed=False):
        if cluster.status >= ClusterStatus.STOPPING:
            return

        self.log.info("Stopping cluster %s...", cluster.name)

        # Move cluster to stopping
        self.db.update_cluster(cluster, status=ClusterStatus.STOPPING)

        # Stop the periodic monitor, if present
        self.stop_cluster_status_monitor(cluster)

        # If running, cancel running start task
        await cancel_task(cluster._start_future)

        # Remove routes from proxies if already set
        await self.web_proxy.delete_route("/gateway/clusters/" + cluster.name)
        await self.scheduler_proxy.delete_route("/" + cluster.name)

        # Shutdown individual workers if no bulk shutdown supported
        if not cluster.manager.supports_bulk_shutdown:
            tasks = (self.stop_worker(cluster, w) for w in cluster.active_workers())
            await asyncio.gather(*tasks, return_exceptions=True)

        # Shutdown the cluster
        try:
            await cluster.manager.stop_cluster(cluster.state)
        except Exception as exc:
            self.log.error("Failed to shutdown cluster %s", cluster.name, exc_info=exc)

        # If we shut the workers down in bulk, cleanup their internal state now
        if cluster.manager.supports_bulk_shutdown:
            tasks = (self.stop_worker(cluster, w) for w in cluster.active_workers())
            await asyncio.gather(*tasks, return_exceptions=True)

        # Update the cluster status
        status = ClusterStatus.FAILED if failed else ClusterStatus.STOPPED
        self.db.update_cluster(cluster, status=status, stop_time=timestamp())
        cluster.pending.clear()

        self.log.info("Stopped cluster %s", cluster.name)

    def schedule_stop_cluster(self, cluster, failed=False):
        self.task_pool.create_task(self.stop_cluster(cluster, failed=failed))

    async def scale(self, cluster, total):
        """Scale cluster to total workers"""
        async with cluster.lock:
            n_active = len(cluster.active_workers())
            delta = total - n_active
            if delta == 0:
                return n_active, None
            self.log.info(
                "Scaling cluster %s to %d workers, a delta of %d",
                cluster.name,
                total,
                delta,
            )
            if delta > 0:
                actual_delta, msg = self.scale_up(cluster, delta)
            else:
                actual_delta = -await self.scale_down(cluster, -delta)
                msg = None
            return n_active + actual_delta, msg

    def scale_up(self, cluster, n_start):
        # Check how many workers we're allowed_to_start
        n_allowed, msg = self.user_limits.check_scale_limits(
            cluster,
            n_start,
            memory=cluster.manager.worker_memory,
            cores=cluster.manager.worker_cores,
        )
        for _ in range(n_allowed):
            w = self.db.create_worker(
                cluster, cluster.manager.worker_memory, cluster.manager.worker_cores
            )
            w._start_future = self.task_pool.create_task(self.start_worker(cluster, w))
            w._start_future.add_done_callback(
                partial(self._monitor_start_worker, worker=w, cluster=cluster)
            )
        return n_allowed, msg

    async def _start_worker(self, cluster, worker):
        self.log.info("Starting worker %s for cluster %s...", worker.name, cluster.name)

        # Walk through the startup process, saving state as updates occur
        async for state in cluster.manager.start_worker(worker.name, cluster.state):
            self.db.update_worker(worker, state=state)

        # Move worker to started
        self.db.update_worker(worker, status=WorkerStatus.STARTED)

    async def _worker_status_monitor(self, cluster, worker):
        while True:
            try:
                res = await cluster.manager.worker_status(
                    worker.name, worker.state, cluster.state
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error while checking worker %s status", worker.name, exc_info=exc
                )
            else:
                running, msg = res if isinstance(res, tuple) else (res, None)
                if not running:
                    return msg
            await asyncio.sleep(cluster.manager.worker_status_period)

    async def start_worker(self, cluster, worker):
        worker_status_monitor = None
        try:
            async with timeout(cluster.manager.worker_start_timeout):
                # Submit the worker
                await self._start_worker(cluster, worker)

                self.log.info(
                    "Worker %s has started, waiting for connection", worker.name
                )

                # Wait for the worker to connect, periodically checking its status
                worker_status_monitor = asyncio.ensure_future(
                    self._worker_status_monitor(cluster, worker)
                )
                done, pending = await asyncio.wait(
                    (worker_status_monitor, worker._connect_future),
                    return_when=asyncio.FIRST_COMPLETED,
                )
        except Exception as exc:
            if worker_status_monitor is not None:
                worker_status_monitor.cancel()
            if type(exc) is asyncio.CancelledError:
                raise
            elif type(exc) is asyncio.TimeoutError:
                self.log.warning(
                    "Worker %s startup timed out after %.1f seconds",
                    worker.name,
                    cluster.manager.worker_start_timeout,
                )
            else:
                self.log.error("Error while starting worker %s", worker, exc_info=exc)
            return False

        # Check monitor for failures
        if worker_status_monitor in done:
            # Failure occurred
            msg = worker_status_monitor.result()
            if msg:
                self.log.warning(
                    "Worker %s failed during startup: %s", worker.name, msg
                )
            else:
                self.log.warning("Worker %s failed during startup", worker.name)
            return False
        else:
            worker_status_monitor.cancel()

        self.log.info("Worker %s connected to cluster %s", worker.name, cluster.name)

        # Mark worker as running
        self.mark_worker_running(cluster, worker)

        return True

    def mark_worker_running(self, cluster, worker):
        if worker.status != WorkerStatus.RUNNING:
            cluster.manager.on_worker_running(worker.name, worker.state, cluster.state)
            self.db.update_worker(worker, status=WorkerStatus.RUNNING)
            cluster.pending.discard(worker.name)

    def _monitor_start_worker(self, future, worker=None, cluster=None):
        try:
            if future.result():
                # Startup succeeded, nothing to do
                return
        except asyncio.CancelledError:
            # Startup cancelled, cleanup is handled separately
            self.log.debug("Cancelled worker %s", worker.name)
            return
        except Exception as exc:
            self.log.error(
                "Unexpected error while starting worker %s for cluster %s",
                worker.name,
                cluster.name,
                exc_info=exc,
            )

        self.schedule_stop_worker(cluster, worker, failed=True)

    async def stop_worker(self, cluster, worker, failed=False):
        # Already stopping elsewhere, return
        if worker.status >= WorkerStatus.STOPPING:
            return

        self.log.info("Stopping worker %s for cluster %s", worker.name, cluster.name)

        # Move worker to stopping
        self.db.update_worker(worker, status=WorkerStatus.STOPPING)
        cluster.pending.discard(worker.name)

        # Cancel a pending start if needed
        await cancel_task(worker._start_future)

        # Shutdown the worker
        if not cluster.manager.supports_bulk_shutdown:
            try:
                await cluster.manager.stop_worker(
                    worker.name, worker.state, cluster.state
                )
            except Exception as exc:
                self.log.error(
                    "Failed to shutdown worker %s for cluster %s",
                    worker.name,
                    cluster.name,
                    exc_info=exc,
                )

        # Update the worker status
        status = WorkerStatus.FAILED if failed else WorkerStatus.STOPPED
        self.db.update_worker(worker, status=status, stop_time=timestamp())

        self.log.info("Stopped worker %s", worker.name)

    def schedule_stop_worker(self, cluster, worker, failed=False):
        self.task_pool.create_task(self.stop_worker(cluster, worker, failed=failed))

    def maybe_fail_worker(self, cluster, worker):
        # Ignore if cluster or worker isn't active (
        if (
            cluster.status != ClusterStatus.RUNNING
            or worker.status >= WorkerStatus.STOPPING
        ):
            return
        self.schedule_stop_worker(cluster, worker, failed=True)

    async def scale_down(self, cluster, n_stop):
        if cluster.pending:
            if len(cluster.pending) > n_stop:
                to_stop = [cluster.pending.pop() for _ in range(n_stop)]
            else:
                to_stop = list(cluster.pending)
                cluster.pending.clear()
            to_stop = [cluster.workers[n] for n in to_stop]

            self.log.debug(
                "Stopping %d pending workers for cluster %s", len(to_stop), cluster.name
            )
            for w in to_stop:
                self.schedule_stop_worker(cluster, w)
            n_stop -= len(to_stop)

        if n_stop:
            # Request scheduler shutdown n_stop workers
            client = AsyncHTTPClient()
            body = json.dumps({"remove_count": n_stop})
            url = "%s/api/scale_down" % cluster.api_address
            req = HTTPRequest(
                url,
                method="POST",
                headers={
                    "Authorization": "token %s" % cluster.token,
                    "Content-type": "application/json",
                },
                body=body,
            )
            resp = await client.fetch(req)
            data = json.loads(resp.body.decode("utf8", "replace"))
            to_stop = [cluster.workers[n] for n in data["workers_closed"]]

            self.log.debug(
                "Stopping %d running workers for cluster %s", len(to_stop), cluster.name
            )
            for w in to_stop:
                self.schedule_stop_worker(cluster, w)

            return len(to_stop)


main = DaskGateway.launch_instance
