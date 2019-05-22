import asyncio
import json
import logging
import os
import socket
import stat
import tempfile
import weakref
from functools import partial
from urllib.parse import urlparse, urlunparse

from tornado import web
from tornado.log import LogFormatter
from tornado.gen import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from traitlets import Unicode, Bool, Type, Bytes, Float, default, validate, List
from traitlets.config import Application, catch_config_error

from . import __version__ as VERSION
from . import handlers, objects
from .cluster import ClusterManager
from .auth import Authenticator
from .objects import WorkerStatus, ClusterStatus
from .proxy import SchedulerProxy, WebProxy
from .utils import cleanup_tmpdir, cancel_task


# Override default values for logging
Application.log_level.default_value = "INFO"
Application.log_format.default_value = (
    "%(color)s[%(levelname)1.1s %(asctime)s.%(msecs).03d "
    "%(name)s]%(end_color)s %(message)s"
)


class GenerateConfig(Application):
    """Generate and write a default configuration file"""

    name = "dask-gateway generate-config"
    version = VERSION
    description = "Generate and write a default configuration file"

    examples = """

        dask-gateway generate-config
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

    name = "dask-gateway"
    version = VERSION

    description = """Start a Dask Gateway server"""

    examples = """

    Start the server on 10.0.1.2:8080:

        dask-gateway --public-url 10.0.1.2:8080
    """

    subcommands = {
        "generate-config": (
            "dask_gateway_server.app.GenerateConfig",
            "Generate a default config file",
        )
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
        "dask_gateway_server.auth.KerberosAuthenticator",
        klass="dask_gateway_server.auth.Authenticator",
        help="The gateway authenticator class to use",
        config=True,
    )

    cluster_manager_class = Type(
        "dask_gateway_server.local_cluster.LocalClusterManager",
        klass="dask_gateway_server.cluster.ClusterManager",
        help="The gateway cluster manager class to use",
        config=True,
    )

    public_url = Unicode(
        "http://:8000",
        help="The public facing URL of the whole Dask Gateway application",
        config=True,
    )

    gateway_url = Unicode(
        "tls://:8786", help="The URL that Dask clients will connect to", config=True
    )

    private_url = Unicode(
        "http://127.0.0.1:8081",
        help="The gateway's private URL used for internal communication",
        config=True,
    )

    @validate("public_url", "gateway_url", "private_url")
    def _resolve_hostname(self, proposal):
        url = proposal.value
        parsed = urlparse(url)
        if parsed.hostname in {"", "0.0.0.0"}:
            host = socket.gethostname()
            parsed = parsed._replace(netloc="%s:%i" % (host, parsed.port))
            url = urlunparse(parsed)
        return url

    cookie_secret = Bytes(
        help="""The cookie secret to use to encrypt cookies.

        Loaded from the DASK_GATEWAY_COOKIE_SECRET environment variable by
        default.
        """,
        config=True,
    )

    cookie_max_age_days = Float(
        7,
        help="""Number of days for a login cookie to be valid.
        Default is one week.
        """,
        config=True,
    )

    db_url = Unicode(
        "sqlite:///dask_gateway.sqlite", help="The URL for the database.", config=True
    )

    db_debug = Bool(
        False, help="If True, all database operations will be logged", config=True
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

    @default("cookie_secret")
    def _cookie_secret_default(self):
        secret = os.environb.get(b"DASK_GATEWAY_COOKIE_SECRET", b"")
        if not secret:
            self.log.info("Generating new cookie secret")
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

    _log_formatter_cls = LogFormatter

    classes = List([ClusterManager, Authenticator, WebProxy, SchedulerProxy])

    @property
    def api_url(self):
        return self.public_url + "/gateway/api"

    def create_task(self, task):
        out = asyncio.ensure_future(task)
        self.pending_tasks.add(out)
        return out

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
        self.init_cluster_manager()
        self.init_authenticator()
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

    def init_database(self):
        self.db = objects.DataManager(url=self.db_url, echo=self.db_debug)

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

    def init_scheduler_proxy(self):
        self.scheduler_proxy = self.scheduler_proxy_class(
            parent=self, log=self.log, public_url=self.gateway_url
        )

    def init_web_proxy(self):
        self.web_proxy = self.web_proxy_class(
            parent=self, log=self.log, public_url=self.public_url
        )

    def init_cluster_manager(self):
        self.cluster_manager = self.cluster_manager_class(
            parent=self, log=self.log, temp_dir=self.temp_dir, api_url=self.api_url
        )

    def init_authenticator(self):
        self.authenticator = self.authenticator_class(parent=self, log=self.log)

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
        self.pending_tasks = weakref.WeakSet()

    async def start_async(self):
        await self.start_scheduler_proxy()
        await self.start_web_proxy()
        await self.start_tornado_application()

    async def stop_async(self, timeout=5):
        if hasattr(self, "http_server"):
            self.http_server.stop()
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *getattr(self, "pending_tasks", ()), return_exceptions=True
                ),
                timeout,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        if hasattr(self, "scheduler_proxy"):
            self.scheduler_proxy.stop()
        if hasattr(self, "web_proxy"):
            self.web_proxy.stop()
        IOLoop.current().stop()

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

    async def _start_cluster(self, cluster):
        self.log.debug("Cluster %s is starting...", cluster.name)

        # Walk through the startup process, saving state as updates occur
        async for state in self.cluster_manager.start_cluster(cluster.info):
            self.log.debug("State update for cluster %s", cluster.name)
            self.db.update_cluster(cluster, state=state)

        # Move cluster to started
        self.db.update_cluster(cluster, status=ClusterStatus.STARTED)

    async def start_cluster(self, cluster):
        """Start the cluster.

        Returns True if successfully started, False otherwise.
        """
        try:
            await asyncio.wait_for(
                self._start_cluster(cluster),
                timeout=self.cluster_manager.cluster_start_timeout,
            )
        except asyncio.TimeoutError:
            self.log.warning(
                "Cluster %s startup timed out after %d seconds",
                cluster.name,
                self.cluster_manager.cluster_start_timeout,
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

        self.log.debug("Cluster %s has started, waiting for connection", cluster.name)

        try:
            addresses = await asyncio.wait_for(
                cluster._connect_future,
                timeout=self.cluster_manager.cluster_connect_timeout,
            )
            scheduler_address, dashboard_address, api_address = addresses
        except asyncio.TimeoutError:
            self.log.warning(
                "Cluster %s failed to connect after %d seconds",
                cluster.name,
                self.cluster_manager.cluster_connect_timeout,
            )
            return False

        self.log.debug("Cluster %s connected at %s", cluster.name, scheduler_address)

        # Register routes with proxies
        await self.web_proxy.add_route(
            "/gateway/clusters/" + cluster.name, dashboard_address
        )
        await self.scheduler_proxy.add_route("/" + cluster.name, scheduler_address)

        # Mark cluster as running
        self.db.update_cluster(
            cluster,
            scheduler_address=scheduler_address,
            dashboard_address=dashboard_address,
            api_address=api_address,
            status=ClusterStatus.RUNNING,
        )
        return True

    def start_new_cluster(self, user):
        cluster = self.db.create_cluster(user)
        f = self.create_task(self.start_cluster(cluster))
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
        self.log.debug("Cluster %s is stopping...", cluster.name)

        # If running, cancel running start task
        await cancel_task(cluster._start_future)

        # Move cluster to stopping
        self.db.update_cluster(cluster, status=ClusterStatus.STOPPING)

        # Remove routes from proxies if already set
        await self.web_proxy.delete_route("/gateway/clusters/" + cluster.name)
        await self.scheduler_proxy.delete_route("/" + cluster.name)

        # Shutdown individual workers if no bulk shutdown supported
        if not self.cluster_manager.supports_bulk_shutdown:
            tasks = (self.stop_worker(cluster, w) for w in cluster.active_workers)
            await asyncio.gather(*tasks, return_exceptions=True)

        # Shutdown the cluster
        await self.cluster_manager.stop_cluster(cluster.info, cluster.state)

        # Update the cluster status
        status = ClusterStatus.FAILED if failed else ClusterStatus.STOPPED
        self.db.update_cluster(cluster, status=status)
        cluster.pending.clear()

        self.log.debug("Cluster %s stopped", cluster.name)

    def schedule_stop_cluster(self, cluster, failed=False):
        self.create_task(self.stop_cluster(cluster, failed=failed))

    async def scale(self, cluster, total):
        """Scale cluster to total workers"""
        async with cluster.lock:
            delta = total - len(cluster.active_workers)
            if delta == 0:
                return
            self.log.debug(
                "Scaling cluster %s to %d workers, a delta of %d",
                cluster.name,
                total,
                delta,
            )
            if delta > 0:
                await self.scale_up(cluster, delta)
            else:
                await self.scale_down(cluster, -delta)

    async def scale_up(self, cluster, n_start):
        for _ in range(n_start):
            w = self.db.create_worker(cluster)
            w._start_future = self.create_task(self.start_worker(cluster, w))
            w._start_future.add_done_callback(
                partial(self._monitor_start_worker, worker=w, cluster=cluster)
            )

    async def _start_worker(self, cluster, worker):
        self.log.debug("Starting worker %r for cluster %r", worker.name, cluster.name)

        # Walk through the startup process, saving state as updates occur
        async for state in self.cluster_manager.start_worker(
            worker.name, cluster.info, cluster.state
        ):
            self.db.update_worker(worker, state=state)

        # Move worker to started
        self.db.update_worker(worker, status=WorkerStatus.STARTED)

    async def start_worker(self, cluster, worker):
        try:
            await asyncio.wait_for(
                self._start_worker(cluster, worker),
                timeout=self.cluster_manager.worker_start_timeout,
            )
        except asyncio.TimeoutError:
            self.log.warning(
                "Worker %s startup timed out after %d seconds",
                worker.name,
                self.cluster_manager.worker_start_timeout,
            )
            return False
        except asyncio.CancelledError:
            # Catch separately to avoid in generic handler below
            raise
        except Exception as exc:
            self.log.error("Error while starting worker %s", worker, exc_info=exc)
            return False

        self.log.debug("Worker %s has started, waiting for connection", worker.name)

        try:
            await asyncio.wait_for(
                worker._connect_future,
                timeout=self.cluster_manager.worker_connect_timeout,
            )
        except asyncio.TimeoutError:
            self.log.warning(
                "Worker %s failed to connect after %d seconds",
                worker.name,
                self.cluster_manager.worker_connect_timeout,
            )
            return False

        self.log.debug("Worker %s connected to cluster %s", worker.name, cluster.name)

        # Mark worker as running
        self.db.update_worker(worker, status=WorkerStatus.RUNNING)
        cluster.pending.discard(worker.name)
        return True

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
        self.log.debug("Stopping worker %s for cluster %s", worker.name, cluster.name)

        # Cancel a pending start if needed
        await cancel_task(worker._start_future)

        # Move worker to stopping
        self.db.update_worker(worker, status=WorkerStatus.STOPPING)
        cluster.pending.discard(worker.name)

        # Shutdown the worker
        try:
            await self.cluster_manager.stop_worker(
                worker.name, worker.state, cluster.info, cluster.state
            )
        except Exception as exc:
            self.log.error("Failed to shutdown worker %s for cluster %s", exc_info=exc)

        # Update the worker status
        status = WorkerStatus.FAILED if failed else WorkerStatus.STOPPED
        self.db.update_worker(worker, status=status)

        self.log.debug("Worker %s stopped", worker.name)

    def schedule_stop_worker(self, cluster, worker, failed=False):
        self.create_task(self.stop_worker(cluster, worker, failed=failed))

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


main = DaskGateway.launch_instance


if __name__ == "__main__":
    main()
