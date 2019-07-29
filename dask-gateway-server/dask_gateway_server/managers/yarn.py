import asyncio
import os
from collections import OrderedDict
from contextlib import contextmanager

try:
    import skein
except ImportError:
    raise ImportError(
        "'%s.YarnClusterManager' requires 'skein' as a dependency. "
        "To install required dependencies, use:\n"
        "  $ pip install dask-gateway-server[yarn]\n"
        "or\n"
        "  $ conda install dask-gateway-server-yarn -c conda-forge\n" % __name__
    )


from traitlets import Unicode, Dict, Set, Integer, Instance, Any
from traitlets.config import SingletonConfigurable

from .base import ClusterManager
from ..compat import get_running_loop
from ..utils import cancel_task


# A cache of clients by (principal, keytab). In most cases this will only
# be a single client.
_skein_client_cache = {}


async def skein_client(principal=None, keytab=None):
    """Return a shared skein client object.

    Calls with the same principal & keytab will return the same client object
    (if one exists).
    """
    key = (principal, keytab)
    client = _skein_client_cache.get(key)
    if client is None:
        kwargs = dict(
            principal=principal,
            keytab=keytab,
            security=skein.Security.new_credentials(),
        )
        fut = get_running_loop().run_in_executor(None, lambda: skein.Client(**kwargs))
        # Save the future first so any concurrent calls will wait on the same
        # future for generating the client
        _skein_client_cache[key] = fut
        client = await fut
        # Replace the future now that the operation is done
        _skein_client_cache[key] = client
    elif asyncio.isfuture(client):
        client = await client
    return client


class YarnAppClientCache(SingletonConfigurable):
    """A LRU cache of YARN application clients."""

    max_size = Integer(
        10,
        help="""
        The max size of the cache for application clients.

        A larger cache will result in improved performance, but will also use
        more resources.
        """,
        config=True,
    )

    cache = Instance(OrderedDict, args=())

    def get(self, key):
        """Get an item from the cache. Returns None if not present"""
        try:
            self.cache.move_to_end(key)
            return self.cache[key]
        except KeyError:
            return None

    def put(self, key, value):
        """Add an item to the cache"""
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            self.cache.popitem(False)

    def discard(self, key):
        """Remove an item from the cache. No-op if not present."""
        try:
            del self.cache[key]
        except KeyError:
            pass


class YarnClusterManager(ClusterManager):
    """A cluster manager for deploying Dask on a YARN cluster."""

    principal = Unicode(
        None,
        help="Kerberos principal for Dask Gateway user",
        allow_none=True,
        config=True,
    )

    keytab = Unicode(
        None,
        help="Path to kerberos keytab for Dask Gateway user",
        allow_none=True,
        config=True,
    )

    queue = Unicode(
        "default", help="The YARN queue to submit applications under", config=True
    )

    localize_files = Dict(
        help="""
        Extra files to distribute to both the worker and scheduler containers.

        This is a mapping from ``local-name`` to ``resource``. Resource paths
        can be local, or in HDFS (prefix with ``hdfs://...`` if so). If an
        archive (``.tar.gz`` or ``.zip``), the resource will be unarchived as
        directory ``local-name``. For finer control, resources can also be
        specified as ``skein.File`` objects, or their ``dict`` equivalents.

        This can be used to distribute conda/virtual environments by
        configuring the following:

        .. code::

            c.YarnClusterManager.localize_files = {
                'environment': {
                    'source': 'hdfs:///path/to/archived/environment.tar.gz',
                    'visibility': 'public'
                }
            }
            c.YarnClusterManager.scheduler_setup = 'source environment/bin/activate'
            c.YarnClusterManager.worker_setup = 'source environment/bin/activate'

        These archives are usually created using either ``conda-pack`` or
        ``venv-pack``. For more information on distributing files, see
        https://jcrist.github.io/skein/distributing-files.html.
        """,
        config=True,
    )

    worker_setup = Unicode(
        "", help="Script to run before dask worker starts.", config=True
    )

    scheduler_setup = Unicode(
        "", help="Script to run before dask scheduler starts.", config=True
    )

    supports_bulk_shutdown = True

    # Data stored per instance
    app_address = Unicode(help="The application address")
    pending_workers = Set(
        help="""
        A set of pending worker container ids.
        - Pending workers are added to this set in `start_worker`.
        - Upon connection, they are removed from this set.
        - Every `worker_status_period`, any pending workers have their
          status requested from their application, and any stopped
          workers are removed from this set.
        """
    )
    status_monitor = Any(help="The status monitor, or None")

    def _get_security(self):
        return skein.Security(cert_bytes=self.tls_cert, key_bytes=self.tls_key)

    async def _get_skein_client(self):
        return await skein_client(self.principal, self.keytab)

    @property
    def _app_client_cache(self):
        return YarnAppClientCache.instance(parent=self.parent or self)

    async def _get_app_client(self, cluster_state):
        out = self._app_client_cache.get(self.cluster_name)
        if out is None:
            app_id = cluster_state["app_id"]
            security = self._get_security()
            if not self.app_address:
                # Lookup and cache the application address
                skein_client = await self._get_skein_client()
                report = skein_client.application_report(app_id)
                if report.state != "RUNNING":  # pragma: nocover
                    raise ValueError("Application %s is not running" % app_id)
                self.app_address = "%s:%d" % (report.host, report.port)
            out = skein.ApplicationClient(self.app_address, app_id, security=security)
            self._app_client_cache.put(self.cluster_name, out)
        return out

    @contextmanager
    def temp_write_credentials(self):
        """Write credentials to disk in secure temporary files.

        The files will be cleaned up upon exiting this context.

        Returns
        -------
        cert_path, key_path
        """
        prefix = os.path.join(self.temp_dir, self.cluster_name)
        cert_path = prefix + ".crt"
        key_path = prefix + ".pem"

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
                with os.fdopen(os.open(path, flags, 0o600), "wb") as fil:
                    fil.write(data)

            yield cert_path, key_path
        finally:
            for path in [cert_path, key_path]:
                if os.path.exists(path):
                    os.unlink(path)

    def get_worker_args(self):
        return [
            "--nthreads",
            "$SKEIN_RESOURCE_VCORES",
            "--memory-limit",
            "${SKEIN_RESOURCE_MEMORY}MiB",
        ]

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return " ".join([self.worker_cmd] + self.get_worker_args())

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return self.scheduler_cmd

    def _build_specification(self, cert_path, key_path):
        files = {
            k: skein.File.from_dict(v) if isinstance(v, dict) else v
            for k, v in self.localize_files.items()
        }

        files["dask.crt"] = cert_path
        files["dask.pem"] = key_path

        env = self.get_env()

        scheduler_script = "\n".join([self.scheduler_setup, self.scheduler_command])
        worker_script = "\n".join([self.worker_setup, self.worker_command])

        master = skein.Master(
            security=self._get_security(),
            resources=skein.Resources(
                memory="%d b" % self.scheduler_memory, vcores=self.scheduler_cores
            ),
            files=files,
            env=env,
            script=scheduler_script,
        )

        services = {
            "dask.worker": skein.Service(
                resources=skein.Resources(
                    memory="%d b" % self.worker_memory, vcores=self.worker_cores
                ),
                instances=0,
                max_restarts=0,
                allow_failures=True,
                files=files,
                env=env,
                script=worker_script,
            )
        }

        return skein.ApplicationSpec(
            name="dask-gateway",
            queue=self.queue,
            user=self.username,
            master=master,
            services=services,
        )

    async def start_cluster(self):
        loop = get_running_loop()

        with self.temp_write_credentials() as (cert_path, key_path):
            spec = self._build_specification(cert_path, key_path)
            skein_client = await self._get_skein_client()
            app_id = await loop.run_in_executor(None, skein_client.submit, spec)

        yield {"app_id": app_id}

    async def cluster_status(self, cluster_state):
        app_id = cluster_state.get("app_id")
        if app_id is None:
            return False, None

        skein_client = await self._get_skein_client()

        report = await get_running_loop().run_in_executor(
            None, skein_client.application_report, app_id
        )
        state = str(report.state)
        if state in {"FAILED", "KILLED", "FINISHED"}:
            msg = "%s ended with exit state %s" % (app_id, state)
            return False, msg
        return True, None

    async def stop_cluster(self, cluster_state):
        app_id = cluster_state.get("app_id")
        if app_id is None:
            return

        skein_client = await self._get_skein_client()

        await get_running_loop().run_in_executor(
            None, skein_client.kill_application, app_id
        )
        # Stop the status monitor
        if self.status_monitor is not None:
            await cancel_task(self.status_monitor)
        # Remove cluster from caches
        self._app_client_cache.discard(self.cluster_name)

    def _start_worker(self, app, worker_name):
        return app.add_container(
            "dask.worker", env={"DASK_GATEWAY_WORKER_NAME": worker_name}
        )

    async def start_worker(self, worker_name, cluster_state):
        app = await self._get_app_client(cluster_state)
        container = await get_running_loop().run_in_executor(
            None, self._start_worker, app, worker_name
        )
        self._track_container(container.id, cluster_state)
        yield {"container_id": container.id}

    def _stop_worker(self, app, container_id):
        self._untrack_container(container_id)
        try:
            app.kill_container(container_id)
        except ValueError:
            pass

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        container_id = worker_state.get("container_id")
        if container_id is None:
            return

        app = await self._get_app_client(cluster_state)

        return await get_running_loop().run_in_executor(
            None, self._stop_worker, app, container_id
        )

    def _get_done_workers(self, app):
        try:
            containers = app.get_containers(
                services=("dask.worker",), states=("SUCCEEDED", "FAILED", "KILLED")
            )
        except Exception as exc:
            self.log.warning(
                "Error getting worker statuses for cluster %s",
                self.cluster_name,
                exc_info=exc,
            )
            return set()
        return {c.id for c in containers}

    async def _status_monitor(self, cluster_state):
        loop = get_running_loop()
        while True:
            if self.pending_workers:
                app = await self._get_app_client(cluster_state)
                done = await loop.run_in_executor(None, self._get_done_workers, app)
                self.pending_workers.difference_update(done)
            await asyncio.sleep(self.worker_status_period)

    def _track_container(self, container_id, cluster_state):
        self.pending_workers.add(container_id)
        # Ensure a status monitor is running
        if self.status_monitor is None:
            self.status_monitor = self.task_pool.create_background_task(
                self._status_monitor(cluster_state)
            )

    def _untrack_container(self, container_id):
        self.pending_workers.discard(container_id)

    def on_worker_running(self, worker_name, worker_state, cluster_state):
        self._untrack_container(worker_state.get("container_id"))

    async def worker_status(self, worker_name, worker_state, cluster_state):
        c_id = worker_state.get("container_id")
        return c_id and c_id in self.pending_workers
