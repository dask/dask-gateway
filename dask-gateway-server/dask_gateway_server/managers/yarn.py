import asyncio
import os
from collections import OrderedDict, defaultdict
from contextlib import contextmanager

import skein
from traitlets import Unicode, Dict, Integer, Float, Instance, default

from .base import ClusterManager
from ..utils import cancel_task


class LRUCache(object):
    def __init__(self, max_size):
        self.max_size = max_size
        self.cache = OrderedDict()

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

    application_client_cache_size = Integer(
        10,
        help="""
        The size of the cache for application clients.

        A larger cache will result in improved performance, but will also use
        more resources.
        """,
        config=True,
    )

    container_status_period = Float(
        help="""
        Time (in seconds) between application container status checks.

        This should be <= ``worker_status_period``. The default is
        ``worker_status_period``.
        """,
        config=True,
    )

    @default("container_status_period")
    def _default_container_status_period(self):
        return self.worker_status_period

    supports_bulk_shutdown = True

    skein_client = Instance(klass="skein.Client", help="The skein client to use")

    @default("skein_client")
    def _default_skein_client(self):
        return skein.Client(
            principal=self.principal,
            keytab=self.keytab,
            security=skein.Security.new_credentials(),
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_cache = LRUCache(self.application_client_cache_size)
        self.app_address_cache = {}
        # A map of container_name => set of pending worker container ids
        # - Pending workers are added to this set in `start_worker`.
        # - Upon connection, they are removed from this set.
        # - Every `container_status_period`, any pending workers have their
        #   status requested from their application, and the any stopped
        #   workers are removed from this set.
        self._pending_workers = defaultdict(set)
        self._status_monitors = {}

    def _get_security(self, cluster_info):
        return skein.Security(
            cert_bytes=cluster_info.tls_cert, key_bytes=cluster_info.tls_key
        )

    def _get_app_client(self, cluster_info, cluster_state):
        out = self.client_cache.get(cluster_info.cluster_name)
        if out is None:
            app_id = cluster_state["app_id"]
            security = self._get_security(cluster_info)
            address = self.app_address_cache.get(cluster_info.cluster_name)
            if address is None:
                # Lookup and cache the application address
                report = self.skein_client.application_report(app_id)
                if report.state != "RUNNING":  # pragma: nocover
                    raise ValueError("Application %s is not running" % app_id)
                address = "%s:%d" % (report.host, report.port)
                self.app_address_cache[cluster_info.cluster_name] = address
            out = skein.ApplicationClient(address, app_id, security=security)
            self.client_cache.put(cluster_info.cluster_name, out)
        return out

    @contextmanager
    def temp_write_credentials(self, cluster_info):
        """Write credentials to disk in secure temporary files.

        The files will be cleaned up upon exiting this context.

        Returns
        -------
        cert_path, key_path
        """
        prefix = os.path.join(self.temp_dir, cluster_info.cluster_name)
        cert_path = prefix + ".crt"
        key_path = prefix + ".pem"

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            for path, data in [
                (cert_path, cluster_info.tls_cert),
                (key_path, cluster_info.tls_key),
            ]:
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

    def _build_specification(self, cluster_info, cert_path, key_path):
        files = {
            k: skein.File.from_dict(v) if isinstance(v, dict) else v
            for k, v in self.localize_files.items()
        }

        files["dask.crt"] = cert_path
        files["dask.pem"] = key_path

        env = self.get_env(cluster_info)

        scheduler_script = "\n".join([self.scheduler_setup, self.scheduler_command])
        worker_script = "\n".join([self.worker_setup, self.worker_command])

        master = skein.Master(
            security=self._get_security(cluster_info),
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
            user=cluster_info.username,
            master=master,
            services=services,
        )

    async def start_cluster(self, cluster_info):
        loop = asyncio.get_running_loop()

        with self.temp_write_credentials(cluster_info) as (cert_path, key_path):
            spec = self._build_specification(cluster_info, cert_path, key_path)
            app_id = await loop.run_in_executor(None, self.skein_client.submit, spec)

        yield {"app_id": app_id}

    async def cluster_status(self, cluster_info, cluster_state):
        app_id = cluster_state.get("app_id")
        if app_id is None:
            return False, None

        report = await asyncio.get_running_loop().run_in_executor(
            None, self.skein_client.application_report, app_id
        )
        state = str(report.state)
        if state in {"FAILED", "KILLED", "FINISHED"}:
            msg = "%s ended with exit state %s" % (app_id, state)
            return False, msg
        return True, None

    async def stop_cluster(self, cluster_info, cluster_state):
        app_id = cluster_state.get("app_id")
        if app_id is None:
            return
        await asyncio.get_running_loop().run_in_executor(
            None, self.skein_client.kill_application, app_id
        )
        # Stop the status monitor
        monitor = self._status_monitors.pop(cluster_info.cluster_name, None)
        if monitor:
            await cancel_task(monitor)
        # Remove internal worker statuses
        self._pending_workers.pop(cluster_info.cluster_name, None)
        # Remove cluster from caches
        self.app_address_cache.pop(cluster_info.cluster_name, None)
        self.client_cache.discard(cluster_info.cluster_name)

    def _start_worker(self, worker_name, cluster_info, cluster_state):
        app = self._get_app_client(cluster_info, cluster_state)
        return app.add_container(
            "dask.worker", env={"DASK_GATEWAY_WORKER_NAME": worker_name}
        )

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        container = await asyncio.get_running_loop().run_in_executor(
            None, self._start_worker, worker_name, cluster_info, cluster_state
        )
        self._track_container(container.id, cluster_info, cluster_state)
        yield {"container_id": container.id}

    def _stop_worker(self, container_id, cluster_info, cluster_state):
        self._untrack_container(container_id, cluster_info)
        app = self._get_app_client(cluster_info, cluster_state)
        try:
            app.kill_container(container_id)
        except ValueError:
            pass

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        container_id = worker_state.get("container_id")
        if container_id is None:
            return
        return await asyncio.get_running_loop().run_in_executor(
            None, self._stop_worker, container_id, cluster_info, cluster_state
        )

    def _get_done_workers(self, cluster_info, cluster_state):
        app = self._get_app_client(cluster_info, cluster_state)
        try:
            containers = app.get_containers(
                services=("dask.worker",), states=("SUCCEEDED", "FAILED", "KILLED")
            )
        except Exception as exc:
            self.log.warning(
                "Error getting worker statuses for cluster %s",
                cluster_info.cluster_name,
                exc_info=exc,
            )
            return set()
        return {c.id for c in containers}

    async def _status_monitor(self, cluster_info, cluster_state):
        loop = asyncio.get_running_loop()
        active = self._pending_workers[cluster_info.cluster_name]
        while True:
            if active:
                done = await loop.run_in_executor(
                    None, self._get_done_workers, cluster_info, cluster_state
                )
                active.difference_update(done)
            await asyncio.sleep(self.container_status_period)

    def _track_container(self, container_id, cluster_info, cluster_state):
        self._pending_workers[cluster_info.cluster_name].add(container_id)
        # Ensure a status monitor is running
        if cluster_info.cluster_name not in self._status_monitors:
            monitor = self.task_pool.create_background_task(
                self._status_monitor(cluster_info, cluster_state)
            )
            self._status_monitors[cluster_info.cluster_name] = monitor

    def _untrack_container(self, container_id, cluster_info):
        self._pending_workers[cluster_info.cluster_name].discard(container_id)

    def on_worker_running(self, worker_name, worker_state, cluster_info, cluster_state):
        self._untrack_container(worker_state.get("container_id"), cluster_info)

    async def worker_status(
        self, worker_name, worker_state, cluster_info, cluster_state
    ):
        c_id = worker_state.get("container_id")
        return c_id and c_id in self._pending_workers[cluster_info.cluster_name]
