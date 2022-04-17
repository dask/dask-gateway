from asyncio import get_running_loop
from collections import defaultdict
from tempfile import NamedTemporaryFile

try:
    import skein
except ImportError:
    raise ImportError(
        "'%s.YarnBackend' requires 'skein' as a dependency. "
        "To install required dependencies, use:\n"
        "  $ pip install dask-gateway-server[yarn]\n"
        "or\n"
        "  $ conda install dask-gateway-server-yarn -c conda-forge\n" % __name__
    )


from traitlets import Dict, Integer, Unicode

from ..traitlets import Type
from ..utils import LRUCache
from .base import ClusterConfig
from .db_base import DBBackendBase

__all__ = ("YarnClusterConfig", "YarnBackend")


class YarnClusterConfig(ClusterConfig):
    """Dask cluster configuration options when running on Hadoop/YARN"""

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

            c.YarnClusterConfig.localize_files = {
                'environment': {
                    'source': 'hdfs:///path/to/archived/environment.tar.gz',
                    'visibility': 'public'
                }
            }
            c.YarnClusterConfig.scheduler_setup = 'source environment/bin/activate'
            c.YarnClusterConfig.worker_setup = 'source environment/bin/activate'

        These archives are usually created using either ``conda-pack`` or
        ``venv-pack``. For more information on distributing files, see
        https://jcristharif.com/skein/distributing-files.html.
        """,
        config=True,
    )

    worker_setup = Unicode(
        "", help="Script to run before dask worker starts.", config=True
    )

    scheduler_setup = Unicode(
        "", help="Script to run before dask scheduler starts.", config=True
    )


class YarnBackend(DBBackendBase):
    """A cluster backend for managing dask clusters on Hadoop/YARN."""

    cluster_config_class = Type(
        "dask_gateway_server.backends.yarn.YarnClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

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

    app_client_cache_max_size = Integer(
        10,
        help="""
        The max size of the cache for application clients.

        A larger cache will result in improved performance, but will also use
        more resources.
        """,
        config=True,
    )

    def async_apply(self, f, *args, **kwargs):
        return get_running_loop().run_in_executor(None, lambda: f(*args, **kwargs))

    def _get_security(self, cluster):
        return skein.Security(cert_bytes=cluster.tls_cert, key_bytes=cluster.tls_key)

    async def _get_app_client(self, cluster):
        out = self.app_client_cache.get(cluster.name)
        if out is None:
            app_id = cluster.state["app_id"]
            security = self._get_security(cluster)
            if cluster.name not in self.app_address_cache:
                # Lookup and cache the application address
                report = self.skein_client.application_report(app_id)
                if report.state != "RUNNING":  # pragma: nocover
                    raise ValueError("Application %s is not running" % app_id)
                app_address = "%s:%d" % (report.host, report.port)
                self.app_address_cache[cluster.name] = app_address
            app_address = self.app_address_cache[cluster.name]
            out = skein.ApplicationClient(app_address, app_id, security=security)
            self.app_client_cache.put(cluster.name, out)
        return out

    def worker_nthreads_memory_limit_args(self, cluster):
        return "$SKEIN_RESOURCE_VCORES", "${SKEIN_RESOURCE_MEMORY}MiB"

    def _build_specification(self, cluster, cert_path, key_path):
        files = {
            k: skein.File.from_dict(v) if isinstance(v, dict) else v
            for k, v in cluster.config.localize_files.items()
        }

        files["dask.crt"] = cert_path
        files["dask.pem"] = key_path

        scheduler_cmd = " ".join(self.get_scheduler_command(cluster))
        worker_cmd = " ".join(
            self.get_worker_command(
                cluster,
                worker_name="$DASK_GATEWAY_WORKER_NAME",
                scheduler_address="$DASK_GATEWAY_SCHEDULER_ADDRESS",
            )
        )
        scheduler_script = f"{cluster.config.scheduler_setup}\n{scheduler_cmd}"
        worker_script = f"{cluster.config.worker_setup}\n{worker_cmd}"

        master = skein.Master(
            security=self._get_security(cluster),
            resources=skein.Resources(
                memory="%d b" % cluster.config.scheduler_memory,
                vcores=cluster.config.scheduler_cores,
            ),
            files=files,
            env=self.get_scheduler_env(cluster),
            script=scheduler_script,
        )

        services = {
            "dask.worker": skein.Service(
                resources=skein.Resources(
                    memory="%d b" % cluster.config.worker_memory,
                    vcores=cluster.config.worker_cores,
                ),
                instances=0,
                max_restarts=0,
                allow_failures=True,
                files=files,
                env=self.get_worker_env(cluster),
                script=worker_script,
            )
        }

        return skein.ApplicationSpec(
            name="dask-gateway",
            queue=cluster.config.queue,
            user=cluster.username,
            master=master,
            services=services,
        )

    supports_bulk_shutdown = True

    async def do_setup(self):
        self.skein_client = await self.async_apply(
            skein.Client,
            principal=self.principal,
            keytab=self.keytab,
            security=skein.Security.new_credentials(),
        )

        self.app_client_cache = LRUCache(self.app_client_cache_max_size)
        self.app_address_cache = {}

    async def do_cleanup(self):
        self.skein_client.close()

    async def do_start_cluster(self, cluster):
        with NamedTemporaryFile() as cert_fil, NamedTemporaryFile() as key_fil:
            cert_fil.write(cluster.tls_cert)
            cert_fil.file.flush()
            key_fil.write(cluster.tls_key)
            key_fil.file.flush()
            spec = self._build_specification(cluster, cert_fil.name, key_fil.name)
            app_id = await self.async_apply(self.skein_client.submit, spec)

        yield {"app_id": app_id}

    async def do_stop_cluster(self, cluster):
        app_id = cluster.state.get("app_id")
        if app_id is None:
            return

        await self.async_apply(self.skein_client.kill_application, app_id)
        # Remove cluster from caches
        self.app_client_cache.discard(cluster.name)
        self.app_address_cache.pop(cluster.name, None)

    async def do_check_clusters(self, clusters):
        results = []
        for cluster in clusters:
            app_id = cluster.state.get("app_id")
            if app_id is None:
                return False
            report = await self.async_apply(
                self.skein_client.application_report, app_id
            )
            ok = str(report.state) not in {"FAILED", "KILLED", "FINISHED"}
            results.append(ok)
        return results

    async def do_start_worker(self, worker):
        app = await self._get_app_client(worker.cluster)
        container = await self.async_apply(
            app.add_container,
            "dask.worker",
            env={
                "DASK_GATEWAY_WORKER_NAME": worker.name,
                "DASK_GATEWAY_SCHEDULER_ADDRESS": worker.cluster.scheduler_address,
            },
        )
        yield {"container_id": container.id}

    async def do_stop_worker(self, worker):
        container_id = worker.state.get("container_id")
        if container_id is None:
            return

        app = await self._get_app_client(worker.cluster)
        try:
            await self.async_apply(app.kill_container, container_id)
        except ValueError:
            pass

    async def do_check_workers(self, workers):
        grouped = defaultdict(list)
        for w in workers:
            grouped[w.cluster].append(w)

        results = {}
        for cluster, workers in grouped.items():
            app = await self._get_app_client(cluster)
            try:
                containers = await self.async_apply(
                    app.get_containers, services=("dask.worker",)
                )
                active = {c.id for c in containers}
                results.update(
                    {w.name: w.state.get("container_id") in active for w in workers}
                )
            except Exception as exc:
                self.log.debug(
                    "Error getting worker statuses for cluster %s",
                    cluster.name,
                    exc_info=exc,
                )
                results.update({w.name: False for w in workers})

        return [results[w.name] for w in workers]
