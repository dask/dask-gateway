from dask_gateway.scheduler_preload import GatewaySchedulerService, make_gateway_client
from distributed import Scheduler, Security, Worker
from distributed.core import Status
from distributed.utils import TimeoutError

from .local import UnsafeLocalBackend

__all__ = ("InProcessBackend",)


class InProcessBackend(UnsafeLocalBackend):
    """A backend that runs everything in the same process"""

    def get_security(self, cluster):
        cert_path, key_path = self.get_tls_paths(cluster)
        return Security(
            tls_ca_file=cert_path,
            tls_scheduler_cert=cert_path,
            tls_scheduler_key=key_path,
            tls_worker_cert=cert_path,
            tls_worker_key=key_path,
        )

    def get_gateway_client(self, cluster):
        return make_gateway_client(
            cluster_name=cluster.name, api_token=cluster.token, api_url=self.api_url
        )

    def _check_status(self, objs, mapping):
        out = []
        for x in objs:
            x = mapping.get(x.name)
            ok = x is not None and not x.status != Status.closed
            out.append(ok)
        return out

    async def do_setup(self):
        self.schedulers = {}
        self.workers = {}

    async def do_start_cluster(self, cluster):
        workdir = self.setup_working_directory(cluster)
        yield {"workdir": workdir}

        security = self.get_security(cluster)
        gateway_client = self.get_gateway_client(cluster)

        self.schedulers[cluster.name] = scheduler = Scheduler(
            protocol="tls",
            host="127.0.0.1",
            port=0,
            dashboard_address="127.0.0.1:0",
            security=security,
            services={
                ("gateway", ":0"): (
                    GatewaySchedulerService,
                    {
                        "gateway": gateway_client,
                        "heartbeat_period": self.cluster_heartbeat_period,
                        "adaptive_period": cluster.config.adaptive_period,
                        "idle_timeout": cluster.config.idle_timeout,
                    },
                )
            },
        )
        await scheduler
        yield {"workdir": workdir, "started": True}

    async def do_stop_cluster(self, cluster):
        scheduler = self.schedulers.pop(cluster.name)

        await scheduler.close(fast=True)
        scheduler.stop()

        workdir = cluster.state.get("workdir")
        if workdir is not None:
            self.cleanup_working_directory(workdir)

    async def do_check_clusters(self, clusters):
        return self._check_status(clusters, self.schedulers)

    async def do_start_worker(self, worker):
        security = self.get_security(worker.cluster)
        workdir = worker.cluster.state["workdir"]
        self.workers[worker.name] = worker = Worker(
            worker.cluster.scheduler_address,
            nthreads=worker.cluster.config.worker_threads,
            memory_limit=0,
            security=security,
            name=worker.name,
            local_directory=workdir,
        )
        await worker
        yield {"started": True}

    async def do_stop_worker(self, worker):
        worker = self.workers.pop(worker.name, None)
        if worker is None:
            return
        try:
            await worker.close(timeout=1)
        except TimeoutError:
            pass

    async def do_check_workers(self, workers):
        return self._check_status(workers, self.workers)

    async def worker_status(self, worker_name, worker_state, cluster_state):
        worker = self.workers.get(worker_name)
        if worker is None:
            return False
        return not worker.status != Status.closed
