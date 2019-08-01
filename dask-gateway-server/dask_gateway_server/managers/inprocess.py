from dask_gateway.dask_cli import (
    make_security,
    make_gateway_client,
    start_scheduler,
    start_worker,
)
from tornado import gen
from traitlets import Any, Dict

from .local import UnsafeLocalClusterManager


class InProcessClusterManager(UnsafeLocalClusterManager):
    """A cluster manager that runs everything in the same process"""

    scheduler = Any()
    workers = Dict()

    def get_security(self):
        cert_path, key_path = self.get_tls_paths()
        return make_security(cert_path, key_path)

    def get_gateway_client(self):
        return make_gateway_client(
            cluster_name=self.cluster_name,
            api_token=self.api_token,
            api_url=self.api_url,
        )

    async def start_cluster(self):
        self.create_working_directory()
        security = self.get_security()
        gateway_client = self.get_gateway_client()
        self.scheduler = await start_scheduler(
            gateway_client, security, exit_on_failure=False
        )
        yield {}

    async def cluster_status(self, cluster_state):
        if self.scheduler is None:
            return False
        return not self.scheduler.status.startswith("clos")

    async def stop_cluster(self, cluster_state):
        if self.scheduler is None:
            return
        await self.scheduler.close(fast=True)
        self.scheduler.stop()
        self.scheduler = None

    async def start_worker(self, worker_name, cluster_state):
        security = self.get_security()
        gateway_client = self.get_gateway_client()
        workdir = self.get_working_directory()
        worker = await start_worker(
            gateway_client, security, worker_name, local_directory=workdir, nanny=False
        )
        self.workers[worker_name] = worker
        yield {}

    async def worker_status(self, worker_name, worker_state, cluster_state):
        worker = self.workers.get(worker_name)
        if worker is None:
            return False
        return not worker.status.startswith("clos")

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        worker = self.workers.pop(worker_name, None)
        if worker is None:
            return
        try:
            await worker.close(timeout=1)
        except gen.TimeoutError:
            pass
