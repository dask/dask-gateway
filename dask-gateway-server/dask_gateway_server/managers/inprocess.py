from dask_gateway.dask_cli import (
    make_security,
    make_gateway_client,
    start_scheduler,
    start_worker,
)
from tornado import gen

from .local import UnsafeLocalClusterManager


class InProcessClusterManager(UnsafeLocalClusterManager):
    """A cluster manager that runs everything in the same process"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_schedulers = {}
        self.active_workers = {}

    def get_security(self, cluster_info):
        cert_path, key_path = self.get_tls_paths(cluster_info)
        return make_security(cert_path, key_path)

    def get_gateway_client(self, cluster_info):
        return make_gateway_client(
            cluster_name=cluster_info.cluster_name,
            api_token=cluster_info.api_token,
            api_url=self.api_url,
        )

    async def start_cluster(self, cluster_info):
        self.create_working_directory(cluster_info)
        security = self.get_security(cluster_info)
        gateway_client = self.get_gateway_client(cluster_info)
        scheduler = await start_scheduler(
            gateway_client, security, exit_on_failure=False
        )
        self.active_schedulers[cluster_info.cluster_name] = scheduler
        yield {}

    async def cluster_status(self, cluster_info, cluster_state):
        scheduler = self.active_schedulers.get(cluster_info.cluster_name)
        if scheduler is None:
            return False
        return not scheduler.status.startswith("clos")

    async def stop_cluster(self, cluster_info, cluster_state):
        scheduler = self.active_schedulers.pop(cluster_info.cluster_name, None)
        if scheduler is None:
            return
        await scheduler.close(fast=True)
        scheduler.stop()

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        security = self.get_security(cluster_info)
        gateway_client = self.get_gateway_client(cluster_info)
        workdir = self.get_working_directory(cluster_info)
        worker = await start_worker(
            gateway_client, security, worker_name, local_dir=workdir, nanny=False
        )
        self.active_workers[worker_name] = worker
        yield {}

    async def worker_status(
        self, worker_name, worker_state, cluster_info, cluster_state
    ):
        worker = self.active_workers.get(worker_name)
        if worker is None:
            return False
        return not worker.status.startswith("clos")

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        worker = self.active_workers.pop(worker_name, None)
        if worker is None:
            return
        try:
            await worker.close(timeout=1)
        except gen.TimeoutError:
            pass
