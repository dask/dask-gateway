from dask_gateway_server.managers.local import is_running

from .utils import ClusterManagerTests, LocalTestingClusterManager


class TestLocalClusterManager(ClusterManagerTests):
    async def cleanup_cluster(
        self, manager, cluster_info, cluster_state, worker_states
    ):
        for state in worker_states + [cluster_state]:
            pid = state.get("pid")
            if pid is not None:
                await manager.stop_process(pid)

    def new_manager(self, **kwargs):
        return LocalTestingClusterManager(**kwargs)

    def cluster_is_running(self, manager, cluster_info, cluster_state):
        pid = cluster_state.get("pid")
        return is_running(pid) if pid is not None else False

    def worker_is_running(self, manager, cluster_info, cluster_state, worker_state):
        pid = worker_state.get("pid")
        return is_running(pid) if pid is not None else False

    def num_start_cluster_stages(self):
        return 1

    def num_start_worker_stages(self):
        return 1
