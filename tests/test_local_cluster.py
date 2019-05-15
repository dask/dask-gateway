import atexit
import os
import signal

from dask_gateway_server.local_cluster import UnsafeLocalClusterManager, is_running

from .utils import ClusterManagerTests


_PIDS = set()


def cleanup_lost_processes():
    if not _PIDS:
        return
    for pid in _PIDS:
        os.kill(pid, signal.SIGTERM)
    print("-- Stopped %d lost processes --" % len(_PIDS))


atexit.register(cleanup_lost_processes)


class LocalTestingClusterManager(UnsafeLocalClusterManager):
    async def start_process(self, *args, **kwargs):
        pid = await super().start_process(*args, **kwargs)
        _PIDS.add(pid)
        return pid

    async def stop_process(self, pid):
        await super().stop_process(pid)
        _PIDS.discard(pid)


class TestLocalClusterManager(ClusterManagerTests):
    async def cleanup_cluster(self, manager, cluster_state, worker_states):
        for state in worker_states + [cluster_state]:
            pid = state.get("pid")
            if pid is not None:
                await manager.stop_process(pid)

    def new_manager(self, **kwargs):
        return LocalTestingClusterManager(**kwargs)

    def is_cluster_running(self, cluster_state):
        pid = cluster_state.get("pid")
        return is_running(pid) if pid is not None else False

    def is_worker_running(self, cluster_state, worker_state):
        pid = worker_state.get("pid")
        return is_running(pid) if pid is not None else False

    def num_start_cluster_stages(self):
        return 1

    def num_start_worker_stages(self):
        return 1
