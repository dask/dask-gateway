import os
import pytest

skein = pytest.importorskip("skein")

if not os.environ.get("TEST_DASK_GATEWAY_YARN"):
    pytest.skip("Not running YARN tests", allow_module_level=True)

from dask_gateway_server.yarn_cluster import YarnClusterManager

from .utils import ClusterManagerTests


pytestmark = pytest.mark.usefixtures("cleanup_applications")


_APPIDS = set()


@pytest.fixture(scope="module")
def cleanup_applications():
    yield

    if not _APPIDS:
        return

    with skein.Client(principal="dask", keytab="/home/dask/dask.keytab") as client:
        for appid in _APPIDS:
            try:
                client.kill_application(appid)
            except OSError:
                pass
    print("-- Stopped %d lost clusters --" % len(_APPIDS))


class YarnTestingClusterManager(YarnClusterManager):
    async def start_cluster(self, *args, **kwargs):
        async for state in super().start_cluster(*args, **kwargs):
            _APPIDS.add(state["app_id"])
            yield state

    async def stop_cluster(self, cluster_info, cluster_state):
        appid = cluster_state.get("app_id")
        await super().stop_cluster(cluster_info, cluster_state)
        _APPIDS.discard(appid)


class TestYarnClusterManager(ClusterManagerTests):
    async def cleanup_cluster(
        self, manager, cluster_info, cluster_state, worker_states
    ):
        app_id = cluster_state.get("app_id")
        if app_id is not None:
            manager.skein_client.kill_application(app_id)

    def new_manager(self, **kwargs):
        return YarnTestingClusterManager(
            scheduler_cmd="/opt/miniconda/bin/dask-gateway-scheduler",
            worker_cmd="/opt/miniconda/bin/dask-gateway-worker",
            keytab="/home/dask/dask.keytab",
            principal="dask",
            scheduler_memory="512M",
            worker_memory="512M",
            scheduler_cores=1,
            worker_cores=1,
            cluster_start_timeout=30,
            **kwargs,
        )

    def is_cluster_running(self, manager, cluster_info, cluster_state):
        app_id = cluster_state.get("app_id")
        if not app_id:
            return False
        report = manager.skein_client.application_report(app_id)
        return report.state not in ("FINISHED", "FAILED", "KILLED")

    def is_worker_running(self, manager, cluster_info, cluster_state, worker_state):
        app_id = cluster_state.get("app_id")
        container_id = worker_state.get("container_id")
        if not app_id or not container_id:
            return False

        app = manager._get_app_client(cluster_info, cluster_state)

        active = app.get_containers(
            services=["dask.worker"], states=["WAITING", "REQUESTED", "RUNNING"]
        )
        return container_id in [c.id for c in active]

    def num_start_cluster_stages(self):
        return 2

    def num_start_worker_stages(self):
        return 1
