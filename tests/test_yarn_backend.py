import os
import pytest

from traitlets.config import Config

skein = pytest.importorskip("skein")

if not os.environ.get("TEST_DASK_GATEWAY_YARN"):
    pytest.skip("Not running YARN tests", allow_module_level=True)

from dask_gateway.auth import BasicAuth
from dask_gateway_server.backends.yarn import YarnBackend

from .utils_test import temp_gateway, wait_for_workers, with_retries


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


class YarnTestingBackend(YarnBackend):
    async def do_start_cluster(self, cluster):
        async for state in super().do_start_cluster(cluster):
            _APPIDS.add(state["app_id"])
            yield state

    async def do_stop_cluster(self, cluster):
        appid = cluster.state.get("app_id")
        await super().do_stop_cluster(cluster)
        _APPIDS.discard(appid)


@pytest.mark.asyncio
async def test_yarn_backend():

    c = Config()
    c.YarnClusterConfig.scheduler_cmd = "/opt/miniconda/bin/dask-scheduler"
    c.YarnClusterConfig.worker_cmd = "/opt/miniconda/bin/dask-worker"
    c.YarnClusterConfig.scheduler_memory = "512M"
    c.YarnClusterConfig.worker_memory = "512M"
    c.YarnClusterConfig.scheduler_cores = 1
    c.YarnClusterConfig.worker_cores = 1

    c.YarnBackend.keytab = "/home/dask/dask.keytab"
    c.YarnBackend.principal = "dask"

    c.DaskGateway.backend_class = YarnTestingBackend

    async with temp_gateway(config=c) as g:
        auth = BasicAuth(username="alice")
        async with g.gateway_client(auth=auth) as gateway:
            async with gateway.new_cluster() as cluster:

                db_cluster = g.gateway.backend.db.get_cluster(cluster.name)

                res = await g.gateway.backend.do_check_clusters([db_cluster])
                assert res == [True]

                await cluster.scale(2)
                await wait_for_workers(cluster, exact=2)
                await cluster.scale(1)
                await wait_for_workers(cluster, exact=1)

                db_workers = list(db_cluster.workers.values())

                async def test():
                    res = await g.gateway.backend.do_check_workers(db_workers)
                    assert sum(res) == 1

                await with_retries(test, 30, 0.25)

                async with cluster.get_client(set_as_default=False) as client:
                    res = await client.submit(lambda x: x + 1, 1)
                    assert res == 2

                await cluster.scale(0)
                await wait_for_workers(cluster, exact=0)

                async def test():
                    res = await g.gateway.backend.do_check_workers(db_workers)
                    assert sum(res) == 0

                await with_retries(test, 30, 0.25)

            # No-op for shutdown of already shutdown worker
            async def test():
                res = await g.gateway.backend.do_check_clusters([db_cluster])
                assert res == [False]

            await with_retries(test, 30, 0.25)
