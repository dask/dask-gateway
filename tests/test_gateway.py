import asyncio

import pytest

from dask_gateway import Gateway
from dask_gateway_server.app import DaskGateway
from dask_gateway_server.cluster import ClusterManager
from dask_gateway_server.utils import random_port

from .utils import InProcessClusterManager


class SlowStartClusterManager(ClusterManager):
    state_1 = {"state_1": 1}
    state_2 = {"state_2": 2}
    state_3 = {"state_3": 3}
    pause_time = 0.2
    stop_cluster_state = None

    async def start_cluster(self, cluster_info):
        yield self.state_1
        await asyncio.sleep(self.pause_time)
        yield self.state_2
        await asyncio.sleep(self.pause_time)
        yield self.state_3

    async def stop_cluster(self, cluster_info, cluster_state):
        self.stop_cluster_state = cluster_state


class FailStartClusterManager(ClusterManager):
    fail_stage = 1
    stop_cluster_state = None

    async def start_cluster(self, cluster_info):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_cluster(self, cluster_info, cluster_state):
        self.stop_cluster_state = cluster_state


class SlowWorkerStartClusterManager(InProcessClusterManager):
    pause_time = 0.2
    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        for i in range(3):
            yield {"i": i}
            await asyncio.sleep(self.pause_time)

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        self.stop_worker_state = worker_state


class FailWorkerStartClusterManager(InProcessClusterManager):
    fail_stage = 1
    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        self.stop_worker_state = worker_state


class temp_gateway(object):
    def __init__(self, **kwargs):
        self.options = {
            "gateway_url": "tls://127.0.0.1:%d" % random_port(),
            "private_url": "http://127.0.0.1:%d" % random_port(),
            "public_url": "http://127.0.0.1:%d" % random_port(),
            "db_url": "sqlite:///:memory:",
            "authenticator_class": "dask_gateway_server.auth.DummyAuthenticator",
        }
        self.options.update(**kwargs)

    async def __aenter__(self):
        self.gateway = DaskGateway.instance(**self.options)
        self.gateway.initialize([])
        await self.gateway.start_async()
        return self.gateway

    async def __aexit__(self, *args):
        await self.gateway.stop_async()
        DaskGateway.clear_instance()


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, "state_1"), (0.25, "state_2")])
async def test_slow_cluster_start(tmpdir, start_timeout, state):

    async with temp_gateway(
        cluster_manager_class=SlowStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.cluster_start_timeout = start_timeout

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            # Submission fails due to start timeout
            cluster_id = await gateway.submit()
            with pytest.raises(Exception) as exc:
                await gateway.connect(cluster_id)
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            res = getattr(gateway_proc.cluster_manager, state)
            assert gateway_proc.cluster_manager.stop_cluster_state == res


@pytest.mark.asyncio
async def test_slow_cluster_connect(tmpdir):

    async with temp_gateway(
        cluster_manager_class=SlowStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.cluster_connect_timeout = 0.1
        gateway_proc.cluster_manager.pause_time = 0

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            # Submission fails due to connect timeout
            cluster_id = await gateway.submit()
            with pytest.raises(Exception) as exc:
                await gateway.connect(cluster_id)
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            res = gateway_proc.cluster_manager.state_3
            assert gateway_proc.cluster_manager.stop_cluster_state == res


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_failing_cluster_start(tmpdir, fail_stage):

    async with temp_gateway(
        cluster_manager_class=FailStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.fail_stage = fail_stage

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            # Submission fails due to error during start
            cluster_id = await gateway.submit()
            with pytest.raises(Exception) as exc:
                await gateway.connect(cluster_id)
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            res = {} if fail_stage == 0 else {"i": fail_stage - 1}
            assert gateway_proc.cluster_manager.stop_cluster_state == res


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, 0), (0.25, 1)])
async def test_slow_worker_start(tmpdir, start_timeout, state):

    async with temp_gateway(
        cluster_manager_class=SlowWorkerStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.worker_start_timeout = start_timeout

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if gateway_proc.cluster_manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            assert gateway_proc.cluster_manager.stop_worker_state == {"i": state}

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
async def test_slow_worker_connect(tmpdir):

    async with temp_gateway(
        cluster_manager_class=SlowWorkerStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.worker_connect_timeout = 0.1
        gateway_proc.cluster_manager.pause_time = 0

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if gateway_proc.cluster_manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            assert gateway_proc.cluster_manager.stop_worker_state == {"i": 2}

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_failing_worker_start(tmpdir, fail_stage):

    async with temp_gateway(
        cluster_manager_class=FailWorkerStartClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        gateway_proc.cluster_manager.fail_stage = fail_stage

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if gateway_proc.cluster_manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            res = {} if fail_stage == 0 else {"i": fail_stage - 1}
            assert gateway_proc.cluster_manager.stop_worker_state == res

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
async def test_successful_cluster(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            cluster = await gateway.new_cluster()
            await cluster.scale(2)

            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            await cluster.scale(1)

            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            await cluster.shutdown()


@pytest.mark.asyncio
async def test_gateway_stop_clusters_on_shutdown(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        manager = gateway_proc.cluster_manager

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            await gateway.new_cluster()
            cluster2 = await gateway.new_cluster()
            await cluster2.shutdown()

            # There are active clusters
            assert manager.active_schedulers

    # Active clusters are stopped on shutdown
    assert not manager.active_schedulers
