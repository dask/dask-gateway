import asyncio
import functools
import inspect

import pytest

from dask_gateway import Gateway, BasicAuth
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


def gateway_test(**options):
    def wrapper(func):
        @functools.wraps(func)
        async def test_func(*args, **kwargs):
            gateway = DaskGateway(
                gateway_url="tls://127.0.0.1:%d" % random_port(),
                private_url="http://127.0.0.1:%d" % random_port(),
                public_url="http://127.0.0.1:%d" % random_port(),
                db_url="sqlite:///:memory:",
                authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
                **options,
            )
            gateway.initialize([])
            await gateway.start_async()
            try:
                await func(gateway, *args, **kwargs)
            finally:
                await gateway.stop_async()

        # Drop the first argument to allow using with other fixtures
        sig = inspect.signature(test_func)
        params = list(sig.parameters.values())[1:]
        test_func.__signature__ = sig.replace(parameters=params)
        return test_func

    return wrapper


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, "state_1"), (0.25, "state_2")])
@gateway_test(cluster_manager_class=SlowStartClusterManager)
async def test_slow_cluster_start(gateway_proc, start_timeout, state):
    gateway_proc.cluster_manager.cluster_start_timeout = start_timeout
    async with Gateway(
        address=gateway_proc.public_url, auth=BasicAuth("testuser"), asynchronous=True
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
@gateway_test(cluster_manager_class=SlowStartClusterManager)
async def test_slow_cluster_connect(gateway_proc):
    gateway_proc.cluster_manager.cluster_connect_timeout = 0.1
    gateway_proc.cluster_manager.pause_time = 0

    async with Gateway(
        address=gateway_proc.public_url, auth=BasicAuth("testuser"), asynchronous=True
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
@gateway_test(cluster_manager_class=FailStartClusterManager)
async def test_failing_cluster_start(gateway_proc, fail_stage):
    gateway_proc.cluster_manager.fail_stage = fail_stage
    async with Gateway(
        address=gateway_proc.public_url, auth=BasicAuth("testuser"), asynchronous=True
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
@gateway_test(cluster_manager_class=InProcessClusterManager)
async def test_successful_cluster(gateway_proc):
    async with Gateway(
        address=gateway_proc.public_url, auth=BasicAuth(), asynchronous=True
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
