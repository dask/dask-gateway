import asyncio
import os
import signal

import pytest
from cryptography.fernet import Fernet
from traitlets.config import Config

from dask_gateway import Gateway
from dask_gateway_server.app import DaskGateway
from dask_gateway_server.managers import ClusterManager
from dask_gateway_server.managers.inprocess import InProcessClusterManager
from dask_gateway_server.utils import random_port

from .utils import LocalTestingClusterManager, temp_gateway


class SlowStartClusterManager(ClusterManager):
    state_1 = {"state_1": 1}
    state_2 = {"state_2": 2}
    state_3 = {"state_3": 3}
    pause_time = 0.2
    stop_cluster_state = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clusters = set()

    async def start_cluster(self, cluster_info):
        yield self.state_1
        await asyncio.sleep(self.pause_time)
        yield self.state_2
        await asyncio.sleep(self.pause_time)
        yield self.state_3
        self.clusters.add(cluster_info.cluster_name)

    async def cluster_status(self, cluster_info, cluster_state):
        return cluster_info.cluster_name in self.clusters

    async def stop_cluster(self, cluster_info, cluster_state):
        self.stop_cluster_state = cluster_state
        self.clusters.discard(cluster_info.cluster_name)


class ClusterFailsDuringStart(ClusterManager):
    fail_stage = 1
    stop_cluster_state = None

    async def start_cluster(self, cluster_info):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_cluster(self, cluster_info, cluster_state):
        self.stop_cluster_state = cluster_state


class ClusterFailsBetweenStartAndConnect(InProcessClusterManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_clusters = set()
        self.stopped_clusters = set()

    async def start_cluster(self, cluster_info):
        yield {"foo": "bar"}
        self.failed_clusters.add(cluster_info.cluster_name)

    async def cluster_status(self, cluster_info, cluster_state):
        return cluster_info.cluster_name not in self.failed_clusters

    async def stop_cluster(self, cluster_info, cluster_state):
        self.stopped_clusters.add(cluster_info.cluster_name)


class ClusterFailsAfterConnect(InProcessClusterManager):
    fail_after = 0.5

    async def start_cluster(self, cluster_info):
        # Initiate state
        loop = asyncio.get_running_loop()
        self.failed = loop.create_future()
        self.stop_cluster_called = loop.create_future()
        # Start the cluster
        async for state in super().start_cluster(cluster_info):
            yield state
        # Launch a task to kill the cluster soon
        self.task_pool.create_task(self.delay_fail_cluster())

    async def delay_fail_cluster(self):
        await asyncio.sleep(self.fail_after)
        self.failed.set_result(True)

    async def cluster_status(self, cluster_info, cluster_state):
        if self.failed.done():
            return False
        return await super().cluster_status(cluster_info, cluster_state)

    async def stop_cluster(self, cluster_info, cluster_state):
        await super().stop_cluster(cluster_info, cluster_state)
        self.stop_cluster_called.set_result(True)


class SlowWorkerStartClusterManager(InProcessClusterManager):
    pause_time = 0.2
    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        for i in range(3):
            yield {"i": i}
            await asyncio.sleep(self.pause_time)
        self.active_workers[worker_name] = None

    async def worker_status(
        self, worker_name, worker_state, cluster_info, cluster_state
    ):
        return worker_name in self.active_workers

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        self.stop_worker_state = worker_state
        self.active_workers.pop(worker_name, None)


class WorkerFailsDuringStart(InProcessClusterManager):
    fail_stage = 1
    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        self.stop_worker_state = worker_state


class WorkerFailsBetweenStartAndConnect(InProcessClusterManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_workers = set()
        self.stopped_workers = set()

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        yield {"foo": "bar"}
        self.failed_workers.add(worker_name)

    async def worker_status(
        self, worker_name, worker_state, cluster_info, cluster_state
    ):
        return worker_name not in self.failed_workers

    async def stop_worker(self, worker_name, worker_state, cluser_info, cluster_state):
        self.stopped_workers.add(worker_name)


class WorkerFailsAfterConnect(InProcessClusterManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        loop = asyncio.get_running_loop()
        self.stop_worker_called = loop.create_future()
        self.worker_connected = loop.create_future()

    def on_worker_running(self, *args, **kwargs):
        self.worker_connected.set_result(True)

    async def stop_worker(self, *args, **kwargs):
        await super().stop_worker(*args, **kwargs)
        if not self.stop_worker_called.done():
            self.stop_worker_called.set_result(True)


@pytest.mark.asyncio
async def test_shutdown_on_startup_error(tmpdir):
    # A configuration that will cause a failure at runtime (not init time)
    gateway = DaskGateway(
        gateway_url="tls://127.0.0.1:%d" % random_port(),
        private_url="http://127.0.0.1:%d" % random_port(),
        public_url="http://127.0.0.1:%d" % random_port(),
        temp_dir=str(tmpdir.join("dask-gateway")),
        tls_cert=str(tmpdir.join("tls_cert.pem")),
        authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
    )
    with pytest.raises(SystemExit) as exc:
        gateway.initialize([])
        await gateway.start_or_exit()
    assert exc.value.code == 1


def test_db_encrypt_keys_required(tmpdir):
    with pytest.raises(ValueError) as exc:
        gateway = DaskGateway(
            gateway_url="tls://127.0.0.1:%d" % random_port(),
            private_url="http://127.0.0.1:%d" % random_port(),
            public_url="http://127.0.0.1:%d" % random_port(),
            temp_dir=str(tmpdir.join("dask-gateway")),
            db_url="sqlite:///%s" % tmpdir.join("dask_gateway.sqlite"),
            authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
        )
        gateway.initialize([])

    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)


def test_db_encrypt_keys_invalid(tmpdir):
    with pytest.raises(ValueError) as exc:
        gateway = DaskGateway(
            gateway_url="tls://127.0.0.1:%d" % random_port(),
            private_url="http://127.0.0.1:%d" % random_port(),
            public_url="http://127.0.0.1:%d" % random_port(),
            temp_dir=str(tmpdir.join("dask-gateway")),
            db_url="sqlite:///%s" % tmpdir.join("dask_gateway.sqlite"),
            db_encrypt_keys=["abc"],
            authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
        )
        gateway.initialize([])

    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)


def test_db_decrypt_keys_from_env(monkeypatch):
    keys = [Fernet.generate_key(), Fernet.generate_key()]
    val = b";".join(keys).decode()
    monkeypatch.setenv("DASK_GATEWAY_ENCRYPT_KEYS", val)
    gateway = DaskGateway()
    assert gateway.db_encrypt_keys == keys


def test_resume_clusters_forbid_in_memory_db(tmpdir):
    with pytest.raises(ValueError) as exc:
        DaskGateway(
            gateway_url="tls://127.0.0.1:%d" % random_port(),
            private_url="http://127.0.0.1:%d" % random_port(),
            public_url="http://127.0.0.1:%d" % random_port(),
            temp_dir=str(tmpdir.join("dask-gateway")),
            db_url="sqlite://",
            stop_clusters_on_shutdown=False,
            authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
        )

    assert "stop_clusters_on_shutdown" in str(exc.value)


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
async def test_cluster_fails_during_start(tmpdir, fail_stage):

    async with temp_gateway(
        cluster_manager_class=ClusterFailsDuringStart,
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
async def test_cluster_fails_between_start_and_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = ClusterFailsBetweenStartAndConnect
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.ClusterFailsBetweenStartAndConnect.cluster_status_period = 0.1

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            # Submit cluster
            cluster_id = await gateway.submit()

            # Wait for cluster failure and stop_cluster called
            timeout = 5
            while timeout > 0:
                if cluster_id in gateway_proc.cluster_manager.stopped_clusters:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"


@pytest.mark.asyncio
async def test_cluster_fails_after_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = ClusterFailsAfterConnect
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.ClusterFailsAfterConnect.cluster_status_period = 0.25

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            # Cluster starts successfully
            cluster_id = await gateway.submit()
            await gateway.connect(cluster_id)

            # Wait for cluster to fail while running
            await asyncio.wait_for(gateway_proc.cluster_manager.failed, 3)

            # Stop cluster called to cleanup after failure
            await asyncio.wait_for(gateway_proc.cluster_manager.stop_cluster_called, 3)


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
async def test_worker_fails_during_start(tmpdir, fail_stage):

    async with temp_gateway(
        cluster_manager_class=WorkerFailsDuringStart,
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
async def test_worker_fails_between_start_and_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = WorkerFailsBetweenStartAndConnect
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.WorkerFailsBetweenStartAndConnect.worker_status_period = 0.1

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)

            # Wait for worker failure and stop_worker called
            timeout = 5
            while timeout > 0:
                if gateway_proc.cluster_manager.stopped_workers:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
async def test_worker_fails_after_connect(tmpdir):
    async with temp_gateway(
        cluster_manager_class=WorkerFailsAfterConnect,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            cluster = await gateway.new_cluster()
            await cluster.scale(1)

            # Wait for worker to connect
            await asyncio.wait_for(gateway_proc.cluster_manager.worker_connected, 30)

            # Close the worker
            worker = list(gateway_proc.cluster_manager.active_workers.values())[0]
            await worker.close(1)

            # Stop cluster called to cleanup after failure
            await asyncio.wait_for(gateway_proc.cluster_manager.stop_worker_called, 30)

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


@pytest.mark.asyncio
async def test_gateway_resume_clusters_after_shutdown(tmpdir):
    temp_dir = str(tmpdir.join("dask-gateway"))
    os.mkdir(temp_dir, mode=0o700)

    db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    db_encrypt_keys = [Fernet.generate_key()]

    async with temp_gateway(
        cluster_manager_class=LocalTestingClusterManager,
        temp_dir=temp_dir,
        db_url=db_url,
        db_encrypt_keys=db_encrypt_keys,
        stop_clusters_on_shutdown=False,
    ) as gateway_proc:

        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            cluster1_name = await gateway.submit()
            cluster1 = await gateway.connect(cluster1_name)
            await cluster1.scale(2)

            cluster2_name = await gateway.submit()
            await gateway.connect(cluster2_name)

            cluster3 = await gateway.new_cluster()
            await cluster3.shutdown()

    active_clusters = {c.name: c for c in gateway_proc.db.active_clusters()}

    # Active clusters are not stopped on shutdown
    assert active_clusters

    # Stop 1 worker in cluster 1
    worker = list(active_clusters[cluster1_name].workers.values())[0]
    pid = worker.state["pid"]
    os.kill(pid, signal.SIGTERM)

    # Stop cluster 2
    pid = active_clusters[cluster2_name].state["pid"]
    os.kill(pid, signal.SIGTERM)

    # Restart a new temp_gateway
    async with temp_gateway(
        cluster_manager_class=LocalTestingClusterManager,
        temp_dir=temp_dir,
        db_url=db_url,
        db_encrypt_keys=db_encrypt_keys,
        stop_clusters_on_shutdown=False,
        gateway_url=gateway_proc.gateway_url,
        private_url=gateway_proc.private_url,
        public_url=gateway_proc.public_url,
        check_cluster_timeout=2,
    ) as gateway_proc:

        active_clusters = list(gateway_proc.db.active_clusters())
        assert len(active_clusters) == 1

        cluster = active_clusters[0]

        assert cluster.name == cluster1_name
        assert len(cluster.active_workers) == 1

        # Check that cluster is available and everything still works
        async with Gateway(
            address=gateway_proc.public_url, asynchronous=True
        ) as gateway:

            cluster = await gateway.connect(cluster1_name)

            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            await cluster.shutdown()
