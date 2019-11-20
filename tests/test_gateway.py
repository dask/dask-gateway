import asyncio
import os
import socket
import signal
import time

import pytest
from cryptography.fernet import Fernet
from traitlets import Integer, Float
from traitlets.config import Config

import dask
from dask_gateway import Gateway, GatewayClusterError, GatewayWarning, GatewayCluster
from dask_gateway_server.app import DaskGateway
from dask_gateway_server.compat import get_running_loop
from dask_gateway_server.objects import ClusterStatus
from dask_gateway_server.managers import ClusterManager
from dask_gateway_server.managers.inprocess import InProcessClusterManager
from dask_gateway_server import options

from .utils import LocalTestingClusterManager, temp_gateway


@pytest.fixture(autouse=True)
def ensure_clusters_closed():
    instances = len(GatewayCluster._instances)
    for c in list(GatewayCluster._instances):
        if not c.asynchronous:
            c.close()
    assert instances == 0


class SlowStartClusterManager(ClusterManager):
    pause_time = Float(0.2, config=True)

    state_1 = {"state_1": 1}
    state_2 = {"state_2": 2}
    state_3 = {"state_3": 3}
    stop_cluster_state = None
    running = False

    async def start_cluster(self):
        self.running = True
        yield self.state_1
        await asyncio.sleep(self.pause_time)
        yield self.state_2
        await asyncio.sleep(self.pause_time)
        yield self.state_3

    async def cluster_status(self, cluster_state):
        return self.running

    async def stop_cluster(self, cluster_state):
        self.stop_cluster_state = cluster_state
        self.running = False


class ClusterFailsDuringStart(ClusterManager):
    fail_stage = Integer(1, config=True)

    stop_cluster_state = None

    async def start_cluster(self):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_cluster(self, cluster_state):
        self.stop_cluster_state = cluster_state


class ClusterFailsBetweenStartAndConnect(InProcessClusterManager):
    status = "starting"

    async def start_cluster(self):
        yield {"foo": "bar"}
        self.status = "failed"

    async def cluster_status(self, cluster_state):
        return self.status not in ("failed", "stopped")

    async def stop_cluster(self, cluster_state):
        self.status = "stopped"


class ClusterFailsAfterConnect(InProcessClusterManager):
    fail_after = 0.5

    async def start_cluster(self):
        # Initiate state
        loop = get_running_loop()
        self.failed = loop.create_future()
        self.stop_cluster_called = loop.create_future()
        # Start the cluster
        async for state in super().start_cluster():
            yield state
        # Launch a task to kill the cluster soon
        self.task_pool.create_task(self.delay_fail_cluster())

    async def delay_fail_cluster(self):
        await asyncio.sleep(self.fail_after)
        self.failed.set_result(True)

    async def cluster_status(self, cluster_state):
        if self.failed.done():
            return False
        return await super().cluster_status(cluster_state)

    async def stop_cluster(self, cluster_state):
        await super().stop_cluster(cluster_state)
        self.stop_cluster_called.set_result(True)


class SlowWorkerStartClusterManager(InProcessClusterManager):
    pause_time = Float(0.2, config=True)

    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_state):
        for i in range(3):
            yield {"i": i}
            await asyncio.sleep(self.pause_time)
        self.workers[worker_name] = None

    async def worker_status(self, worker_name, worker_state, cluster_state):
        return worker_name in self.workers

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        self.stop_worker_state = worker_state
        self.workers.pop(worker_name, None)


class WorkerFailsDuringStart(InProcessClusterManager):
    fail_stage = Integer(1, config=True)

    stop_worker_state = None

    async def start_worker(self, worker_name, cluster_state):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        self.stop_worker_state = worker_state


class WorkerFailsBetweenStartAndConnect(InProcessClusterManager):
    status = "starting"

    async def start_worker(self, worker_name, cluster_state):
        yield {"foo": "bar"}
        self.status = "failed"

    async def worker_status(self, worker_name, worker_state, cluster_state):
        return self.status not in ("failed", "stopped")

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        self.status = "stopped"


class WorkerFailsAfterConnect(InProcessClusterManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        loop = get_running_loop()
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
        gateway_url="tls://127.0.0.1:0",
        private_url="http://127.0.0.1:0",
        public_url="http://127.0.0.1:0",
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
            gateway_url="tls://127.0.0.1:0",
            private_url="http://127.0.0.1:0",
            public_url="http://127.0.0.1:0",
            temp_dir=str(tmpdir.join("dask-gateway")),
            db_url="sqlite:///%s" % tmpdir.join("dask_gateway.sqlite"),
            authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
        )
        gateway.initialize([])

    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)


def test_db_encrypt_keys_invalid(tmpdir):
    with pytest.raises(ValueError) as exc:
        gateway = DaskGateway(
            gateway_url="tls://127.0.0.1:0",
            private_url="http://127.0.0.1:0",
            public_url="http://127.0.0.1:0",
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
            gateway_url="tls://127.0.0.1:0",
            private_url="http://127.0.0.1:0",
            public_url="http://127.0.0.1:0",
            temp_dir=str(tmpdir.join("dask-gateway")),
            db_url="sqlite://",
            stop_clusters_on_shutdown=False,
            authenticator_class="dask_gateway_server.auth.DummyAuthenticator",
        )

    assert "stop_clusters_on_shutdown" in str(exc.value)


def test_default_urls_from_config():
    hostname = socket.gethostname()
    gateway = DaskGateway()
    gateway.init_server_urls()

    assert gateway.public_urls.bind_host == ""
    assert gateway.public_urls.connect.hostname == hostname
    assert gateway.gateway_urls.bind.scheme == "tls"
    assert gateway.gateway_urls.bind_host == ""
    assert gateway.gateway_urls.connect.hostname == hostname
    assert gateway.private_urls.bind_port != 0


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, "state_1"), (0.25, "state_2")])
async def test_slow_cluster_start(tmpdir, start_timeout, state):

    config = Config()
    config.DaskGateway.cluster_manager_class = SlowStartClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.SlowStartClusterManager.cluster_start_timeout = start_timeout

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Submission fails due to start timeout
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            cluster = gateway_proc.db.cluster_from_name(cluster_id)

            # Stop cluster called with last reported state
            res = getattr(cluster.manager, state)
            assert cluster.manager.stop_cluster_state == res


@pytest.mark.asyncio
async def test_slow_cluster_connect(tmpdir):

    config = Config()
    config.DaskGateway.cluster_manager_class = SlowStartClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.SlowStartClusterManager.cluster_start_timeout = 0.1
    config.SlowStartClusterManager.pause_time = 0

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Submission fails due to connect timeout
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            cluster = gateway_proc.db.cluster_from_name(cluster_id)

            # Stop cluster called with last reported state
            res = cluster.manager.state_3
            assert cluster.manager.stop_cluster_state == res


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_cluster_fails_during_start(tmpdir, fail_stage):

    config = Config()
    config.DaskGateway.cluster_manager_class = ClusterFailsDuringStart
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.ClusterFailsDuringStart.fail_stage = fail_stage

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Submission fails due to error during start
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            cluster_obj = gateway_proc.db.cluster_from_name(cluster_id)

            # Stop cluster called with last reported state
            res = {} if fail_stage == 0 else {"i": fail_stage - 1}
            assert cluster_obj.manager.stop_cluster_state == res


@pytest.mark.asyncio
async def test_cluster_fails_between_start_and_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = ClusterFailsBetweenStartAndConnect
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.ClusterFailsBetweenStartAndConnect.cluster_status_period = 0.1

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Submit cluster
            cluster_id = await gateway.submit()

            cluster_obj = gateway_proc.db.cluster_from_name(cluster_id)

            # Connect and wait for start failure
            with pytest.raises(GatewayClusterError) as exc:
                await asyncio.wait_for(gateway.connect(cluster_id), 5)
            assert cluster_id in str(exc.value)
            assert "failed to start" in str(exc.value)

            assert cluster_obj.manager.status == "stopped"


@pytest.mark.asyncio
async def test_cluster_fails_after_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = ClusterFailsAfterConnect
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.ClusterFailsAfterConnect.cluster_status_period = 0.25

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Cluster starts successfully
            cluster_id = await gateway.submit()

            cluster_obj = gateway_proc.db.cluster_from_name(cluster_id)

            async with gateway.connect(cluster_id):
                # Wait for cluster to fail while running
                await asyncio.wait_for(cluster_obj.manager.failed, 3)

                # Stop cluster called to cleanup after failure
                await asyncio.wait_for(cluster_obj.manager.stop_cluster_called, 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, 0), (0.25, 1)])
async def test_slow_worker_start(tmpdir, start_timeout, state):
    config = Config()
    config.DaskGateway.cluster_manager_class = SlowWorkerStartClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.SlowWorkerStartClusterManager.worker_start_timeout = start_timeout

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if cluster_obj.manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            assert cluster_obj.manager.stop_worker_state == {"i": state}

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
async def test_slow_worker_connect(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = SlowWorkerStartClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.SlowWorkerStartClusterManager.worker_start_timeout = 0.1
    config.SlowWorkerStartClusterManager.pause_time = 0

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if cluster_obj.manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            assert cluster_obj.manager.stop_worker_state == {"i": 2}

            # Stop the cluster
            await cluster.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_worker_fails_during_start(tmpdir, fail_stage):
    config = Config()
    config.DaskGateway.cluster_manager_class = WorkerFailsDuringStart
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.WorkerFailsDuringStart.fail_stage = fail_stage

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)

            # Wait for worker failure
            timeout = 5
            while timeout > 0:
                if cluster_obj.manager.stop_worker_state is not None:
                    break
                await asyncio.sleep(0.1)
                timeout -= 0.1
            else:
                assert False, "Operation timed out"

            # Stop worker called with last reported state
            res = {} if fail_stage == 0 else {"i": fail_stage - 1}
            assert cluster_obj.manager.stop_worker_state == res

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
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            cluster = await gateway.new_cluster()
            await cluster.scale(1)
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)

            # Wait for worker failure and stop_worker called
            timeout = 5
            while timeout > 0:
                if cluster_obj.manager.status == "stopped":
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
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            cluster = await gateway.new_cluster()
            await cluster.scale(1)
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)

            # Wait for worker to connect
            await asyncio.wait_for(cluster_obj.manager.worker_connected, 30)

            # Close the worker
            worker = list(cluster_obj.manager.workers.values())[0]
            await worker.close(1)

            # Stop cluster called to cleanup after failure
            await asyncio.wait_for(cluster_obj.manager.stop_worker_called, 30)

            await cluster.shutdown()


@pytest.mark.asyncio
async def test_successful_cluster(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # No currently running clusters
            clusters = await gateway.list_clusters()
            assert clusters == []

            # Start a cluster
            cluster = await gateway.new_cluster()

            # Cluster is now present in list
            clusters = await gateway.list_clusters()
            assert len(clusters)
            assert clusters[0].name == cluster.name

            # Scale up, connect, and compute
            await cluster.scale(2)

            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            # Scale down
            await cluster.scale(1)

            # Can still compute
            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            # Shutdown the cluster
            await cluster.shutdown()


class ClusterOptionsManager(InProcessClusterManager):
    option_two = Float(config=True)
    option_one_b = Integer(config=True)


@pytest.mark.asyncio
async def test_cluster_manager_options(tmpdir):
    async with temp_gateway(
        cluster_manager_class=ClusterOptionsManager,
        cluster_manager_options=options.Options(
            options.Integer(
                "option_one", default=1, min=1, max=4, target="option_one_b"
            ),
            options.Select("option_two", options=[("small", 1.5), ("large", 15)]),
        ),
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            # Create with no parameters
            cluster = await gateway.new_cluster()
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
            assert cluster_obj.manager.option_one_b == 1
            assert cluster_obj.manager.option_two == 1.5
            assert cluster_obj.options == {"option_one": 1, "option_two": "small"}
            await cluster.shutdown()

            # Create with parameters
            cluster = await gateway.new_cluster(option_two="large")
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
            assert cluster_obj.manager.option_one_b == 1
            assert cluster_obj.manager.option_two == 15
            assert cluster_obj.options == {"option_one": 1, "option_two": "large"}
            await cluster.shutdown()

            # With options object
            opts = await gateway.cluster_options()
            opts.option_one = 2
            cluster = await gateway.new_cluster(opts, option_two="large")
            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
            assert cluster_obj.manager.option_one_b == 2
            assert cluster_obj.manager.option_two == 15
            assert cluster_obj.options == {"option_one": 2, "option_two": "large"}
            await cluster.shutdown()

            # Bad parameters
            with pytest.raises(TypeError):
                await gateway.new_cluster(cluster_options=10)

            with pytest.raises(ValueError) as exc:
                await gateway.new_cluster(option_two="medium")
            assert "option_two" in str(exc.value)


@pytest.mark.asyncio
async def test_cluster_manager_options_client_config(tmpdir, monkeypatch):
    monkeypatch.setenv("TEST_OPTION_TWO", "large")

    async with temp_gateway(
        cluster_manager_class=ClusterOptionsManager,
        cluster_manager_options=options.Options(
            options.Integer(
                "option_one", default=1, min=1, max=4, target="option_one_b"
            ),
            options.Select("option_two", options=[("small", 1.5), ("large", 15)]),
        ),
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            with dask.config.set(gateway__cluster__options={"option_one": 2}):
                # Local config can override
                opts = await gateway.cluster_options()
                assert opts.option_one == 2
                assert opts.option_two == "small"
                # Without local config, uses server-side defaults
                opts = await gateway.cluster_options(use_local_defaults=False)
                assert opts.option_one == 1
                assert opts.option_two == "small"

            with dask.config.set(
                gateway__cluster__options={"option_two": "{TEST_OPTION_TWO}"}
            ):
                # Values can format from environment variables
                opts = await gateway.cluster_options()
                assert opts.option_one == 1
                assert opts.option_two == "large"

            with dask.config.set(
                gateway__cluster__options={
                    "option_two": "{TEST_OPTION_TWO}",
                    "option_one": 3,
                }
            ):
                # Defaults are merged with kwargs to new_cluster
                cluster = await gateway.new_cluster(option_one=2)
                cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
                assert cluster_obj.options == {"option_one": 2, "option_two": "large"}
                await cluster.shutdown()

                # If passing `cluster_options`, defaults are assumed already applied
                opts = await gateway.cluster_options()
                opts.option_two = "small"
                cluster = await gateway.new_cluster(opts)
                cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
                assert cluster_obj.options == {"option_one": 3, "option_two": "small"}
                await cluster.shutdown()


@pytest.mark.asyncio
async def test_gateway_stop_clusters_on_shutdown(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            cluster = await gateway.new_cluster()
            cluster2 = await gateway.new_cluster()
            await cluster2.shutdown()

            cluster_obj = gateway_proc.db.cluster_from_name(cluster.name)
            cluster_obj2 = gateway_proc.db.cluster_from_name(cluster2.name)

            # There is an active cluster
            assert cluster_obj.manager.scheduler is not None

    # Active clusters are stopped on shutdown
    for c in [cluster_obj, cluster_obj2]:
        assert c.manager.scheduler is None
        assert c.status >= ClusterStatus.STOPPED


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
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:

            cluster1_name = await gateway.submit()
            async with gateway.connect(cluster1_name) as c:
                await c.scale(2)

            cluster2_name = await gateway.submit()
            async with gateway.connect(cluster2_name):
                pass

            async with gateway.new_cluster():
                pass

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
        gateway_url=gateway_proc.gateway_urls.connect_url,
        private_url=gateway_proc.private_urls.connect_url,
        public_url=gateway_proc.public_urls.connect_url,
        check_cluster_timeout=2,
    ) as gateway_proc:

        active_clusters = list(gateway_proc.db.active_clusters())
        assert len(active_clusters) == 1

        cluster = active_clusters[0]

        assert cluster.name == cluster1_name
        assert len(cluster.active_workers()) == 1

        # Check that cluster is available and everything still works
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            async with gateway.connect(
                cluster1_name, shutdown_on_close=True
            ) as cluster:
                with cluster.get_client(set_as_default=False) as client:
                    res = await client.submit(lambda x: x + 1, 1)
                    assert res == 2


@pytest.mark.asyncio
async def test_user_limits(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = InProcessClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.UserLimits.max_clusters = 1
    config.UserLimits.max_cores = 3
    config.InProcessClusterManager.scheduler_cores = 1
    config.InProcessClusterManager.worker_cores = 2

    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # Start a cluster
            cluster = await gateway.new_cluster()

            # Only one cluster allowed
            with pytest.raises(ValueError) as exc:
                await gateway.new_cluster()
            assert "user limit" in str(exc.value)

            # Scaling > 1 triggers a warning, only scales to 1
            with pytest.warns(GatewayWarning, match="user cores limit"):
                await cluster.scale(2)

            # Shutdown the cluster
            await cluster.shutdown()

            # Can create a new cluster after resources returned
            cluster = await gateway.new_cluster()
            await cluster.shutdown()


async def wait_for_workers(cluster, atleast=None, exact=None, timeout=30):
    timeout = time.time() + timeout
    while time.time() < timeout:
        workers = cluster.scheduler_info.get("workers")
        nworkers = len(workers)
        if atleast is not None and nworkers >= atleast:
            break
        elif exact is not None and nworkers == exact:
            break
        await asyncio.sleep(0.25)
    else:
        assert False, "scaling timed out"


@pytest.mark.asyncio
async def test_scaling(tmpdir):
    config = Config()
    config.DaskGateway.cluster_manager_class = InProcessClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # Start a cluster
            cluster = await gateway.new_cluster()

            await cluster.scale(5)
            await wait_for_workers(cluster, atleast=3)

            await cluster.scale(1)
            await wait_for_workers(cluster, exact=1)

            await cluster.shutdown()


@pytest.mark.asyncio
async def test_adaptive_scaling(tmpdir):
    # XXX: we should be able to use `InProcessClusterManager` here, but due to
    # https://github.com/dask/distributed/issues/3251 this results in periodic
    # failures.
    config = Config()
    config.DaskGateway.cluster_manager_class = LocalTestingClusterManager
    config.DaskGateway.temp_dir = str(tmpdir.join("dask-gateway"))
    config.LocalTestingClusterManager.adaptive_period = 0.25
    async with temp_gateway(config=config) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # Start a cluster
            cluster = await gateway.new_cluster()

            async def is_adaptive():
                report = await gateway.get_cluster(cluster.name)
                return report.adaptive

            # Not in adaptive mode
            assert not await is_adaptive()

            # Turn on adaptive scaling
            await cluster.adapt()

            # Now in adaptive mode
            assert await is_adaptive()

            # Worker is automatically requested
            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            # Scales down automatically
            await wait_for_workers(cluster, exact=0)

            # Still in adaptive mode
            assert await is_adaptive()

            # Turn off adaptive scaling implicitly
            await cluster.scale(1)
            assert not await is_adaptive()

            # Turn off adaptive scaling explicitly
            await cluster.adapt()
            assert await is_adaptive()
            await cluster.adapt(active=False)
            assert not await is_adaptive()

            # Shutdown the cluster
            await cluster.shutdown()
