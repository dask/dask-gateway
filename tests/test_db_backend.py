import asyncio
import base64
import os
import signal
import time
from collections import defaultdict

import pytest
from cryptography.fernet import Fernet
from traitlets import Integer, Float
from traitlets.config import Config

import dask
from dask_gateway import GatewayCluster, GatewayClusterError, GatewayWarning
from dask_gateway_server.app import DaskGateway
from dask_gateway_server.backends import db_base
from dask_gateway_server.backends.base import ClusterConfig
from dask_gateway_server.backends.db_base import (
    DBBackendBase,
    timestamp,
    JobStatus,
    DataManager,
)
from dask_gateway_server.backends.inprocess import InProcessBackend
from dask_gateway_server.compat import get_running_loop
from dask_gateway_server.utils import random_port
from dask_gateway_server import options

from .utils_test import temp_gateway, LocalTestingBackend, wait_for_workers


@pytest.fixture(autouse=True)
def ensure_clusters_closed():
    instances = len(GatewayCluster._instances)
    for c in list(GatewayCluster._instances):
        if not c.asynchronous:
            c.close()
    assert instances == 0


class ClusterSlowToStart(DBBackendBase):
    pause_time = Float(0.25, config=True)

    state_1 = {"state": 1}
    state_2 = {"state": 2}
    state_3 = {"state": 3}
    stop_cluster_state = None
    running = False

    async def do_start_cluster(self, cluster):
        self.running = True
        yield self.state_1
        await asyncio.sleep(self.pause_time)
        yield self.state_2
        await asyncio.sleep(self.pause_time)
        yield self.state_3

    async def do_check_clusters(self, clusters):
        return [self.running for _ in clusters]

    async def do_stop_cluster(self, cluster):
        self.stop_cluster_state = cluster.state
        self.running = False


class ClusterFailsDuringStart(DBBackendBase):
    fail_stage = Integer(1, config=True)

    stop_cluster_state = None

    async def do_start_cluster(self, cluster):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def do_check_clusters(self, clusters):
        return [True] * len(clusters)

    async def do_stop_cluster(self, cluster):
        self.stop_cluster_state = cluster.state


class ClusterFailsBetweenStartAndConnect(InProcessBackend):
    status = "starting"

    async def do_start_cluster(self, cluster):
        yield {"foo": "bar"}
        self.status = "failed"

    async def do_check_clusters(self, clusters):
        return [self.status not in ("failed", "stopped") for c in clusters]

    async def do_stop_cluster(self, cluster):
        self.status = "stopped"


class ClusterFailsAfterConnect(InProcessBackend):
    async def do_setup(self):
        await super().do_setup()
        loop = get_running_loop()
        self.stop_cluster_called = loop.create_future()

    async def do_stop_cluster(self, cluster):
        if not self.stop_cluster_called.done():
            self.stop_cluster_called.set_result(True)


class TracksStopWorkerCalls(InProcessBackend):
    async def do_setup(self):
        await super().do_setup()
        loop = get_running_loop()
        self.stop_worker_called = loop.create_future()
        self.stop_worker_state = None

    async def do_stop_worker(self, worker):
        if not self.stop_worker_called.done():
            self.stop_worker_called.set_result(True)
            self.stop_worker_state = worker.state


class WorkerSlowToStart(TracksStopWorkerCalls):
    pause_time = Float(0.2, config=True)

    async def do_start_worker(self, worker):
        for i in range(3):
            yield {"i": i}
            await asyncio.sleep(self.pause_time)


class WorkerFailsDuringStart(TracksStopWorkerCalls):
    fail_stage = Integer(1, config=True)

    async def do_setup(self):
        await super().do_setup()
        loop = get_running_loop()
        self.stop_cluster_called = loop.create_future()

    async def do_start_worker(self, worker):
        for i in range(3):
            if i == self.fail_stage:
                raise ValueError("Oh No")
            yield {"i": i}

    async def do_stop_cluster(self, cluster):
        if not self.stop_cluster_called.done():
            self.stop_cluster_called.set_result(True)
        await super().do_stop_cluster(cluster)


class WorkerFailsBetweenStartAndConnect(TracksStopWorkerCalls):
    async def do_start_worker(self, worker):
        yield {"state": 1}

    async def do_check_workers(self, workers):
        return [False] * len(workers)


def test_shutdown_on_startup_error(tmpdir, capsys):
    # A configuration that will cause a failure at runtime (not init time)
    c = Config()
    c.Proxy.tls_cert = str(tmpdir.join("tls_cert.pem"))
    gateway = DaskGateway(config=c)
    with pytest.raises(SystemExit) as exc:
        gateway.initialize([])
        gateway.start()
    assert exc.value.code == 1

    captured = capsys.readouterr()

    assert "tls_cert" in captured.err


def test_db_encrypt_keys_required(tmpdir, capsys):
    c = Config()
    c.DBBackendBase.db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    with pytest.raises(SystemExit) as exc:
        gateway = DaskGateway(config=c)
        gateway.initialize([])
        gateway.start()
    assert exc.value.code == 1

    captured = capsys.readouterr()

    assert "DASK_GATEWAY_ENCRYPT_KEYS" in captured.err


def test_db_encrypt_keys_invalid(tmpdir):
    c = Config()
    c.DBBackendBase.db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    c.DBBackendBase.db_encrypt_keys = ["abc"]
    with pytest.raises(ValueError) as exc:
        gateway = DaskGateway(config=c)
        gateway.initialize([])

    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)


def test_db_encrypt_keys_from_env(monkeypatch):
    keys = [Fernet.generate_key(), Fernet.generate_key()]
    val = b";".join(keys).decode()
    monkeypatch.setenv("DASK_GATEWAY_ENCRYPT_KEYS", val)
    gateway = DaskGateway()
    gateway.initialize([])
    assert gateway.backend.db_encrypt_keys == keys


def test_resume_clusters_forbid_in_memory_db():
    c = Config()
    c.DBBackendBase.db_url = "sqlite://"
    c.DBBackendBase.stop_clusters_on_shutdown = False
    with pytest.raises(ValueError) as exc:
        gateway = DaskGateway(config=c)
        gateway.initialize([])

    assert "stop_clusters_on_shutdown" in str(exc.value)


@pytest.mark.asyncio
async def test_encryption(tmpdir):
    db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    encrypt_keys = [Fernet.generate_key() for i in range(3)]
    db = DataManager(url=db_url, encrypt_keys=encrypt_keys)

    assert db.fernet is not None

    data = b"my secret data"
    encrypted = db.encrypt(data)
    assert encrypted != data

    data2 = db.decrypt(encrypted)
    assert data == data2

    c = db.create_cluster("alice", {}, {})
    assert c.tls_cert is not None
    assert c.tls_key is not None

    # Check database state is encrypted
    with db.db.begin() as conn:
        res = conn.execute(
            db_base.clusters.select(db_base.clusters.c.id == c.id)
        ).fetchone()
    assert res.tls_credentials != b";".join((c.tls_cert, c.tls_key))
    cert, key = db.decrypt(res.tls_credentials).split(b";")
    token = db.decrypt(res.token).decode()
    assert cert == c.tls_cert
    assert key == c.tls_key
    assert token == c.token

    # Check can reload database with keys
    db2 = DataManager(url=db_url, encrypt_keys=encrypt_keys)
    c2 = db2.id_to_cluster[c.id]
    assert c2.tls_cert == c.tls_cert
    assert c2.tls_key == c.tls_key
    assert c2.token == c.token


def test_normalize_encrypt_key():
    key = Fernet.generate_key()
    # b64 bytes
    assert db_base._normalize_encrypt_key(key) == key
    # b64 string
    assert db_base._normalize_encrypt_key(key.decode()) == key
    # raw bytes
    raw = base64.urlsafe_b64decode(key)
    assert db_base._normalize_encrypt_key(raw) == key

    # Too short
    with pytest.raises(ValueError) as exc:
        db_base._normalize_encrypt_key(b"abcde")
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)

    # Too short decoded
    with pytest.raises(ValueError) as exc:
        db_base._normalize_encrypt_key(b"\x00" * 43 + b"=")
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)

    # Invalid b64 encode
    with pytest.raises(ValueError) as exc:
        db_base._normalize_encrypt_key(b"=" + b"a" * 43)
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)


def check_db_consistency(db):
    for u, clusters in db.username_to_clusters.items():
        # Users without clusters are flushed
        assert clusters

    clusters = db.db.execute(db_base.clusters.select()).fetchall()
    workers = db.db.execute(db_base.workers.select()).fetchall()

    # Check cluster state
    for c in clusters:
        cluster = db.id_to_cluster[c.id]
        assert db.name_to_cluster[c.name] is cluster
        assert db.username_to_clusters[c.username][c.name] is cluster
    assert len(db.id_to_cluster) == len(clusters)
    assert len(db.name_to_cluster) == len(clusters)
    assert sum(map(len, db.username_to_clusters.values())) == len(clusters)

    # Check worker state
    cluster_to_workers = defaultdict(set)
    for w in workers:
        cluster = db.id_to_cluster[w.cluster_id]
        cluster_to_workers[cluster.name].add(w.name)
    for cluster in db.id_to_cluster.values():
        expected = cluster_to_workers[cluster.name]
        assert set(cluster.workers) == expected


@pytest.mark.asyncio
async def test_cleanup_expired_clusters(monkeypatch):
    db = DataManager()

    current_time = time.time()

    def mytime():
        nonlocal current_time
        current_time += 0.5
        return current_time

    monkeypatch.setattr(time, "time", mytime)

    def add_cluster(user, stop=True):
        c = db.create_cluster(user, {}, {})
        for _ in range(5):
            w = db.create_worker(c)
            if stop:
                db.update_worker(
                    w,
                    target=JobStatus.STOPPED,
                    status=JobStatus.STOPPED,
                    stop_time=timestamp(),
                )
        if stop:
            db.update_cluster(
                c,
                status=JobStatus.STOPPED,
                target=JobStatus.STOPPED,
                stop_time=timestamp(),
            )
        return c

    add_cluster("alice", stop=True)  # c1
    add_cluster("alice", stop=True)  # c2
    add_cluster("bob", stop=True)  # c3
    c4 = add_cluster("alice", stop=False)

    cutoff = mytime()

    c5 = add_cluster("alice", stop=True)
    c6 = add_cluster("alice", stop=False)

    check_db_consistency(db)

    # Set time to always return same value
    now = mytime()
    monkeypatch.setattr(time, "time", lambda: now)

    # 3 clusters are expired
    max_age = now - cutoff
    n = db.cleanup_expired(max_age)
    assert n == 3

    check_db_consistency(db)

    # Only alice remains, bob is removed since they have no clusters
    assert "alice" in db.username_to_clusters
    assert "bob" not in db.username_to_clusters

    # c4, c5, c6 are all that remains
    assert set(db.id_to_cluster) == {c4.id, c5.id, c6.id}

    # Running again expires no clusters
    max_age = now - cutoff
    n = db.cleanup_expired(max_age)
    assert n == 0

    check_db_consistency(db)


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, 1), (0.4, 2)])
async def test_slow_cluster_start(start_timeout, state):
    config = Config()
    config.DaskGateway.backend_class = ClusterSlowToStart
    config.ClusterSlowToStart.check_timeouts_period = 0.05
    config.ClusterSlowToStart.cluster_start_timeout = start_timeout

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Submission fails due to start timeout
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            assert g.gateway.backend.stop_cluster_state == {"state": state}


@pytest.mark.asyncio
async def test_slow_cluster_connect():
    config = Config()
    config.DaskGateway.backend_class = ClusterSlowToStart
    config.ClusterSlowToStart.check_timeouts_period = 0.05
    config.ClusterSlowToStart.cluster_start_timeout = 0.1
    config.ClusterSlowToStart.pause_time = 0
    config.DaskGateway.log_level = "DEBUG"

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Submission fails due to connect timeout
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            assert g.gateway.backend.stop_cluster_state == {"state": 3}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_cluster_fails_during_start(fail_stage):

    config = Config()
    config.DaskGateway.backend_class = ClusterFailsDuringStart
    config.ClusterFailsDuringStart.fail_stage = fail_stage

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Submission fails due to error during start
            cluster_id = await gateway.submit()
            with pytest.raises(GatewayClusterError) as exc:
                async with gateway.connect(cluster_id):
                    pass
            assert cluster_id in str(exc.value)

            # Stop cluster called with last reported state
            res = {} if fail_stage == 0 else {"i": fail_stage - 1}
            assert g.gateway.backend.stop_cluster_state == res


@pytest.mark.asyncio
async def test_cluster_fails_between_start_and_connect():
    config = Config()
    config.DaskGateway.backend_class = ClusterFailsBetweenStartAndConnect
    config.ClusterFailsBetweenStartAndConnect.cluster_status_period = 0.1

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Submit cluster
            cluster_id = await gateway.submit()

            # Connect and wait for start failure
            with pytest.raises(GatewayClusterError) as exc:
                await asyncio.wait_for(gateway.connect(cluster_id), 5)
            assert cluster_id in str(exc.value)
            assert "failed to start" in str(exc.value)

            assert g.gateway.backend.status == "stopped"


@pytest.mark.asyncio
async def test_cluster_fails_after_connect():
    config = Config()
    config.DaskGateway.backend_class = ClusterFailsAfterConnect
    config.DaskGateway.log_level = "DEBUG"
    config.ClusterFailsAfterConnect.cluster_heartbeat_period = 1
    config.ClusterFailsAfterConnect.check_timeouts_period = 0.5

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Cluster starts successfully
            async with gateway.new_cluster() as cluster:
                # Kill scheduler
                scheduler = g.gateway.backend.schedulers[cluster.name]
                await scheduler.close(fast=True)
                scheduler.stop()

                # Gateway notices and cleans up cluster in time
                await asyncio.wait_for(g.gateway.backend.stop_cluster_called, 5)


@pytest.mark.asyncio
@pytest.mark.parametrize("start_timeout,state", [(0.1, 0), (0.25, 1)])
async def test_slow_worker_start(start_timeout, state):
    config = Config()
    config.DaskGateway.backend_class = WorkerSlowToStart
    config.WorkerSlowToStart.worker_start_timeout = start_timeout
    config.WorkerSlowToStart.check_timeouts_period = 0.05

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)

                # Wait for worker failure
                await asyncio.wait_for(g.gateway.backend.stop_worker_called, 5)

                # Stop worker called with last reported state
                assert g.gateway.backend.stop_worker_state == {"i": state}


@pytest.mark.asyncio
async def test_slow_worker_connect():
    config = Config()
    config.DaskGateway.backend_class = WorkerSlowToStart
    config.WorkerSlowToStart.pause_time = 0
    config.WorkerSlowToStart.worker_start_timeout = 0.1
    config.WorkerSlowToStart.check_timeouts_period = 0.05

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)

                # Wait for worker failure
                await asyncio.wait_for(g.gateway.backend.stop_worker_called, 5)

                # Stop worker called with last reported state
                assert g.gateway.backend.stop_worker_state == {"i": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_stage", [0, 1])
async def test_worker_fails_during_start(fail_stage):
    config = Config()
    config.DaskGateway.backend_class = WorkerFailsDuringStart
    config.WorkerFailsDuringStart.fail_stage = fail_stage

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)

                # Wait for worker failure
                await asyncio.wait_for(g.gateway.backend.stop_worker_called, 5)

                # Stop worker called with last reported state
                res = {} if fail_stage == 0 else {"i": fail_stage - 1}
                assert g.gateway.backend.stop_worker_state == res


@pytest.mark.asyncio
async def test_worker_fails_between_start_and_connect():
    config = Config()
    config.DaskGateway.backend_class = WorkerFailsBetweenStartAndConnect
    config.WorkerFailsBetweenStartAndConnect.worker_status_period = 0.1

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)

                # Wait for worker failure
                await asyncio.wait_for(g.gateway.backend.stop_worker_called, 5)

                assert g.gateway.backend.stop_worker_state == {"state": 1}


@pytest.mark.asyncio
async def test_worker_fails_after_connect():
    config = Config()
    config.DaskGateway.backend_class = TracksStopWorkerCalls
    config.TracksStopWorkerCalls.cluster_heartbeat_period = 1
    config.TracksStopWorkerCalls.check_timeouts_period = 0.5
    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)
                await wait_for_workers(cluster, atleast=1)

                # Close the worker
                worker = list(g.gateway.backend.workers.values())[0]
                await worker.close(1)

                # Stop cluster called to cleanup after failure
                await asyncio.wait_for(g.gateway.backend.stop_worker_called, 30)


@pytest.mark.asyncio
async def test_worker_start_failure_limit():
    config = Config()
    config.DaskGateway.backend_class = WorkerFailsDuringStart

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(1)

                # Wait for cluster failure
                await asyncio.wait_for(g.gateway.backend.stop_cluster_called, 10)


@pytest.mark.asyncio
async def test_successful_cluster():
    async with temp_gateway() as g:
        async with g.gateway_client() as gateway:
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

            async with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            # Scale down
            await cluster.scale(1)

            # Can still compute
            async with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

            # Shutdown the cluster
            await cluster.shutdown()


class MyClusterConfig(ClusterConfig):
    option_two = Float(config=True)
    option_one_b = Integer(config=True)


@pytest.mark.asyncio
async def test_cluster_options():
    config = Config()
    config.InProcessBackend.cluster_config_class = MyClusterConfig
    config.InProcessBackend.cluster_options = options.Options(
        options.Integer("option_one", default=1, min=1, max=4, target="option_one_b"),
        options.Select("option_two", options=[("small", 1.5), ("large", 15)]),
    )
    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Create with no parameters
            async with gateway.new_cluster() as cluster:
                report = await gateway.get_cluster(cluster.name)
                assert report.options == {"option_one": 1, "option_two": "small"}

            # Create with parameters
            async with gateway.new_cluster(option_two="large") as cluster:
                report = await gateway.get_cluster(cluster.name)
                assert report.options == {"option_one": 1, "option_two": "large"}

            # With options object
            opts = await gateway.cluster_options()
            opts.option_one = 2
            async with gateway.new_cluster(opts, option_two="large") as cluster:
                report = await gateway.get_cluster(cluster.name)
                assert report.options == {"option_one": 2, "option_two": "large"}

            # Bad parameters
            with pytest.raises(TypeError):
                await gateway.new_cluster(cluster_options=10)

            with pytest.raises(ValueError) as exc:
                await gateway.new_cluster(option_two="medium")
            assert "option_two" in str(exc.value)


@pytest.mark.asyncio
async def test_cluster_options_client_config(monkeypatch):
    monkeypatch.setenv("TEST_OPTION_TWO", "large")

    config = Config()
    config.InProcessBackend.cluster_config_class = MyClusterConfig
    config.InProcessBackend.cluster_options = options.Options(
        options.Integer("option_one", default=1, min=1, max=4, target="option_one_b"),
        options.Select("option_two", options=[("small", 1.5), ("large", 15)]),
    )

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
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
                async with gateway.new_cluster(option_one=2) as cluster:
                    report = await gateway.get_cluster(cluster.name)
                    assert report.options == {"option_one": 2, "option_two": "large"}

                # If passing `cluster_options`, defaults are assumed already applied
                opts = await gateway.cluster_options()
                opts.option_two = "small"
                async with gateway.new_cluster(opts) as cluster:
                    report = await gateway.get_cluster(cluster.name)
                    assert report.options == {"option_one": 3, "option_two": "small"}


@pytest.mark.asyncio
async def test_gateway_stop_clusters_on_shutdown():
    async with temp_gateway() as g:
        async with g.gateway_client() as gateway:
            await gateway.new_cluster()
            async with gateway.new_cluster():
                pass

    # No active clusters after shutdown
    assert not list(g.gateway.backend.db.active_clusters())


@pytest.mark.asyncio
async def test_gateway_resume_clusters_after_shutdown(tmpdir):
    db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    db_encrypt_keys = [Fernet.generate_key()]

    config = Config()
    config.DaskGateway.backend_class = LocalTestingBackend
    config.LocalTestingBackend.db_url = db_url
    config.LocalTestingBackend.db_encrypt_keys = db_encrypt_keys
    config.LocalTestingBackend.stop_clusters_on_shutdown = False
    config.LocalTestingBackend.cluster_heartbeat_period = 1
    config.LocalTestingBackend.check_timeouts_period = 0.5
    config.DaskGateway.address = "127.0.0.1:%d" % random_port()
    config.Proxy.address = "127.0.0.1:%d" % random_port()

    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            cluster1_name = await gateway.submit()
            async with gateway.connect(cluster1_name) as c:
                await c.scale(2)

            cluster2_name = await gateway.submit()
            async with gateway.connect(cluster2_name):
                pass

            async with gateway.new_cluster():
                pass

    active_clusters = {c.name: c for c in g.gateway.backend.db.active_clusters()}

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
    config.LocalTestingBackend.stop_clusters_on_shutdown = True
    async with temp_gateway(config=config) as g:
        backend = g.gateway.backend
        for retry in range(10):
            try:
                clusters = list(backend.db.active_clusters())
                assert len(clusters) == 1

                cluster = clusters[0]

                assert cluster.name == cluster1_name
                assert len(cluster.workers) >= 3
                assert len(cluster.active_workers()) == 2
                break
            except AssertionError:
                if retry < 9:
                    await asyncio.sleep(0.5)
                else:
                    raise
        # Check that cluster is available and everything still works
        async with g.gateway_client() as gateway:
            async with gateway.connect(
                cluster1_name, shutdown_on_close=True
            ) as cluster:
                async with cluster.get_client(set_as_default=False) as client:
                    res = await client.submit(lambda x: x + 1, 1)
                    assert res == 2


@pytest.mark.asyncio
async def test_scaling():
    async with temp_gateway() as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                await cluster.scale(5)
                await wait_for_workers(cluster, atleast=3)

                await cluster.scale(1)
                await wait_for_workers(cluster, exact=1)


@pytest.mark.asyncio
async def test_adaptive_scaling():
    # XXX: we should be able to use `InProcessClusterManager` here, but due to
    # https://github.com/dask/distributed/issues/3251 this results in periodic
    # failures.
    config = Config()
    config.DaskGateway.backend_class = LocalTestingBackend
    config.ClusterConfig.adaptive_period = 0.25
    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                # Turn on adaptive scaling
                await cluster.adapt()

                # Worker is automatically requested
                async with cluster.get_client(set_as_default=False) as client:
                    res = await client.submit(lambda x: x + 1, 1)
                    assert res == 2

                # Scales down automatically
                await wait_for_workers(cluster, exact=0)

                # Turn off adaptive scaling implicitly
                await cluster.scale(1)
                await wait_for_workers(cluster, exact=1)

                await cluster.adapt()
                await wait_for_workers(cluster, exact=0)

                # Turn off adaptive scaling explicitly
                await cluster.adapt(active=False)


def test_cluster_config_resource_limits():
    # Default no limit
    config = ClusterConfig()
    assert config.cluster_max_workers is None

    # Worker limit inferred from cores limit
    config = ClusterConfig(cluster_max_cores=6, worker_cores=2, scheduler_cores=1)
    assert config.cluster_max_workers == 2

    # Worker limit inferred from memory limit
    config = ClusterConfig(
        cluster_max_memory="6G", worker_memory="2G", scheduler_memory="1G"
    )
    assert config.cluster_max_workers == 2

    common = dict(
        worker_cores=2,
        scheduler_cores=1,
        cluster_max_memory="6G",
        worker_memory="2G",
        scheduler_memory="1G",
    )

    # Worker limit is min of cores, memory, explicit count
    config = ClusterConfig(cluster_max_cores=10, **common)
    assert config.cluster_max_workers == 2

    config = ClusterConfig(cluster_max_cores=4, **common)
    assert config.cluster_max_workers == 1

    config = ClusterConfig(cluster_max_workers=10, cluster_max_cores=6, **common)
    assert config.cluster_max_workers == 2

    config = ClusterConfig(cluster_max_workers=1, cluster_max_cores=6, **common)
    assert config.cluster_max_workers == 1

    # Explicit None doesn't break validators
    config = ClusterConfig(cluster_max_workers=None)
    assert config.cluster_max_workers is None

    config = ClusterConfig(cluster_max_workers=None, cluster_max_cores=6, **common)
    assert config.cluster_max_workers == 2


@pytest.mark.parametrize(
    "opts",
    [
        dict(cluster_max_cores=0.1),
        dict(cluster_max_memory="0.5G"),
        dict(cluster_max_cores=1, scheduler_cores=2),
        dict(cluster_max_memory="1G", scheduler_memory="2G"),
    ],
)
def test_cluster_config_resource_limits_less_than_scheduler_usage(opts):
    with pytest.raises(ValueError, match="Scheduler (cores|memory) request"):
        ClusterConfig(**opts)


@pytest.mark.asyncio
async def test_cluster_resource_limits():
    config = Config()
    config.ClusterConfig.cluster_max_workers = 2
    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:
                # Limits on scale count
                with pytest.warns(GatewayWarning, match="Scaling to 2 instead"):
                    await cluster.scale(3)

                # Limits on adapt maximum
                with pytest.warns(
                    GatewayWarning, match="Using `maximum=2, minimum=0` instead"
                ):
                    await cluster.adapt(maximum=3)

                # Limits on adapt minimum
                with pytest.warns(
                    GatewayWarning, match="Using `maximum=2, minimum=2` instead"
                ):
                    await cluster.adapt(minimum=3, maximum=4)


@pytest.mark.asyncio
async def test_idle_timeout():
    config = Config()
    config.ClusterConfig.idle_timeout = 2
    async with temp_gateway(config=config) as g:
        async with g.gateway_client() as gateway:
            # Start a cluster
            cluster = await gateway.new_cluster()
            # Add some workers
            await cluster.scale(2)

            # Schedule some work that takes enough time that the idle check
            # will loop at least once
            async with cluster.get_client(set_as_default=False) as client:
                await client.submit(time.sleep, 1)

            waited = 0
            while await gateway.list_clusters():
                await asyncio.sleep(0.25)
                waited += 0.25
                if waited >= 5:
                    assert False, "Failed to automatically shutdown in time"

            # Calling shutdown doesn't break anything
            await cluster.shutdown()
