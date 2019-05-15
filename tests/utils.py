import asyncio
import json
import uuid

import pytest

from tornado import web

from dask_gateway_server.utils import random_port
from dask_gateway_server.tls import new_keypair
from dask_gateway_server.objects import (
    Cluster,
    User,
    Worker,
    ClusterStatus,
    WorkerStatus,
)


class MockGateway(object):
    def __init__(self):
        self.clusters = {}

    def new_cluster(self):
        cluster_name = uuid.uuid4().hex
        cert, key = new_keypair(cluster_name)
        cluster = Cluster(
            name=cluster_name,
            user=User(name="testuser"),
            token=uuid.uuid4().hex,
            tls_cert=cert,
            tls_key=key,
            state={},
            status=ClusterStatus.STARTING,
        )
        self.clusters[cluster_name] = cluster
        return cluster

    def get_cluster(self, cluster_name):
        return self.clusters[cluster_name]

    def mark_cluster_started(self, cluster_name, json_data):
        cluster = self.clusters[cluster_name]
        for k in ["scheduler_address", "dashboard_address", "api_address"]:
            setattr(cluster, k, json_data[k])
        cluster._connect_future.set_result(True)

    def mark_cluster_stopped(self, cluster_name):
        del self.clusters[cluster_name]

    def new_worker(self, cluster_name):
        cluster = self.get_cluster(cluster_name)
        worker = Worker(
            name=uuid.uuid4().hex,
            cluster=cluster,
            state={},
            status=WorkerStatus.STARTING,
        )
        cluster.workers[worker.name] = worker
        return worker

    def mark_worker_started(self, cluster_name, worker_name):
        cluster = self.get_cluster(cluster_name)
        worker = cluster.workers[worker_name]
        worker._connect_future.set_result(True)

    def mark_worker_stopped(self, cluster_name, worker_name):
        cluster = self.get_cluster(cluster_name)
        cluster.workers.pop(worker_name)


class BaseHandler(web.RequestHandler):
    @property
    def gateway(self):
        return self.settings["gateway"]

    def prepare(self):
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json_data = json.loads(self.request.body)
        else:
            self.json_data = None

    def assert_token_matches(self, cluster_name):
        cluster = self.gateway.get_cluster(cluster_name)
        auth_header = self.request.headers.get("Authorization")
        assert auth_header
        auth_type, auth_key = auth_header.split(" ", 1)
        assert auth_type == "token"
        assert auth_key == cluster.info.api_token


class ClusterRegistrationHandler(BaseHandler):
    async def put(self, cluster_name):
        self.assert_token_matches(cluster_name)
        self.gateway.mark_cluster_started(cluster_name, self.json_data)

    async def get(self, cluster_name):
        self.assert_token_matches(cluster_name)
        cluster = self.gateway.get_cluster(cluster_name)
        self.write(
            {
                "scheduler_address": cluster.scheduler_address,
                "dashboard_address": cluster.dashboard_address,
                "api_address": cluster.api_address,
            }
        )


class ClusterWorkersHandler(BaseHandler):
    async def put(self, cluster_name, worker_name):
        self.assert_token_matches(cluster_name)
        self.gateway.mark_worker_started(cluster_name, worker_name)

    async def delete(self, cluster_name, worker_name):
        self.assert_token_matches(cluster_name)
        self.gateway.mark_worker_stopped(cluster_name, worker_name)


def gateway_test(func):
    async def inner(self, tmpdir):
        port = random_port()
        api_url = "http://127.0.0.1:%d" % port
        gateway = MockGateway()
        app = web.Application(
            [
                (
                    "/clusters/([a-zA-Z0-9-_.]*)/workers/([a-zA-Z0-9-_.]*)",
                    ClusterWorkersHandler,
                ),
                ("/clusters/([a-zA-Z0-9-_.]*)/addresses", ClusterRegistrationHandler),
            ],
            gateway=gateway,
        )
        manager = self.new_manager(api_url=api_url, temp_dir=str(tmpdir))
        server = None
        try:
            server = app.listen(port, address="127.0.0.1")
            await func(self, gateway, manager)
        finally:
            if server is not None:
                server.stop()
            for cluster in gateway.clusters.values():
                worker_states = [w.state for w in cluster.workers.values()]
                await self.cleanup_cluster(manager, cluster.state, worker_states)

        # Only raise if test didn't fail earlier
        if gateway.clusters:
            assert False, "Clusters %r not fully cleaned up" % list(gateway.clusters)

    inner.__name__ = func.__name__

    return inner


class ClusterManagerTests(object):
    async def cleanup_cluster(self, manager, cluster_state, worker_states):
        raise NotImplementedError

    def new_manager(self, **kwargs):
        raise NotImplementedError

    def is_cluster_running(self, cluster_state):
        raise NotImplementedError

    def is_worker_running(self, cluster_state, worker_state):
        raise NotImplementedError

    def num_start_cluster_stages(self):
        raise NotImplementedError

    def num_start_worker_stages(self):
        raise NotImplementedError

    @pytest.mark.asyncio
    @gateway_test
    async def test_start_stop_cluster(self, gateway, manager):
        # Create a new cluster
        cluster = gateway.new_cluster()

        # Start the cluster
        async for state in manager.start_cluster(cluster.info):
            cluster.state = state

        # Wait for connection
        await asyncio.wait_for(cluster._connect_future, manager.cluster_connect_timeout)
        assert self.is_cluster_running(cluster.state)

        # Stop the cluster
        await manager.stop_cluster(cluster.info, cluster.state)
        assert not self.is_cluster_running(cluster.state)
        gateway.mark_cluster_stopped(cluster.name)

    async def check_cancel_during_cluster_startup(self, gateway, manager, fail_stage):
        # Create a new cluster
        cluster = gateway.new_cluster()

        # Start the cluster
        start_task = manager.start_cluster(cluster.info)
        if fail_stage > 0:
            async for i, state in enumerate(start_task, start=1):
                cluster.state = state
                if i == fail_stage:
                    break

        # Cleanup cancelled async generator
        await start_task.athrow(GeneratorExit)

        # Stop the cluster
        await manager.stop_cluster(cluster.info, cluster.state)
        assert not self.is_cluster_running(cluster.state)
        gateway.mark_cluster_stopped(cluster.name)

    @pytest.mark.asyncio
    @gateway_test
    async def test_cancel_during_cluster_startup(self, gateway, manager):
        for fail_stage in range(self.num_start_cluster_stages()):
            await self.check_cancel_during_cluster_startup(gateway, manager, fail_stage)

    @pytest.mark.asyncio
    @gateway_test
    async def test_start_stop_worker(self, gateway, manager):
        # Create a new cluster
        cluster = gateway.new_cluster()

        # Start the cluster
        async for state in manager.start_cluster(cluster.info):
            cluster.state = state

        # Wait for connection
        await asyncio.wait_for(cluster._connect_future, manager.cluster_connect_timeout)
        assert self.is_cluster_running(cluster.state)

        # Create a new worker
        worker = gateway.new_worker(cluster.name)

        # Start the worker
        async for state in manager.start_worker(
            worker.name, cluster.info, cluster.state
        ):
            worker.state = state

        # Wait for worker to connect
        await asyncio.wait_for(worker._connect_future, manager.worker_connect_timeout)
        assert self.is_worker_running(cluster.state, worker.state)

        # Stop the worker
        await manager.stop_worker(
            worker.name, worker.state, cluster.info, cluster.state
        )
        assert not self.is_worker_running(cluster.state, worker.state)
        gateway.mark_worker_stopped(cluster.name, worker.name)

        # Stop the cluster
        await manager.stop_cluster(cluster.info, cluster.state)
        assert not self.is_cluster_running(cluster.state)
        gateway.mark_cluster_stopped(cluster.name)

    async def check_cancel_during_worker_startup(self, gateway, manager, fail_stage):
        # Create a new cluster
        cluster = gateway.new_cluster()

        # Start the cluster
        async for state in manager.start_cluster(cluster.info):
            cluster.state = state

        # Wait for connection
        await asyncio.wait_for(cluster._connect_future, manager.cluster_connect_timeout)
        assert self.is_cluster_running(cluster.state)

        # Create a new worker
        worker = gateway.new_worker(cluster.name)

        # Start the worker
        start_task = manager.start_worker(worker.name, cluster.info, cluster.state)
        if fail_stage > 0:
            async for i, state in enumerate(start_task, start=1):
                cluster.state = state
                if i == fail_stage:
                    break

        # Cleanup cancelled async generator
        await start_task.athrow(GeneratorExit)

        # Stop the worker
        await manager.stop_worker(
            worker.name, worker.state, cluster.info, cluster.state
        )
        assert not self.is_worker_running(cluster.state, worker.state)
        gateway.mark_worker_stopped(cluster.name, worker.name)

        # Stop the cluster
        await manager.stop_cluster(cluster.info, cluster.state)
        assert not self.is_cluster_running(cluster.state)
        gateway.mark_cluster_stopped(cluster.name)

    @pytest.mark.asyncio
    @gateway_test
    async def test_cancel_during_worker_startup(self, gateway, manager):
        for fail_stage in range(self.num_start_worker_stages()):
            await self.check_cancel_during_worker_startup(gateway, manager, fail_stage)
