import os

import pytest
from async_timeout import timeout

import dask_gateway

from ..utils_test import wait_for_workers, with_retries

kubernetes_asyncio = pytest.importorskip("kubernetes_asyncio")

if not os.environ.get("TEST_DASK_GATEWAY_KUBE"):
    pytest.skip("Not running kubernetes tests", allow_module_level=True)


@pytest.fixture
async def gateway():
    addr = os.environ.get("TEST_DASK_GATEWAY_KUBE_ADDRESS", "http://localhost:8000")
    auth = dask_gateway.BasicAuth(username="alice@non-alpha.characters")
    async with dask_gateway.Gateway(
        address=addr, asynchronous=True, auth=auth
    ) as gateway:
        for cluster in await gateway.list_clusters():
            await gateway.stop_cluster(cluster.name)

        yield gateway

        for cluster in await gateway.list_clusters():
            await gateway.stop_cluster(cluster.name)


@pytest.fixture
async def core_client():
    kubeconfig = os.environ.get("KUBECONFIG")
    await kubernetes_asyncio.config.load_kube_config(kubeconfig)
    c = kubernetes_asyncio.client.CoreV1Api()
    yield c
    await c.api_client.rest_client.pool_manager.close()


@pytest.mark.asyncio
async def test_list_clusters(gateway):
    clusters = await gateway.list_clusters()
    assert clusters == []


@pytest.mark.asyncio
async def test_cluster_options(gateway):
    options = await gateway.cluster_options()
    assert isinstance(options, dask_gateway.Options)


@pytest.mark.asyncio
async def test_cluster_operations(gateway):
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

    await wait_for_workers(cluster, exact=2, timeout=120)

    async with timeout(15):
        async with cluster.get_client(set_as_default=False) as client:
            res = await client.submit(lambda x: x + 1, 1)
            assert res == 2

    # Scale down
    await cluster.scale(1)

    await wait_for_workers(cluster, exact=1, timeout=120)

    # Can still compute
    async with timeout(15):
        async with cluster.get_client(set_as_default=False) as client:
            res = await client.submit(lambda x: x + 1, 1)
            assert res == 2

    # Shutdown the cluster
    await cluster.shutdown()

    # No currently running clusters
    async def test():
        clusters = await gateway.list_clusters()
        assert clusters == []

    await with_retries(test, 20, wait=0.5)


@pytest.mark.asyncio
async def test_deleted_worker_recreated(gateway, core_client):
    async with gateway.new_cluster() as cluster:
        # Scale up, connect, and compute
        await cluster.scale(2)

        await wait_for_workers(cluster, exact=2, timeout=120)

        cluster_name = cluster.name.split(".")[1]
        selector = (
            f"gateway.dask.org/instance=test-dask-gateway,"
            f"gateway.dask.org/cluster={cluster_name},"
            f"app.kubernetes.io/component=dask-worker"
        )

        pods = await core_client.list_namespaced_pod("default", label_selector=selector)
        pod_names = [p.metadata.name for p in pods.items]
        assert len(pod_names) == 2
        deleted_pod = pod_names[0]
        await core_client.delete_namespaced_pod(deleted_pod, "default")

        async def test():
            pods = await core_client.list_namespaced_pod(
                "default", label_selector=selector
            )
            pods = [p.metadata.name for p in pods.items]
            # New worker pod created to replace old pod
            assert len(pods) == 2
            assert deleted_pod not in pods

        await with_retries(test, 60, wait=0.5)


@pytest.mark.asyncio
async def test_adaptive_scaling(gateway):
    async with gateway.new_cluster() as cluster:
        # Turn on adaptive scaling
        await cluster.adapt()

        # Worker is automatically requested
        async with timeout(120):
            async with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

        # Scales down automatically
        await wait_for_workers(cluster, exact=0, timeout=120)

        # Turn off adaptive scaling implicitly
        await cluster.scale(1)
        await wait_for_workers(cluster, exact=1, timeout=120)

        await cluster.adapt()
        await wait_for_workers(cluster, exact=0, timeout=120)

        # Turn off adaptive scaling explicitly
        await cluster.adapt(active=False)
