import pytest

from .utils_test import (
    LocalTestingBackend,
    temp_gateway,
    wait_for_workers,
    with_retries,
)


@pytest.mark.asyncio
async def test_local_cluster_backend():
    async with temp_gateway(backend_class=LocalTestingBackend) as g:
        async with g.gateway_client() as gateway:
            async with gateway.new_cluster() as cluster:

                db_cluster = g.gateway.backend.db.get_cluster(cluster.name)

                res = await g.gateway.backend.do_check_clusters([db_cluster])
                assert res == [True]

                await cluster.scale(3)
                await wait_for_workers(cluster, exact=3)
                await cluster.scale(1)
                await wait_for_workers(cluster, exact=1)

                db_workers = list(db_cluster.workers.values())

                async def test():
                    res = await g.gateway.backend.do_check_workers(db_workers)
                    assert sum(res) == 1

                await with_retries(test, 20)

                async with cluster.get_client(set_as_default=False) as client:
                    res = await client.submit(lambda x: x + 1, 1)
                    assert res == 2

                await cluster.scale(0)
                await wait_for_workers(cluster, exact=0)

                async def test():
                    res = await g.gateway.backend.do_check_workers(db_workers)
                    assert sum(res) == 0

                await with_retries(test, 20)

                # No-op for shutdown of already shutdown worker
                db_worker = db_workers[0]
                res = await g.gateway.backend.do_stop_worker(db_worker)

            async def test():
                res = await g.gateway.backend.do_check_clusters([db_cluster])
                assert res == [False]

            await with_retries(test, 20)
