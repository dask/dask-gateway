import os
import subprocess

import pytest
from traitlets.config import Config

if not os.environ.get("TEST_DASK_GATEWAY_SLURM"):
    pytest.skip("Not running Slurm tests", allow_module_level=True)

from dask_gateway.auth import BasicAuth
from dask_gateway_server.backends.jobqueue.slurm import (
    SlurmBackend,
    slurm_format_memory,
)

from .utils_test import temp_gateway, wait_for_workers, with_retries


pytestmark = pytest.mark.usefixtures("cleanup_jobs")


JOBIDS = set()


def kill_job(job_id):
    try:
        subprocess.check_output(
            ["/usr/local/bin/scancel", job_id], stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as exc:
        if b"Job has finished" not in exc.output:
            print("Failed to stop %s, output: %s" % (job_id, exc.output.decode()))


def is_job_running(job_id):
    stdout = subprocess.check_output(
        ["/usr/local/bin/squeue", "-h", "-j", job_id, "-o", "%t"]
    )
    state = stdout.decode().strip()
    return state in ("PD", "CF", "R", "CG")


@pytest.fixture(scope="module")
def cleanup_jobs():
    yield

    if not JOBIDS:
        return

    for job in JOBIDS:
        kill_job(job)

    print("-- Stopped %d lost clusters --" % len(JOBIDS))


class SlurmTestingBackend(SlurmBackend):
    async def do_start_cluster(self, cluster):
        async for state in super().do_start_cluster(cluster):
            JOBIDS.add(state["job_id"])
            yield state

    async def do_stop_cluster(self, cluster):
        job_id = cluster.state.get("job_id")
        await super().do_stop_cluster(cluster)
        JOBIDS.discard(job_id)


@pytest.mark.asyncio
async def test_slurm_backend():
    c = Config()

    c.SlurmClusterConfig.scheduler_cmd = "/opt/miniconda/bin/dask-scheduler"
    c.SlurmClusterConfig.worker_cmd = "/opt/miniconda/bin/dask-worker"
    c.SlurmClusterConfig.scheduler_memory = "256M"
    c.SlurmClusterConfig.worker_memory = "256M"
    c.SlurmClusterConfig.scheduler_cores = 1
    c.SlurmClusterConfig.worker_cores = 1
    c.DaskGateway.backend_class = SlurmTestingBackend

    async with temp_gateway(config=c) as g:
        auth = BasicAuth(username="alice")
        async with g.gateway_client(auth=auth) as gateway:
            async with gateway.new_cluster() as cluster:

                db_cluster = g.gateway.backend.db.get_cluster(cluster.name)

                res = await g.gateway.backend.do_check_clusters([db_cluster])
                assert res == [True]

                await cluster.scale(2)
                await wait_for_workers(cluster, atleast=1)
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


def test_slurm_format_memory():
    # Minimum is 1 K
    assert slurm_format_memory(2) == "1K"
    assert slurm_format_memory(2 ** 10) == "1K"
    assert slurm_format_memory(2 ** 20) == "1024K"
    assert slurm_format_memory(2 ** 20 + 1) == "1025K"
    assert slurm_format_memory(2 ** 30) == "1024M"
    assert slurm_format_memory(2 ** 30 + 1) == "1025M"
    assert slurm_format_memory(2 ** 40) == "1024G"
    assert slurm_format_memory(2 ** 40 + 1) == "1025G"
