import os
import subprocess

import pytest

if not os.environ.get("TEST_DASK_GATEWAY_SLURM"):
    pytest.skip("Not running Slurm tests", allow_module_level=True)

from dask_gateway_server.managers.jobqueue.slurm import (
    SlurmClusterManager,
    slurm_format_memory,
)

from .utils import ClusterManagerTests


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


class SlurmTestingClusterManager(SlurmClusterManager):
    async def start_cluster(self, *args, **kwargs):
        async for state in super().start_cluster(*args, **kwargs):
            JOBIDS.add(state["job_id"])
            yield state

    async def stop_cluster(self, cluster_info, cluster_state):
        job_id = cluster_state.get("job_id")
        await super().stop_cluster(cluster_info, cluster_state)
        JOBIDS.discard(job_id)


class TestSlurmClusterManager(ClusterManagerTests):
    async def cleanup_cluster(
        self, manager, cluster_info, cluster_state, worker_states
    ):
        job_id = cluster_state.get("job_id")
        if job_id:
            kill_job(job_id)

    def new_manager(self, **kwargs):
        return SlurmTestingClusterManager(
            scheduler_cmd="/opt/miniconda/bin/dask-gateway-scheduler",
            worker_cmd="/opt/miniconda/bin/dask-gateway-worker",
            scheduler_memory="512M",
            worker_memory="512M",
            scheduler_cores=1,
            worker_cores=1,
            cluster_start_timeout=30,
            job_status_period=0.5,
            **kwargs,
        )

    def cluster_is_running(self, manager, cluster_info, cluster_state):
        job_id = cluster_state.get("job_id")
        if not job_id:
            return False
        return is_job_running(job_id)

    def worker_is_running(self, manager, cluster_info, cluster_state, worker_state):
        job_id = worker_state.get("job_id")
        if not job_id:
            return False
        return is_job_running(job_id)

    def num_start_cluster_stages(self):
        return 1

    def num_start_worker_stages(self):
        return 1


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
