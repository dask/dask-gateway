import os
import subprocess

import pytest

if not os.environ.get("TEST_DASK_GATEWAY_PBS"):
    pytest.skip("Not running PBS tests", allow_module_level=True)

from dask_gateway_server.managers.jobqueue.pbs import (
    PBSClusterManager,
    qsub_format_memory,
)

from .utils import ClusterManagerTests


pytestmark = pytest.mark.usefixtures("cleanup_jobs")


JOBIDS = set()


def kill_job(job_id):
    try:
        subprocess.check_output(["/opt/pbs/bin/qdel", job_id], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        if b"Job has finished" not in exc.output:
            print("Failed to stop %s, output: %s" % (job_id, exc.output.decode()))


def is_job_running(job_id):
    stdout = subprocess.check_output(["/opt/pbs/bin/qstat", "-x", job_id]).decode()
    line = stdout.splitlines()[2]
    out = line.split()[4] in ("R", "Q", "H")
    return out


@pytest.fixture(scope="module")
def cleanup_jobs():
    yield

    if not JOBIDS:
        return

    for job in JOBIDS:
        kill_job(job)

    print("-- Stopped %d lost clusters --" % len(JOBIDS))


class PBSTestingClusterManager(PBSClusterManager):
    async def start_cluster(self, *args, **kwargs):
        async for state in super().start_cluster(*args, **kwargs):
            JOBIDS.add(state["job_id"])
            yield state

    async def stop_cluster(self, cluster_info, cluster_state):
        job_id = cluster_state.get("job_id")
        await super().stop_cluster(cluster_info, cluster_state)
        JOBIDS.discard(job_id)


class TestPBSClusterManager(ClusterManagerTests):
    async def cleanup_cluster(
        self, manager, cluster_info, cluster_state, worker_states
    ):
        job_id = cluster_state.get("job_id")
        if job_id:
            kill_job(job_id)

    def new_manager(self, **kwargs):
        return PBSTestingClusterManager(
            scheduler_cmd="/opt/miniconda/bin/dask-gateway-scheduler",
            worker_cmd="/opt/miniconda/bin/dask-gateway-worker",
            scheduler_memory="512M",
            worker_memory="512M",
            scheduler_cores=1,
            worker_cores=1,
            cluster_start_timeout=30,
            job_status_period=0.5,
            use_stagein=True,
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


def test_qsub_format_memory():
    assert qsub_format_memory(2) == "2B"
    assert qsub_format_memory(2 ** 10) == "1024B"
    assert qsub_format_memory(2 ** 20) == "1024kB"
    assert qsub_format_memory(2 ** 20 + 1) == "1025kB"
    assert qsub_format_memory(2 ** 30) == "1024MB"
    assert qsub_format_memory(2 ** 30 + 1) == "1025MB"
    assert qsub_format_memory(2 ** 40) == "1024GB"
    assert qsub_format_memory(2 ** 40 + 1) == "1025GB"
