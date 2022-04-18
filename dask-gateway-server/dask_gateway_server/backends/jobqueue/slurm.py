import math
import os
import shutil

from traitlets import Unicode, default

from ...traitlets import Type
from .base import JobQueueBackend, JobQueueClusterConfig

__all__ = ("SlurmBackend", "SlurmClusterConfig")


def slurm_format_memory(n):
    """Format memory in bytes for use with slurm."""
    if n >= 10 * (1024**3):
        return "%dG" % math.ceil(n / (1024**3))
    if n >= 10 * (1024**2):
        return "%dM" % math.ceil(n / (1024**2))
    if n >= 10 * 1024:
        return "%dK" % math.ceil(n / 1024)
    return "1K"


class SlurmClusterConfig(JobQueueClusterConfig):
    """Dask cluster configuration options when running on SLURM"""

    partition = Unicode("", help="The partition to submit jobs to.", config=True)

    qos = Unicode("", help="QOS string associated with each job.", config=True)

    account = Unicode("", help="Account string associated with each job.", config=True)


class SlurmBackend(JobQueueBackend):
    """A backend for deploying Dask on a Slurm cluster."""

    cluster_config_class = Type(
        "dask_gateway_server.backends.jobqueue.slurm.SlurmClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("sbatch") or "sbatch"

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("scancel") or "scancel"

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("squeue") or "squeue"

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        cmd = [self.submit_command, "--parsable"]
        cmd.append("--job-name=dask-gateway")
        if cluster.config.partition:
            cmd.append("--partition=" + cluster.config.partition)
        if cluster.config.account:
            cmd.account("--account=" + cluster.config.account)
        if cluster.config.qos:
            cmd.extend("--qos=" + cluster.config.qos)

        if worker:
            cpus = cluster.config.worker_cores
            mem = slurm_format_memory(cluster.config.worker_memory)
            log_file = "dask-worker-%s.log" % worker.name
            script = "\n".join(
                [
                    "#!/bin/sh",
                    cluster.config.worker_setup,
                    " ".join(self.get_worker_command(cluster, worker.name)),
                ]
            )
            env = self.get_worker_env(cluster)
        else:
            cpus = cluster.config.scheduler_cores
            mem = slurm_format_memory(cluster.config.scheduler_memory)
            log_file = "dask-scheduler-%s.log" % cluster.name
            script = "\n".join(
                [
                    "#!/bin/sh",
                    cluster.config.scheduler_setup,
                    " ".join(self.get_scheduler_command(cluster)),
                ]
            )
            env = self.get_scheduler_env(cluster)

        staging_dir = self.get_staging_directory(cluster)

        cmd.extend(
            [
                "--chdir=" + staging_dir,
                "--output=" + os.path.join(staging_dir, log_file),
                "--cpus-per-task=%d" % cpus,
                "--mem=%s" % mem,
                "--export=%s" % (",".join(sorted(env))),
            ]
        )

        return cmd, env, script

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    def get_status_cmd_env(self, job_ids):
        cmd = [self.status_command, "-h", "--job=%s" % ",".join(job_ids), "-o", "%i %t"]
        return cmd, {}

    def parse_job_states(self, stdout):
        states = {}
        for l in stdout.splitlines():
            job_id, state = l.split()
            states[job_id] = state in ("R", "CG", "PD", "CF")
        return states

    def parse_job_id(self, stdout):
        return stdout.strip()
