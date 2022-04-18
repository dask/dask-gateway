import math
import shutil
import socket

from traitlets import Bool, Unicode, default

from ...traitlets import Type
from .base import JobQueueBackend, JobQueueClusterConfig

__all__ = ("PBSBackend", "PBSClusterConfig")


def qsub_format_memory(n):
    """Format memory in bytes for use in qsub resources."""
    if n >= 10 * (1024**3):
        return "%dGB" % math.ceil(n / (1024**3))
    if n >= 10 * (1024**2):
        return "%dMB" % math.ceil(n / (1024**2))
    if n >= 10 * 1024:
        return "%dkB" % math.ceil(n / 1024)
    return "%dB" % n


def format_resource_list(template, cores, memory):
    return template.format(cores=cores, memory=qsub_format_memory(memory))


class PBSClusterConfig(JobQueueClusterConfig):
    """Dask cluster configuration options when running on PBS"""

    queue = Unicode("", help="The queue to submit jobs to.", config=True)

    account = Unicode(
        "", help="Accounting string associated with each job.", config=True
    )

    project = Unicode("", help="Project associated with each job.", config=True)

    worker_resource_list = Unicode(
        "select=1:ncpus={cores}:mem={memory}",
        help="""
        The resource list to use for the workers.

        This is a template, and receives the following fields:

        - cores
        - memory
        """,
        config=True,
    )

    scheduler_resource_list = Unicode(
        "select=1:ncpus={cores}:mem={memory}",
        help="""
        The resource list to use for the scheduler.

        This is a template, and receives the following fields:

        - cores
        - memory
        """,
        config=True,
    )

    use_stagein = Bool(
        True,
        help="""
        If true, the staging directory created above will be copied into the
        job working directories at runtime using the ``-Wstagein`` directive.

        If the staging directory is on a networked filesystem, you can set this
        to False and rely on the networked filesystem for access.
        """,
        config=True,
    )


class PBSBackend(JobQueueBackend):
    """A backend for deploying Dask on a PBS cluster."""

    cluster_config_class = Type(
        "dask_gateway_server.backends.jobqueue.pbs.PBSClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    gateway_hostname = Unicode(
        help="""
        The hostname of the node running the gateway. Used for referencing the
        local host in PBS directives.
        """,
        config=True,
    )

    @default("gateway_hostname")
    def _default_gateway_hostname(self):
        return socket.gethostname()

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("qsub") or "qsub"

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("qdel") or "qdel"

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("qstat") or "qstat"

    def get_tls_paths(self, cluster):
        if cluster.config.use_stagein:
            return "dask.crt", "dask.pem"
        return super().get_tls_paths(cluster)

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        cmd = [self.submit_command]
        cmd.extend(["-N", "dask-gateway"])
        if cluster.config.queue:
            cmd.extend(["-q", cluster.config.queue])
        if cluster.config.account:
            cmd.extend(["-A", cluster.config.account])
        if cluster.config.project:
            cmd.extend(["-P", cluster.config.project])
        cmd.extend(["-j", "eo", "-R", "eo", "-Wsandbox=PRIVATE"])

        if cluster.config.use_stagein:
            staging_dir = self.get_staging_directory(cluster)
            cmd.append(f"-Wstagein=.@{self.gateway_hostname}:{staging_dir}/*")

        if worker:
            resources = format_resource_list(
                cluster.config.worker_resource_list,
                cluster.config.worker_cores,
                cluster.config.worker_memory,
            )
            script = "\n".join(
                [
                    cluster.config.worker_setup,
                    " ".join(self.get_worker_command(cluster, worker.name)),
                ]
            )
            env = self.get_worker_env(cluster)
        else:
            resources = format_resource_list(
                cluster.config.scheduler_resource_list,
                cluster.config.scheduler_cores,
                cluster.config.scheduler_memory,
            )
            script = "\n".join(
                [
                    cluster.config.scheduler_setup,
                    " ".join(self.get_scheduler_command(cluster)),
                ]
            )
            env = self.get_scheduler_env(cluster)

        cmd.extend(["-v", ",".join(sorted(env))])
        cmd.extend(["-l", resources])
        cmd.extend(["--", "/bin/sh", "-c", script])

        return cmd, env, None

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    def get_status_cmd_env(self, job_ids):
        out = [self.status_command, "-x"]
        out.extend(job_ids)
        return out, {}

    def parse_job_id(self, stdout):
        return stdout.strip()

    def parse_job_states(self, stdout):
        lines = stdout.splitlines()[2:]

        states = {}

        for l in lines:
            parts = l.split()
            job_id = parts[0]
            status = parts[4]
            states[job_id] = status in ("R", "Q", "H")

        return states
