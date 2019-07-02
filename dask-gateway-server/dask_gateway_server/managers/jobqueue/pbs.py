import math
import shutil
import socket

from traitlets import Unicode, Bool, default

from .base import JobQueueClusterManager, JobQueueStatusTracker


__all__ = ("PBSClusterManager", "PBSStatusTracker")


def qsub_format_memory(n):
    """Format memory in bytes for use in qsub resources."""
    if n >= 10 * (1024 ** 3):
        return "%dGB" % math.ceil(n / (1024 ** 3))
    if n >= 10 * (1024 ** 2):
        return "%dMB" % math.ceil(n / (1024 ** 2))
    if n >= 10 * 1024:
        return "%dkB" % math.ceil(n / 1024)
    return "%dB" % n


class PBSStatusTracker(JobQueueStatusTracker):
    """A tracker for all PBS jobs submitted by the gateway"""

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("qstat") or "qstat"

    def get_status_cmd_env(self, job_ids):
        out = [self.status_command, "-x"]
        out.extend(job_ids)
        return out, {}

    def parse_job_states(self, stdout):
        lines = stdout.splitlines()[2:]

        finished = {}

        for l in lines:
            parts = l.split()
            job_id = parts[0]
            status = parts[4]
            if status not in ("R", "Q", "H"):
                finished[job_id] = status
        return finished


class PBSClusterManager(JobQueueClusterManager):
    """A cluster manager for deploying Dask on a PBS cluster."""

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

    # The following fields are configurable only for just-in-case reasons. The
    # defaults should be sufficient for most users.

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

    def format_resource_list(self, template, cores, memory):
        return template.format(cores=cores, memory=qsub_format_memory(memory))

    def get_tls_paths(self):
        """Get the absolute paths to the tls cert and key files."""
        if self.use_stagein:
            return "dask.crt", "dask.pem"
        else:
            return super().get_tls_paths()

    def get_submit_cmd_env_stdin(self, worker_name=None):
        env = self.get_env()

        cmd = [self.submit_command]
        cmd.extend(["-N", "dask-gateway"])
        if self.queue:
            cmd.extend(["-q", self.queue])
        if self.account:
            cmd.extend(["-A", self.account])
        if self.project:
            cmd.extend(["-P", self.project])
        cmd.extend(["-j", "eo", "-R", "eo", "-Wsandbox=PRIVATE"])

        if self.use_stagein:
            staging_dir = self.get_staging_directory()
            cmd.append("-Wstagein=.@%s:%s/*" % (self.gateway_hostname, staging_dir))

        if worker_name:
            env["DASK_GATEWAY_WORKER_NAME"] = worker_name
            resources = self.format_resource_list(
                self.worker_resource_list, self.worker_cores, self.worker_memory
            )
            script = "\n".join([self.worker_setup, self.worker_command])
        else:
            resources = self.format_resource_list(
                self.scheduler_resource_list,
                self.scheduler_cores,
                self.scheduler_memory,
            )
            script = "\n".join([self.scheduler_setup, self.scheduler_command])

        cmd.extend(["-v", ",".join(sorted(env))])
        cmd.extend(["-l", resources])
        cmd.extend(["--", "/bin/sh", "-c", script])

        return cmd, env, None

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    def parse_job_id(self, stdout):
        return stdout.strip()

    def get_status_tracker(self):
        return PBSStatusTracker.instance(
            parent=self.parent or self, task_pool=self.task_pool
        )
