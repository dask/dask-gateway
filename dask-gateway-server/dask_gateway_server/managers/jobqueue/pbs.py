import asyncio
import json
import math
import os
import pwd
import shutil
import socket
from weakref import WeakValueDictionary

from traitlets import Float, Unicode, Bool, default

from ..base import ClusterManager


__all__ = ("PBSClusterManager",)


def qsub_format_memory(n):
    """Format memory in bytes for use in qsub resources."""
    if n >= 10 * (1024 ** 3):
        return "%dGB" % math.ceil(n / (1024 ** 3))
    if n >= 10 * (1024 ** 2):
        return "%dMB" % math.ceil(n / (1024 ** 2))
    if n >= 10 * 1024:
        return "%dkB" % math.ceil(n / 1024)
    return "%dB" % n


class PBSClusterManager(ClusterManager):
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

    worker_setup = Unicode(
        "", help="Script to run before dask worker starts.", config=True
    )

    scheduler_setup = Unicode(
        "", help="Script to run before dask scheduler starts.", config=True
    )

    staging_directory = Unicode(
        "{home}/.dask-gateway/",
        help="""
        The staging directory for storing files before the job starts.

        A subdirectory will be created for each new cluster which will store
        temporary files for that cluster. On cluster shutdown the subdirectory
        will be removed.

        This field can be a template, which recieves the following fields:

        - home (the user's home directory)
        - username (the user's name)
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

    status_poll_interval = Float(
        0.5,
        help="The interval (in seconds) in which to poll for job statuses.",
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

    dask_gateway_jobqueue_launcher = Unicode(
        help="The path to the dask-gateway-jobqueue-launcher executable", config=True
    )

    @default("dask_gateway_jobqueue_launcher")
    def _default_launcher_path(self):
        return (
            shutil.which("dask-gateway-jobqueue-launcher")
            or "dask-gateway-jobqueue-launcher"
        )

    submit_command = Unicode(help="The path to the job submit command", config=True)

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("qsub") or "qsub"

    cancel_command = Unicode(help="The path to the job cancel command", config=True)

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("qdel") or "qdel"

    status_command = Unicode(help="The path to the job status command", config=True)

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("qstat") or "qstat"

    def get_worker_args(self):
        return [
            "--nthreads",
            str(self.worker_cores),
            "--memory-limit",
            str(self.worker_memory),
        ]

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return " ".join([self.worker_cmd] + self.get_worker_args())

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return self.scheduler_cmd

    def format_resource_list(self, template, cores, memory):
        return template.format(cores=cores, memory=qsub_format_memory(memory))

    def get_staging_directory(self, cluster_info):
        staging_dir = self.staging_directory.format(
            home=pwd.getpwnam(cluster_info.username).pw_dir,
            username=cluster_info.username,
        )
        return os.path.join(staging_dir, cluster_info.cluster_name)

    def get_tls_paths(self, cluster_info):
        """Get the absolute paths to the tls cert and key files."""
        if self.use_stagein:
            cert_path = "dask.crt"
            key_path = "dask.pem"
        else:
            staging_dir = self.get_staging_directory(cluster_info)
            cert_path = os.path.join(staging_dir, "dask.crt")
            key_path = os.path.join(staging_dir, "dask.pem")
        return cert_path, key_path

    def get_submit_cmd_and_env(self, cluster_info, worker_name=None):
        env = self.get_env(cluster_info)

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
            staging_dir = self.get_staging_directory(cluster_info)
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

        return cmd, env

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    async def do_as_user(self, user, action, **kwargs):
        cmd = ["sudo", "-nHu", user, self.dask_gateway_jobqueue_launcher]
        kwargs["action"] = action
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env={},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(json.dumps(kwargs).encode("utf8"))
        stdout = stdout.decode("utf8", "replace")
        stderr = stderr.decode("utf8", "replace")

        if proc.returncode != 0:
            raise Exception(
                "Error running `dask-gateway-jobqueue-launcher`\n"
                "  returncode: %d\n"
                "  stdout: %s\n"
                "  stderr: %s" % (proc.returncode, stdout, stderr)
            )
        result = json.loads(stdout)
        if not result["ok"]:
            raise Exception(result["error"])
        return result["returncode"], result["stdout"], result["stderr"]

    async def start_job(self, cluster_info, worker_name=None):
        cmd, env = self.get_submit_cmd_and_env(cluster_info, worker_name=worker_name)
        if not worker_name:
            staging_dir = self.get_staging_directory(cluster_info)
            files = {
                "dask.pem": cluster_info.tls_key.decode("utf8"),
                "dask.crt": cluster_info.tls_cert.decode("utf8"),
            }
        else:
            staging_dir = files = None

        code, stdout, stderr = await self.do_as_user(
            user=cluster_info.username,
            action="start",
            cmd=cmd,
            env=env,
            staging_dir=staging_dir,
            files=files,
        )
        if code != 0:
            raise Exception(
                (
                    "Failed to submit job to batch system\n"
                    "  exit_code: %d\n"
                    "  stdout: %s\n"
                    "  stderr: %s"
                )
                % (code, stdout, stderr)
            )
        return stdout.strip()

    async def stop_job(self, cluster_info, job_id, worker_name=None):
        cmd, env = self.get_stop_cmd_env(job_id)

        if not worker_name:
            staging_dir = self.get_staging_directory(cluster_info)
        else:
            staging_dir = None

        code, stdout, stderr = await self.do_as_user(
            user=cluster_info.username,
            action="stop",
            cmd=cmd,
            env=env,
            staging_dir=staging_dir,
        )
        if code != 0 and "Job has finished" not in stderr:
            raise Exception(
                "Failed to stop job_id %s" % (job_id, cluster_info.cluster_name)
            )

    async def job_status_tracker(self):
        while True:
            if self.jobs_to_track:
                self.log.debug("Polling status of %d jobs", len(self.jobs_to_track))
                proc = await asyncio.create_subprocess_exec(
                    self.status_command,
                    "-x",
                    *self.jobs_to_track,
                    env={},
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                stdout = stdout.decode("utf8", "replace")
                if proc.returncode != 0:
                    stderr = stderr.decode("utf8", "replace")
                    self.log.warning(
                        "Job status check failed with returncode %d, stderr: %s",
                        proc.returncode,
                        stderr,
                    )

                lines = stdout.splitlines()[2:]

                for l in lines:
                    parts = l.split()
                    job_id = parts[0]
                    status = parts[4]
                    if status == "R":
                        fut = self.jobs_to_track.pop(job_id, None)
                        if fut:
                            fut.set_result(True)
                    elif status not in ("Q", "H"):
                        fut = self.jobs_to_track.pop(job_id, None)
                        if fut:
                            fut.set_result(False)

            await asyncio.sleep(self.status_poll_interval)

    def is_job_running(self, job_id):
        if not hasattr(self, "job_tracker"):
            self.jobs_to_track = WeakValueDictionary()
            self.job_tracker = self.task_pool.create_background_task(
                self.job_status_tracker()
            )

        if job_id not in self.jobs_to_track:
            loop = asyncio.get_running_loop()
            fut = self.jobs_to_track[job_id] = loop.create_future()
        else:
            fut = self.jobs_to_track[job_id]
        return fut

    async def start_cluster(self, cluster_info):
        job_id = await self.start_job(cluster_info)
        yield {"job_id": job_id}
        await self.is_job_running(job_id)

    async def stop_cluster(self, cluster_info, cluster_state):
        job_id = cluster_state.get("job_id")
        if job_id is None:
            return
        await self.stop_job(cluster_info, job_id)

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        job_id = await self.start_job(cluster_info, worker_name=worker_name)
        yield {"job_id": job_id}
        await self.is_job_running(job_id)

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        job_id = worker_state.get("job_id")
        if job_id is None:
            return
        await self.stop_job(cluster_info, job_id, worker_name=worker_name)
