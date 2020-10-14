import math
import os
import shutil
import asyncio
import json

from traitlets import Unicode, Integer, default

from .base import JobQueueBackend, JobQueueClusterConfig
from ...traitlets import Type, Command


__all__ = ("LSFBackend", "LSFClusterConfig")


class LSFClusterConfig(JobQueueClusterConfig):
    """Dask cluster configuration options when running on SLURM"""

    worker_cmd = Command(
        "dask-worker", help="""Shell command to start a dask worker.
        The ``{nnodes}`` format string is expanded to the number of nodes.
        """, config=True
    )

    worker_queue = Unicode("", help="The queue to submit worker jobs to.", config=True)
    scheduler_queue = Unicode("", help="The queue to submit scheduler jobs to.", config=True)

    wall_time = Unicode("", help="The job duration in hours:minutes.", config=True)
    worker_nodes = Integer(1, help="The number of nodes the worker runs on.", config=True)
    scheduler_nodes = Integer(1, help="The number of nodes the scheduler runs on.", config=True)

    alloc_flags = Unicode("", help="allocation flags (BSUB -alloc_flags) associated with each job.", config=True)

    account = Unicode("", help="Account string associated with each job.", config=True)


class LSFBackend(JobQueueBackend):
    """A backend for deploying Dask on an LSF cluster."""

    # override the sudo command
    async def do_as_user(self, user, action, **kwargs):
        cmd = [self.dask_gateway_jobqueue_launcher]
        kwargs["action"] = action
        proc = await asyncio.create_subprocess_exec(
            *cmd,
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


    cluster_config_class = Type(
        "dask_gateway_server.backends.jobqueue.lsf.LSFClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    @default("submit_command")
    def _default_submit_command(self):
        return shutil.which("bsub") or "bsub"

    @default("cancel_command")
    def _default_cancel_command(self):
        return shutil.which("bkill") or "bkill"

    @default("status_command")
    def _default_status_command(self):
        return shutil.which("bjobs") or "bjobs"

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        cmd = [self.submit_command]

        script = ['#!/bin/sh']

        script.append("#BSUB -J dask-gateway")
        if cluster.config.account:
            cmd.append("-P " + cluster.config.account)
        if cluster.config.alloc_flags:
            script.append("#BSUB -alloc_flags " + cluster.config.alloc_flags)

        if cluster.config.wall_time:
            cmd.append("-W " + cluster.config.wall_time)

        staging_dir = self.get_staging_directory(cluster)

        if worker:
            nodes = cluster.config.worker_nodes
            if cluster.config.worker_queue:
                script.append("#BSUB -q " + cluster.config.worker_queue)

            stdout_file = "dask-worker-%s.out" % worker.name
            stderr_file = "dask-worker-%s.err" % worker.name
            env = self.get_worker_env(cluster)
        else:
            nodes = cluster.config.scheduler_nodes
            if cluster.config.scheduler_queue:
                script.append("#BSUB -q " + cluster.config.scheduler_queue)

            stdout_file = "dask-scheduler-%s.out" % cluster.name
            stderr_file = "dask-scheduler-%s.err" % cluster.name
            env = self.get_scheduler_env(cluster)

        script.append('#BSUB -o {}'.format(os.path.join(staging_dir, stdout_file)))
        script.append('#BSUB -e {}'.format(os.path.join(staging_dir, stderr_file)))
        script.append('')

        for var in env:
            script.append('export {}={}'.format(var,env[var]))

        cmd.append('-nnodes {}'.format(nodes))

        script.append('cd {}'.format(staging_dir))

        if worker:
            script.append(cluster.config.worker_setup)
            script.append(' '.join(
                    self.get_worker_command(
                        cluster,
                        worker.name
                    )
                ).format(nnodes=nodes)
            )
        else:
            script.append(cluster.config.scheduler_setup)
            script.append(' '.join(self.get_scheduler_command(cluster)))

        script = '\n'.join(script)
        return cmd, env, script

    def get_stop_cmd_env(self, job_id):
        return [self.cancel_command, job_id], {}

    def get_status_cmd_env(self, job_ids):
        cmd = [self.status_command, "-json", " %s" % " ".join(job_ids)]
        return cmd, {}

    def parse_job_states(self, stdout):
        result = json.loads(stdout)
        states = {}
        for record in result['RECORDS']:
            job_id, state = record['JOBID'], record['STAT']
            states[job_id] = state in ("RUN", "PEND", "WAIT")
        return states

    def parse_job_id(self, stdout):
        return stdout.split('<')[1].split('>')[0]
