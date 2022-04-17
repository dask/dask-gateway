import asyncio
import json
import os
import pwd
import shutil

from traitlets import Unicode, default

from ..base import ClusterConfig
from ..db_base import DBBackendBase

__all__ = ("JobQueueClusterConfig", "JobQueueBackend")


class JobQueueClusterConfig(ClusterConfig):
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

        This field can be a template, which receives the following fields:

        - home (the user's home directory)
        - username (the user's name)
        """,
        config=True,
    )


class JobQueueBackend(DBBackendBase):
    """A base cluster manager for deploying Dask on a jobqueue cluster."""

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

    cancel_command = Unicode(help="The path to the job cancel command", config=True)

    status_command = Unicode(help="The path to the job status command", config=True)

    def get_submit_cmd_env_stdin(self, cluster, worker=None):
        raise NotImplementedError

    def get_stop_cmd_env(self, job_id):
        raise NotImplementedError

    def get_status_cmd_env(self, job_ids):
        raise NotImplementedError

    def parse_job_id(self, stdout):
        raise NotImplementedError

    def parse_job_states(self, stdout):
        raise NotImplementedError

    def get_staging_directory(self, cluster):
        staging_dir = cluster.config.staging_directory.format(
            home=pwd.getpwnam(cluster.username).pw_dir, username=cluster.username
        )
        return os.path.join(staging_dir, cluster.name)

    def get_tls_paths(self, cluster):
        """Get the absolute paths to the tls cert and key files."""
        staging_dir = self.get_staging_directory(cluster)
        cert_path = os.path.join(staging_dir, "dask.crt")
        key_path = os.path.join(staging_dir, "dask.pem")
        return cert_path, key_path

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

    async def start_job(self, username, cmd, env, stdin, staging_dir=None, files=None):
        code, stdout, stderr = await self.do_as_user(
            user=username,
            action="start",
            cmd=cmd,
            env=env,
            stdin=stdin,
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
        return self.parse_job_id(stdout)

    async def stop_job(self, username, job_id, staging_dir=None):
        cmd, env = self.get_stop_cmd_env(job_id)

        code, stdout, stderr = await self.do_as_user(
            user=username, action="stop", cmd=cmd, env=env, staging_dir=staging_dir
        )
        if code != 0 and "Job has finished" not in stderr:
            raise Exception("Failed to stop job_id %s" % job_id)

    async def check_jobs(self, job_ids):
        if not job_ids:
            return {}
        self.log.debug("Checking status of %d jobs", len(job_ids))
        cmd, env = self.get_status_cmd_env(job_ids)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
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
            raise Exception("Job status check failed")
        return self.parse_job_states(stdout)

    async def do_start_cluster(self, cluster):
        cmd, env, stdin = self.get_submit_cmd_env_stdin(cluster)
        staging_dir = self.get_staging_directory(cluster)
        files = {
            "dask.pem": cluster.tls_key.decode("utf8"),
            "dask.crt": cluster.tls_cert.decode("utf8"),
        }
        job_id = await self.start_job(
            cluster.username, cmd, env, stdin, staging_dir=staging_dir, files=files
        )
        self.log.info("Job %s submitted for cluster %s", job_id, cluster.name)
        yield {"job_id": job_id, "staging_dir": staging_dir}

    async def do_stop_cluster(self, cluster):
        job_id = cluster.state.get("job_id")
        if job_id is not None:
            staging_dir = cluster.state["staging_dir"]
            await self.stop_job(cluster.username, job_id, staging_dir=staging_dir)

    async def do_start_worker(self, worker):
        cmd, env, stdin = self.get_submit_cmd_env_stdin(worker.cluster, worker)
        job_id = await self.start_job(worker.cluster.username, cmd, env, stdin)
        self.log.info("Job %s submitted for worker %s", job_id, worker.name)
        yield {"job_id": job_id}

    async def do_stop_worker(self, worker):
        job_id = worker.state.get("job_id")
        if job_id is not None:
            await self.stop_job(worker.cluster.username, job_id)

    async def _do_check(self, objs):
        id_map = {}
        for x in objs:
            job_id = x.state.get("job_id")
            if job_id is not None:
                id_map[x.name] = job_id
        states = await self.check_jobs(list(id_map.values()))
        return [states.get(id_map.get(x.name), False) for x in objs]

    async def do_check_clusters(self, clusters):
        return await self._do_check(clusters)

    async def do_check_workers(self, workers):
        return await self._do_check(workers)
