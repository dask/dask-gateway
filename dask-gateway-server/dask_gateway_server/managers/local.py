import asyncio
import errno
import os
import pwd
import shutil
import signal
import sys

from traitlets import List, Unicode, Integer, default

from .base import ClusterManager


__all__ = ("LocalClusterManager", "UnsafeLocalClusterManager")


def _signal(pid, sig):
    """Send given signal to a pid.

    Returns True if the process still exists, False otherwise."""
    try:
        os.kill(pid, sig)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False
        raise
    return True


def is_running(pid):
    return _signal(pid, 0)


async def wait_is_shutdown(pid, timeout=10):
    """Wait for a pid to shutdown, using exponential backoff"""
    pause = 0.1
    while timeout >= 0:
        if not _signal(pid, 0):
            return True
        await asyncio.sleep(pause)
        timeout -= pause
        pause *= 2
    return False


class LocalClusterManager(ClusterManager):
    """A cluster manager that launches local processes.

    Requires super-user permissions in order to run processes for the
    requesting username.
    """

    sigint_timeout = Integer(
        10,
        help="""
        Seconds to wait for process to stop after SIGINT.

        If the process has not stopped after this time, a SIGTERM is sent.
        """,
        config=True,
    )

    sigterm_timeout = Integer(
        5,
        help="""
        Seconds to wait for process to stop after SIGTERM.

        If the process has not stopped after this time, a SIGKILL is sent.
        """,
        config=True,
    )

    sigkill_timeout = Integer(
        5,
        help="""
        Seconds to wait for process to stop after SIGKILL.

        If the process has not stopped after this time, a warning is logged and
        the process is deemed a zombie process.
        """,
        config=True,
    )

    clusters_directory = Unicode(
        help="""
        The base directory for cluster working directories.

        A subdirectory will be created for each new cluster which will serve as
        the working directory for that cluster. On cluster shutdown the
        subdirectory will be removed.
        """,
        config=True,
    )

    inherited_environment = List(
        [
            "PATH",
            "PYTHONPATH",
            "CONDA_ROOT",
            "CONDA_DEFAULT_ENV",
            "VIRTUAL_ENV",
            "LANG",
            "LC_ALL",
        ],
        help="""
        Whitelist of environment variables for the scheduler and worker
        processes to inherit from the Dask-Gateway process.
        """,
        config=True,
    )

    pid = Integer(0, help="The pid of the scheduler process")

    @default("clusters_directory")
    def _default_clusters_directory(self):
        return os.path.join(self.temp_dir, "clusters")

    def get_working_directory(self):
        return os.path.join(self.clusters_directory, self.cluster_name)

    def get_certs_directory(self, workdir):
        return os.path.join(workdir, ".certs")

    def get_logs_directory(self, workdir):
        return os.path.join(workdir, "logs")

    def get_env(self):
        env = super().get_env()
        for key in self.inherited_environment:
            if key in os.environ:
                env[key] = os.environ[key]
        env["USER"] = self.username
        return env

    def create_working_directory(self):  # pragma: nocover
        user = pwd.getpwnam(self.username)
        uid = user.pw_uid
        gid = user.pw_gid

        workdir = self.get_working_directory()
        certsdir = self.get_certs_directory(workdir)
        logsdir = self.get_logs_directory(workdir)

        for path in [workdir, certsdir, logsdir]:
            os.makedirs(path, 0o700, exist_ok=True)
            os.chown(path, uid, gid)

        cert_path, key_path = self.get_tls_paths()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
            with os.fdopen(os.open(path, flags, 0o600), "wb") as fil:
                fil.write(data)
            os.chown(path, uid, gid)

    def remove_working_directory(self):
        workdir = self.get_working_directory()
        if not os.path.exists(workdir):
            return
        try:
            shutil.rmtree(workdir)
        except Exception:  # pragma: nocover
            self.log.warn("Failed to remove working directory %r", workdir)

    def get_tls_paths(self):
        """Get the absolute paths to the tls cert and key files."""
        workdir = self.get_working_directory()
        certsdir = self.get_certs_directory(workdir)
        cert_path = os.path.join(certsdir, "dask.crt")
        key_path = os.path.join(certsdir, "dask.pem")
        return cert_path, key_path

    def make_preexec_fn(self):  # pragma: nocover
        # Borrowed and modified from jupyterhub/spawner.py
        import grp
        import pwd

        user = pwd.getpwnam(self.username)
        uid = user.pw_uid
        gid = user.pw_gid
        groups = [g.gr_gid for g in grp.getgrall() if self.username in g.gr_mem]
        workdir = self.get_working_directory()

        def preexec():
            os.setgid(gid)
            try:
                os.setgroups(groups)
            except Exception as e:
                print("Failed to set groups %s" % e, file=sys.stderr)
            os.setuid(uid)
            os.chdir(workdir)

        return preexec

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

    async def start_process(self, cmd, env, name):
        workdir = self.get_working_directory()
        logsdir = self.get_logs_directory(workdir)
        log_path = os.path.join(logsdir, name + ".log")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = None
        try:
            fd = os.open(log_path, flags, 0o755)
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                preexec_fn=self.make_preexec_fn(),
                start_new_session=True,
                env=env,
                stdout=fd,
                stderr=asyncio.subprocess.STDOUT,
            )
        finally:
            if fd is not None:
                os.close(fd)
        return proc.pid

    async def stop_process(self, pid):
        methods = [
            ("SIGINT", signal.SIGINT, self.sigint_timeout),
            ("SIGTERM", signal.SIGTERM, self.sigterm_timeout),
            ("SIGKILL", signal.SIGKILL, self.sigkill_timeout),
        ]

        for msg, sig, timeout in methods:
            self.log.debug("Sending %s to process %d", msg, pid)
            _signal(pid, sig)
            if await wait_is_shutdown(pid, timeout):
                return

        if is_running(pid):
            # all attempts failed, zombie process
            self.log.warn("Failed to stop process %d", pid)

    async def start_cluster(self):
        self.create_working_directory()
        pid = await self.start_process(
            self.scheduler_command.split(), self.get_env(), "scheduler"
        )
        yield {"pid": pid}

    async def cluster_status(self, cluster_state):
        pid = cluster_state.get("pid")
        if pid is not None:
            return is_running(pid)
        return False

    async def stop_cluster(self, cluster_state):
        pid = cluster_state.get("pid")
        if pid is not None:
            await self.stop_process(pid)
        self.remove_working_directory()

    async def start_worker(self, worker_name, cluster_state):
        cmd = self.worker_command.split()
        env = self.get_env()
        env["DASK_GATEWAY_WORKER_NAME"] = worker_name
        pid = await self.start_process(cmd, env, "worker-%s" % worker_name)
        yield {"pid": pid}

    async def worker_status(self, worker_name, worker_state, cluster_state):
        pid = worker_state.get("pid")
        if pid is not None:
            return is_running(pid)
        return False

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        pid = worker_state.get("pid")
        if pid is None:
            return
        await self.stop_process(pid)


class UnsafeLocalClusterManager(LocalClusterManager):
    """A version of LocalClusterManager that doesn't set permissions.

    FOR TESTING ONLY! This provides no user separations - clusters run with the
    same level of permission as the gateway.
    """

    def make_preexec_fn(self):
        workdir = self.get_working_directory()

        def preexec():  # pragma: nocover
            os.chdir(workdir)

        return preexec

    def create_working_directory(self):
        workdir = self.get_working_directory()
        certsdir = self.get_certs_directory(workdir)
        logsdir = self.get_logs_directory(workdir)

        for path in [workdir, certsdir, logsdir]:
            os.makedirs(path, 0o700, exist_ok=True)

        cert_path, key_path = self.get_tls_paths()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
            with os.fdopen(os.open(path, flags, 0o600), "wb") as fil:
                fil.write(data)
