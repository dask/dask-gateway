import asyncio
import errno
import os
import pwd
import shutil
import signal
import sys

from traitlets import Unicode, Integer, default

from .cluster import ClusterManager


__all__ = ('LocalClusterManager', 'UnsafeLocalClusterManager')


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
    if pid != 0:
        return _signal(pid, 0)
    return False


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
        config=True
    )

    sigterm_timeout = Integer(
        5,
        help="""
        Seconds to wait for process to stop after SIGTERM.

        If the process has not stopped after this time, a SIGKILL is sent.
        """,
        config=True
    )

    sigkill_timeout = Integer(
        5,
        help="""
        Seconds to wait for process to stop after SIGKILL.

        If the process has not stopped after this time, a warning is logged and
        the process is deemed a zombie process.
        """,
        config=True
    )

    clusters_directory = Unicode(
        help="""
        The base directory for cluster working directories.

        A subdirectory will be created for each new cluster which will serve as
        the working directory for that cluster. On cluster shutdown the
        subdirectory will be removed.
        """,
        config=True
    )

    pid = Integer(0, help="The pid of the scheduler process")

    @default("clusters_directory")
    def _default_clusters_directory(self):
        return os.path.join(self.temp_dir, "clusters")

    @property
    def working_directory(self):
        return os.path.join(self.clusters_directory, self.cluster_name)

    @property
    def certs_directory(self):
        return os.path.join(self.working_directory, ".certs")

    @property
    def logs_directory(self):
        return os.path.join(self.working_directory, "logs")

    def load_state(self, state):
        super().load_state(state)
        if 'pid' in state:
            self.pid = state['pid']

    def get_state(self):
        state = super().get_state()
        if self.pid:
            state['pid'] = self.pid
        return state

    def get_env(self):
        env = super().get_env()
        env['USER'] = self.username
        env['HOME'] = self.working_directory
        return env

    def create_working_directory(self):
        user = pwd.getpwnam(self.username)
        uid = user.pw_uid
        gid = user.pw_gid

        for path in [self.working_directory,
                     self.certs_directory,
                     self.logs_directory]:
            os.makedirs(path, 0o700, exist_ok=True)
            os.chown(path, uid, gid)

        cert_path, key_path = self.get_tls_paths()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
            with os.fdopen(os.open(path, flags, 0o600), 'wb') as fil:
                fil.write(data)
            os.chown(path, uid, gid)

    def remove_working_directory(self):
        try:
            shutil.rmtree(self.working_directory)
        except Exception:
            self.log.warn("Failed to remove working directory %r",
                          self.working_directory)

    def get_tls_paths(self):
        """Get the absolute paths to the tls cert and key files."""
        cert_path = os.path.join(self.certs_directory, "dask.crt")
        key_path = os.path.join(self.certs_directory, "dask.pem")
        return cert_path, key_path

    def make_preexec_fn(self):
        # Borrowed from jupyterhub/spawner.py
        import grp
        import pwd

        user = pwd.getpwnam(self.username)
        uid = user.pw_uid
        gid = user.pw_gid
        groups = [g.gr_gid for g in grp.getgrall() if self.username in g.gr_mem]
        workdir = self.working_directory

        def preexec():
            os.setgid(gid)
            try:
                os.setgroups(groups)
            except Exception as e:
                print('Failed to set groups %s' % e, file=sys.stderr)
            os.setuid(uid)
            os.chdir(workdir)

        return preexec

    def get_worker_args(self):
        return ['--nthreads', str(self.worker_cores),
                '--memory-limit', str(self.worker_memory)]

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return ' '.join([self.worker_cmd] + self.get_worker_args())

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return self.scheduler_cmd

    async def start_process(self, cmd, env, name):
        log_path = os.path.join(self.logs_directory, name + ".log")
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
                stderr=asyncio.subprocess.STDOUT
            )
        finally:
            if fd is not None:
                os.close(fd)
        return proc.pid

    async def stop_process(self, pid):
        methods = [('SIGINT', signal.SIGINT, self.sigint_timeout),
                   ('SIGTERM', signal.SIGTERM, self.sigterm_timeout),
                   ('SIGKILL', signal.SIGKILL, self.sigkill_timeout)]

        for msg, sig, timeout in methods:
            self.log.debug("Sending %s to process %d", msg, pid)
            _signal(pid, sig)
            if await wait_is_shutdown(pid, timeout):
                return

        if is_running(pid):
            # all attempts failed, zombie process
            self.log.warn("Failed to stop process %d", pid)

    async def start(self):
        self.create_working_directory()
        self.pid = await self.start_process(
            self.scheduler_command.split(),
            self.get_env(),
            "scheduler"
        )

    async def is_running(self):
        return is_running(self.pid)

    async def stop(self):
        await self.stop_process(self.pid)
        self.remove_working_directory()

    async def add_worker(self, worker_name):
        cmd = self.worker_command.split()
        env = self.get_env()
        env['DASK_GATEWAY_WORKER_NAME'] = worker_name
        pid = await self.start_process(cmd, env, "worker-%s" % worker_name)
        return {'pid': pid}

    async def remove_worker(self, worker_name, state):
        await self.stop_process(state['pid'])


class UnsafeLocalClusterManager(LocalClusterManager):
    """A version of LocalClusterManager that doesn't set permissions.

    FOR TESTING ONLY! This provides no user separations - clusters run with the
    same level of permission as the gateway.
    """
    def make_preexec_fn(self):
        workdir = self.working_directory

        def preexec():
            os.chdir(workdir)

        return preexec

    def create_working_directory(self):
        for path in [self.working_directory,
                     self.certs_directory,
                     self.logs_directory]:
            os.makedirs(path, 0o755, exist_ok=True)

        cert_path, key_path = self.get_tls_paths()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
            with os.fdopen(os.open(path, flags, 0o755), 'wb') as fil:
                fil.write(data)
