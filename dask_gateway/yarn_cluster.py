import os
from contextlib import contextmanager

import skein
from tornado import gen
from traitlets import Unicode, Integer, Dict

from .cluster import ClusterManager
from .utils import MemoryLimit


class YarnClusterManager(ClusterManager):
    """A cluster manager for deploying Dask on a YARN cluster."""

    principal = Unicode(
        None,
        help='Kerberos principal for Dask Gateway user',
        allow_none=True,
        config=True,
    )

    keytab = Unicode(
        None,
        help='Path to kerberos keytab for Dask Gateway user',
        allow_none=True,
        config=True,
    )

    queue = Unicode(
        'default',
        help='The YARN queue to submit applications under',
        config=True,
    )

    localize_files = Dict(
        help="""
        Extra files to distribute to both the worker and scheduler containers.

        This is a mapping from ``local-name`` to ``resource``. Resource paths
        can be local, or in HDFS (prefix with ``hdfs://...`` if so). If an
        archive (``.tar.gz`` or ``.zip``), the resource will be unarchived as
        directory ``local-name``. For finer control, resources can also be
        specified as ``skein.File`` objects, or their ``dict`` equivalents.

        This can be used to distribute conda/virtual environments by
        configuring the following:

        .. code::

            c.YarnSpawner.localize_files = {
                'environment': {
                    'source': 'hdfs:///path/to/archived/environment.tar.gz',
                    'visibility': 'public'
                }
            }
            c.YarnSpawner.prologue = 'source environment/bin/activate'

        These archives are usually created using either ``conda-pack`` or
        ``venv-pack``. For more information on distributing files, see
        https://jcrist.github.io/skein/distributing-files.html.
        """,
        config=True,
    )

    worker_memory = MemoryLimit(
        '2 G',
        help="""
        Maximum number of bytes a dask worker is allowed to use. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True
    )

    worker_cores = Integer(
        1,
        min=1,
        help="""
        Maximum number of cpu-cores a dask worker is allowed to use.
        """,
        config=True
    )

    worker_setup = Unicode(
        '',
        help='Script to run before dask worker starts.',
        config=True,
    )

    worker_cmd = Unicode(
        'dask-gateway-worker',
        help='Shell command to start a dask-gateway worker.',
        config=True
    )

    scheduler_memory = MemoryLimit(
        '2 G',
        help="""
        Maximum number of bytes a dask scheduler is allowed to use. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes
        """,
        config=True
    )

    scheduler_cores = Integer(
        1,
        min=1,
        help="""
        Maximum number of cpu-cores a dask scheduler is allowed to use.
        """,
        config=True
    )

    scheduler_setup = Unicode(
        '',
        help='Script to run before dask scheduler starts.',
        config=True,
    )

    scheduler_cmd = Unicode(
        'dask-gateway-scheduler',
        help='Shell command to start a dask-gateway scheduler.',
        config=True
    )

    clients = {}

    async def _get_client(self):
        key = (self.principal, self.keytab)
        client = type(self).clients.get(key)
        if client is None:
            kwargs = dict(principal=self.principal,
                          keytab=self.keytab,
                          security=skein.Security.new_credentials())
            client = await gen.IOLoop.current().run_in_executor(
                None, lambda: skein.Client(**kwargs)
            )
            type(self).clients[key] = client
        return client

    def _get_security(self):
        return skein.Security(cert_bytes=self.tls_cert, key_bytes=self.tls_key)

    def _get_app_client(self):
        # TODO: maybe keep an LRU cache of these?
        return skein.ApplicationClient(self.app_address, self.app_id,
                                       security=self._get_security())

    @contextmanager
    def temp_write_credentials(self):
        """Write credentials to disk in secure temporary files.

        The files will be cleaned up upon exiting this context.

        Returns
        -------
        cert_path, key_path
        """
        prefix = os.path.join(self.temp_dir, self.cluster_name)
        cert_path = prefix + ".crt"
        key_path = prefix + ".pem"

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            for path, data in [(cert_path, self.tls_cert), (key_path, self.tls_key)]:
                with os.fdopen(os.open(path, flags, 0o600), 'wb') as fil:
                    fil.write(data)

            yield cert_path, key_path
        finally:
            for path in [cert_path, key_path]:
                if os.path.exists(path):
                    os.unlink(path)

    def get_worker_args(self):
        return ['--nthreads', '$SKEIN_RESOURCE_VCORES',
                '--memory-limit', '${SKEIN_RESOURCE_MEMORY}MiB']

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return ' '.join([self.worker_cmd] + self.get_worker_args())

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return self.scheduler_cmd

    def _build_specification(self, cert_path, key_path):
        files = {k: skein.File.from_dict(v) if isinstance(v, dict) else v
                 for k, v in self.localize_files.items()}

        files['dask.crt'] = cert_path
        files['dask.pem'] = key_path

        env = self.get_env()

        scheduler_script = '\n'.join([self.scheduler_setup, self.scheduler_command])
        worker_script = '\n'.join([self.worker_setup, self.worker_command])

        master = skein.Master(
            security=self._get_security(),
            resources=skein.Resources(
                memory='%d b' % self.scheduler_memory,
                vcores=self.scheduler_cores
            ),
            files=files,
            env=env,
            script=scheduler_script
        )

        services = {
            'dask.worker': skein.Service(
                resources=skein.Resources(
                    memory='%d b' % self.worker_memory,
                    vcores=self.worker_cores
                ),
                instances=0,
                max_restarts=0,
                allow_failures=True,
                files=files,
                env=env,
                script=worker_script
            )
        }

        return skein.ApplicationSpec(
            name='dask-gateway',
            queue=self.queue,
            user=self.username,
            master=master,
            services=services
        )

    def load_state(self, state):
        super().load_state(state)
        self.app_id = state.get('app_id', '')
        self.app_address = state.get('app_address', '')

    def get_state(self):
        state = super().get_state()
        if self.app_id:
            state['app_id'] = self.app_id
            state['app_address'] = self.app_address
        return state

    async def start(self):
        loop = gen.IOLoop.current()

        client = await self._get_client()

        with self.temp_write_credentials() as (cert_path, key_path):
            spec = self._build_specification(cert_path, key_path)
            self.app_id = await loop.run_in_executor(None, client.submit, spec)

        # Wait for application to start
        while True:
            report = await loop.run_in_executor(
                None, client.application_report, self.app_id
            )
            state = str(report.state)
            if state in {'FAILED', 'KILLED', 'FINISHED'}:
                raise Exception("Application %s failed to start, check "
                                "application logs for more information"
                                % self.app_id)
            elif state == 'RUNNING':
                self.app_address = '%s:%d' % (report.host, report.port)
                break
            else:
                await gen.sleep(1)

    async def is_running(self):
        if self.app_id == '':
            return False

        client = await self._get_client()
        report = await gen.IOLoop.current().run_in_executor(
            None, client.application_report, self.app_id
        )
        return report.state == 'RUNNING'

    async def stop(self):
        if self.app_id == '':
            return

        client = await self._get_client()
        await gen.IOLoop.current().run_in_executor(
            None, client.kill_application, self.app_id
        )

    def _add_worker(self, worker_name):
        app = self._get_app_client()
        container = app.add_container(
            'dask.worker',
            env={'DASK_GATEWAY_WORKER_NAME': worker_name}
        )
        return {'container_id': container.id}

    async def add_worker(self, worker_name):
        return await gen.IOLoop.current().run_in_executor(
            None, self._add_worker, worker_name
        )

    def _remove_worker(self, worker_name, state):
        app = self._get_app_client()
        try:
            app.kill_container(state['container_id'])
        except ValueError:
            pass

    async def remove_worker(self, worker_name, state):
        return await gen.IOLoop.current().run_in_executor(
            None, self._remove_worker, worker_name, state
        )
