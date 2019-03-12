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

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return ' '.join(self.worker_cmd + self.get_worker_args())

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return ' '.join(self.scheduler_cmd + self.get_scheduler_args())

    def _build_specification(self):
        files = {k: skein.File.from_dict(v) if isinstance(v, dict) else v
                 for k, v in self.localize_files.items()}

        # TODO: handle credentials
        credentials = self.get_credentials()
        files['dask.crt'] = credentials['dask.crt']
        files['dask.pem'] = credentials['dask.pem']

        scheduler_script = '\n'.join([self.scheduler_setup, self.scheduler_command])
        worker_script = '\n'.join([self.worker_setup, self.worker_command])

        master = skein.Master(
            security=skein.Security.new_credentials(),
            resources=skein.Resources(
                memory='%d b' % self.scheduler_memory,
                vcores=self.scheduler_cores
            ),
            files=files,
            env=self.get_env(),
            script=scheduler_script
        )

        services = {
            'dask.worker': skein.Service(
                instances=0,
                resources=skein.Resources(
                    memory='%d b' % self.worker_memory,
                    vcores=self.worker_cores
                ),
                max_restarts=-1,
                files=files,
                env=self.get_env(),
                script=worker_script
            )
        }

        return skein.ApplicationSpec(
            name='dask-gateway',
            queue=self.queue,
            user=self.user.name,
            master=master,
            services=services
        )

    def load_state(self, state):
        super().load_state(state)
        self.app_id = state.get('app_id', '')

    def get_state(self):
        state = super().get_state()
        if self.app_id:
            state['app_id'] = self.app_id
        return state

    async def start(self):
        loop = gen.IOLoop.current()

        spec = self._build_specification()
        # Set app_id == 'PENDING' to signal `poll` that we're starting
        self.app_id = 'PENDING'

        client = await self._get_client()
        app_id = await loop.run_in_executor(None, client.submit, spec)
        self.app_id = app_id

        # Wait for application to start
        while True:
            report = await loop.run_in_executor(
                None, client.application_report, app_id
            )
            state = str(report.state)
            if state in {'FAILED', 'KILLED', 'FINISHED'}:
                raise Exception("Application %s failed to start, check "
                                "application logs for more information"
                                % app_id)
            elif state == 'RUNNING':
                break
            else:
                await gen.sleep(0.5)

        # Wait for address to be set
        while not getattr(self, 'scheduler_address', ''):
            await gen.sleep(0.5)

            report = await loop.run_in_executor(
                None, client.application_report, app_id
            )
            if str(report.state) in {'FAILED', 'KILLED', 'FINISHED'}:
                raise Exception("Application %s failed to start, check "
                                "application logs for more information"
                                % app_id)

        return self.scheduler_address, self.dashboard_address

    async def is_running(self):
        if self.app_id in {'', 'PENDING'}:
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
