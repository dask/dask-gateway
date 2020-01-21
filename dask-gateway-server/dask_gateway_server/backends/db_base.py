import os

from traitlets import Unicode, Bool, List, Float, validate, default

from . import db
from .base import Backend


class DatabaseBackend(Backend):
    db_url = Unicode(
        "sqlite:///:memory:",
        help="""
        The URL for the database. Default is in-memory only.

        If not in-memory, ``db_encrypt_keys`` must also be set.
        """,
        config=True,
    )

    db_encrypt_keys = List(
        help="""
        A list of keys to use to encrypt private data in the database. Can also
        be set by the environment variable ``DASK_GATEWAY_ENCRYPT_KEYS``, where
        the value is a ``;`` delimited string of encryption keys.

        Each key should be a base64-encoded 32 byte value, and should be
        cryptographically random. Lacking other options, openssl can be used to
        generate a single key via:

        .. code-block:: shell

            $ openssl rand -base64 32

        A single key is valid, multiple keys can be used to support key rotation.
        """,
        config=True,
    )

    @default("db_encrypt_keys")
    def _db_encrypt_keys_default(self):
        keys = os.environb.get(b"DASK_GATEWAY_ENCRYPT_KEYS", b"").strip()
        if not keys:
            return []
        return [db.normalize_encrypt_key(k) for k in keys.split(b";") if k.strip()]

    @validate("db_encrypt_keys")
    def _db_encrypt_keys_validate(self, proposal):
        if not proposal.value and not db.is_in_memory_db(self.db_url):
            raise ValueError(
                "Must configure `db_encrypt_keys`/`DASK_GATEWAY_ENCRYPT_KEYS` "
                "when not using an in-memory database"
            )
        return [db.normalize_encrypt_key(k) for k in proposal.value]

    db_debug = Bool(
        False, help="If True, all database operations will be logged", config=True
    )

    db_cleanup_period = Float(
        600,
        help="""
        Time (in seconds) between database cleanup tasks.

        This sets how frequently old records are removed from the database.
        This shouldn't be too small (to keep the overhead low), but should be
        smaller than ``db_record_max_age`` (probably by an order of magnitude).
        """,
        config=True,
    )

    db_cluster_max_age = Float(
        3600 * 24,
        help="""
        Max time (in seconds) to keep around records of completed clusters.

        Every ``db_cleanup_period``, completed clusters older than
        ``db_cluster_max_age`` are removed from the database.
        """,
        config=True,
    )

    async def on_startup(self):
        """Called when the server is starting up.

        Do any setup tasks in this method"""
        self.db = db.DataManager(
            url=self.db_url, echo=self.db_debug, encrypt_keys=self.db_encrypt_keys
        )

    async def on_shutdown(self):
        """Called when the server is shutting down.

        Do any cleanup tasks in this method"""
        pass

    async def has_permissions(self, user, cluster_id):
        """Check if a user has permissions to interact with a cluster.

        Parameters
        ----------
        user : User
            The user to check.
        cluster_id : str
            The cluster to check

        Returns
        -------
        has_permissions : bool
        """
        pass

    async def list_clusters(self, user=None, statuses=None):
        """List known clusters.

        Parameters
        ----------
        user : User, optional
            A user to filter on. If not provided, defaults to all users.
        statuses : list, optional
            A list of statuses to filter on. If not provided, defaults to all
            running and pending clusters.

        Returns
        -------
        clusters : List[Cluster]
        """
        return self.db.list_clusters(user=user, statuses=statuses)

    async def get_cluster(self, cluster_id):
        """Get information about a cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.

        Returns
        -------
        cluster : Cluster
        """
        return self.db.get_cluster(cluster_id)

    async def start_cluster(self, user, cluster_options):
        """Start a new cluster.

        Parameters
        ----------
        user : User
            The user making the request.
        cluster_options : dict
            Any additional options provided by the user.

        Returns
        -------
        cluster_id : str
        """
        cluster_options = await self.process_cluster_options(cluster_options)
        cluster = self.db.create_cluster(user, cluster_options)
        await self.enqueue(cluster.name)
        return cluster.name

    async def stop_cluster(self, cluster_id):
        """Stop a cluster.

        No-op if the cluster is already stopped.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.
        """
        cluster = self.db.get_cluster(cluster_id)
        self.db.update_cluster(cluster, status="STOPPING")
        await self.enqueue(cluster.name)

    async def scale_cluster(self, cluster_id, n):
        """Scale a cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.
        n : int
            The number of workers to scale to.
        """
        raise NotImplementedError

    async def adapt_cluster(self, cluster_id, minimum=None, maximum=None, active=True):
        """Adaptively scale a cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.
        minimum : int, optional
            The minimum number of workers to adaptively scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to adaptively scale to. Defaults to infinity.
        active : bool, optional
            Set to False to disable adaptive scaling.
        """
        raise NotImplementedError
