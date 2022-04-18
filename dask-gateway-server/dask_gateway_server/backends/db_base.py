import asyncio
import base64
import json
import os
import uuid
from collections import defaultdict
from itertools import chain, islice

import sqlalchemy as sa
from async_timeout import timeout
from cryptography.fernet import Fernet, MultiFernet
from traitlets import Bool, Float, Integer, List, Unicode, default, validate

from .. import models
from ..proxy import Proxy
from ..tls import new_keypair
from ..utils import Flag, FrozenAttrDict, TaskPool, normalize_address, timestamp
from ..workqueue import Backoff, WorkQueue, WorkQueueClosed
from .base import Backend

__all__ = ("DBBackendBase", "Cluster", "Worker")


def _normalize_encrypt_key(key):
    if isinstance(key, str):
        key = key.encode("ascii")

    if len(key) == 44:
        try:
            key = base64.urlsafe_b64decode(key)
        except ValueError:
            pass

    if len(key) == 32:
        return base64.urlsafe_b64encode(key)

    raise ValueError(
        "All keys in `db_encrypt_keys`/`DASK_GATEWAY_ENCRYPT_KEYS` must be 32 "
        "bytes, base64-encoded"
    )


def _is_in_memory_db(url):
    return url in ("sqlite://", "sqlite:///:memory:")


class _IntEnum(sa.TypeDecorator):
    impl = sa.Integer

    def __init__(self, enumclass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumclass = enumclass

    def process_bind_param(self, value, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        return self._enumclass(value)


class _JSON(sa.TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = sa.LargeBinary

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value).encode("utf-8")
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class JobStatus(models.IntEnum):
    CREATED = 1
    SUBMITTED = 2
    RUNNING = 3
    CLOSING = 4
    STOPPED = 5
    FAILED = 6


class Cluster:
    """Information on a cluster.

    Not all attributes on this object are publically accessible. When writing a
    backend, you may access the following attributes:

    Attributes
    ----------
    name : str
        The cluster name.
    username : str
        The user associated with this cluster.
    token : str
        The API token associated with this cluster. Used to authenticate the
        cluster with the gateway.
    config : FrozenAttrDict
        The serialized ``ClusterConfig`` associated with this cluster.
    state : dict
        Any cluster state, as yielded from ``do_start_cluster``.
    scheduler_address : str
        The scheduler address. The empty string if the cluster is not running.
    dashboard_address : str
        The dashboard address. The empty string if the cluster is not running,
        or no dashboard is running on the cluster.
    api_address : str
        The cluster's api address. The empty string if the cluster is not running.
    tls_cert : bytes
        The TLS cert credentials associated with the cluster.
    tls_key : bytes
        The TLS key credentials associated with the cluster.
    """

    def __init__(
        self,
        id=None,
        name=None,
        username=None,
        token=None,
        options=None,
        config=None,
        status=None,
        target=None,
        count=0,
        state=None,
        scheduler_address="",
        dashboard_address="",
        api_address="",
        tls_cert=b"",
        tls_key=b"",
        start_time=None,
        stop_time=None,
    ):
        self.id = id
        self.name = name
        self.username = username
        self.token = token
        self.options = options
        self.config = config
        self.status = status
        self.target = target
        self.count = count
        self.state = state
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.start_time = start_time
        self.stop_time = stop_time

        if self.status == JobStatus.RUNNING:
            self.last_heartbeat = timestamp()
        else:
            self.last_heartbeat = None
        self.worker_start_failure_count = 0
        self.added_to_proxies = False
        self.workers = {}

        self.ready = Flag()
        if self.status >= JobStatus.RUNNING:
            self.ready.set()
        self.shutdown = Flag()
        if self.status >= JobStatus.STOPPED:
            self.shutdown.set()

    _status_map = {
        (JobStatus.CREATED, JobStatus.RUNNING): models.ClusterStatus.PENDING,
        (JobStatus.CREATED, JobStatus.CLOSING): models.ClusterStatus.STOPPING,
        (JobStatus.CREATED, JobStatus.STOPPED): models.ClusterStatus.STOPPING,
        (JobStatus.CREATED, JobStatus.FAILED): models.ClusterStatus.STOPPING,
        (JobStatus.SUBMITTED, JobStatus.RUNNING): models.ClusterStatus.PENDING,
        (JobStatus.SUBMITTED, JobStatus.CLOSING): models.ClusterStatus.STOPPING,
        (JobStatus.SUBMITTED, JobStatus.STOPPED): models.ClusterStatus.STOPPING,
        (JobStatus.SUBMITTED, JobStatus.FAILED): models.ClusterStatus.STOPPING,
        (JobStatus.RUNNING, JobStatus.RUNNING): models.ClusterStatus.RUNNING,
        (JobStatus.RUNNING, JobStatus.CLOSING): models.ClusterStatus.STOPPING,
        (JobStatus.RUNNING, JobStatus.STOPPED): models.ClusterStatus.STOPPING,
        (JobStatus.RUNNING, JobStatus.FAILED): models.ClusterStatus.STOPPING,
        (JobStatus.CLOSING, JobStatus.STOPPED): models.ClusterStatus.STOPPING,
        (JobStatus.CLOSING, JobStatus.FAILED): models.ClusterStatus.STOPPING,
        (JobStatus.STOPPED, JobStatus.STOPPED): models.ClusterStatus.STOPPED,
        (JobStatus.FAILED, JobStatus.FAILED): models.ClusterStatus.FAILED,
    }

    def active_workers(self):
        return [w for w in self.workers.values() if w.is_active()]

    def is_active(self):
        return self.target < JobStatus.STOPPED

    def all_workers_at_least(self, status):
        return all(w.status >= status for w in self.workers.values())

    @property
    def model_status(self):
        return self._status_map[self.status, self.target]

    def to_model(self):
        return models.Cluster(
            name=self.name,
            username=self.username,
            token=self.token,
            options=self.options,
            config=self.config,
            status=self.model_status,
            scheduler_address=self.scheduler_address,
            dashboard_address=self.dashboard_address,
            api_address=self.api_address,
            tls_cert=self.tls_cert,
            tls_key=self.tls_key,
            start_time=self.start_time,
            stop_time=self.stop_time,
        )


class Worker:
    """Information on a worker.

    Not all attributes on this object are publically accessible. When writing a
    backend, you may access the following attributes:

    Attributes
    ----------
    name : str
        The worker name.
    cluster : Cluster
        The cluster associated with this worker.
    state : dict
        Any worker state, as yielded from ``do_start_worker``.
    """

    def __init__(
        self,
        id=None,
        name=None,
        cluster=None,
        status=None,
        target=None,
        state=None,
        start_time=None,
        stop_time=None,
        close_expected=False,
    ):
        self.id = id
        self.name = name
        self.cluster = cluster
        self.status = status
        self.target = target
        self.state = state
        self.start_time = start_time
        self.stop_time = stop_time
        self.close_expected = close_expected

    def is_active(self):
        return self.target < JobStatus.STOPPED


metadata = sa.MetaData()

clusters = sa.Table(
    "clusters",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
    sa.Column("username", sa.Unicode(255), nullable=False),
    sa.Column("status", _IntEnum(JobStatus), nullable=False),
    sa.Column("target", _IntEnum(JobStatus), nullable=False),
    sa.Column("count", sa.Integer, nullable=False),
    sa.Column("options", _JSON, nullable=False),
    sa.Column("config", _JSON, nullable=False),
    sa.Column("state", _JSON, nullable=False),
    sa.Column("token", sa.BINARY(140), nullable=False, unique=True),
    sa.Column("scheduler_address", sa.Unicode(255), nullable=False),
    sa.Column("dashboard_address", sa.Unicode(255), nullable=False),
    sa.Column("api_address", sa.Unicode(255), nullable=False),
    sa.Column("tls_credentials", sa.LargeBinary, nullable=False),
    sa.Column("start_time", sa.Integer, nullable=False),
    sa.Column("stop_time", sa.Integer, nullable=True),
)

workers = sa.Table(
    "workers",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.Unicode(255), nullable=False),
    sa.Column(
        "cluster_id", sa.ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False
    ),
    sa.Column("status", _IntEnum(JobStatus), nullable=False),
    sa.Column("target", _IntEnum(JobStatus), nullable=False),
    sa.Column("state", _JSON, nullable=False),
    sa.Column("start_time", sa.Integer, nullable=False),
    sa.Column("stop_time", sa.Integer, nullable=True),
    sa.Column("close_expected", sa.Integer, nullable=False),
)


class DataManager:
    """Holds the internal state for a single Dask Gateway.

    Keeps the memory representation in-sync with the database.
    """

    def __init__(self, url="sqlite:///:memory:", encrypt_keys=(), **kwargs):
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}

        if _is_in_memory_db(url):
            kwargs["poolclass"] = sa.pool.StaticPool
            self.fernet = None
        else:
            self.fernet = MultiFernet([Fernet(key) for key in encrypt_keys])

        engine = sa.create_engine(url, **kwargs)
        if url.startswith("sqlite"):
            # Register PRAGMA foreigh_keys=on for sqlite
            @sa.event.listens_for(engine, "connect")
            def connect(dbapi_con, con_record):
                cursor = dbapi_con.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        metadata.create_all(engine)

        self.db = engine

        self.username_to_clusters = defaultdict(dict)
        self.name_to_cluster = {}
        self.id_to_cluster = {}

        # Load all existing clusters into memory
        for c in self.db.execute(clusters.select()):
            tls_cert, tls_key = self.decode_tls_credentials(c.tls_credentials)
            token = self.decode_token(c.token)
            cluster = Cluster(
                id=c.id,
                name=c.name,
                username=c.username,
                token=token,
                options=c.options,
                config=FrozenAttrDict(c.config),
                status=c.status,
                target=c.target,
                count=c.count,
                state=c.state,
                scheduler_address=c.scheduler_address,
                dashboard_address=c.dashboard_address,
                api_address=c.api_address,
                tls_cert=tls_cert,
                tls_key=tls_key,
                start_time=c.start_time,
                stop_time=c.stop_time,
            )
            self.username_to_clusters[cluster.username][cluster.name] = cluster
            self.id_to_cluster[cluster.id] = cluster
            self.name_to_cluster[cluster.name] = cluster

        # Next load all existing workers into memory
        for w in self.db.execute(workers.select()):
            cluster = self.id_to_cluster[w.cluster_id]
            worker = Worker(
                id=w.id,
                name=w.name,
                status=w.status,
                target=w.target,
                cluster=cluster,
                state=w.state,
                start_time=w.start_time,
                stop_time=w.stop_time,
                close_expected=w.close_expected,
            )
            cluster.workers[worker.name] = worker

    def cleanup_expired(self, max_age_in_seconds):
        cutoff = timestamp() - max_age_in_seconds * 1000
        with self.db.begin() as conn:
            to_delete = conn.execute(
                sa.select([clusters.c.id]).where(clusters.c.stop_time < cutoff)
            ).fetchall()

            if to_delete:
                to_delete = [i for i, in to_delete]

                conn.execute(
                    clusters.delete().where(clusters.c.id == sa.bindparam("id")),
                    [{"id": i} for i in to_delete],
                )

                for i in to_delete:
                    cluster = self.id_to_cluster.pop(i)
                    self.name_to_cluster.pop(cluster.name, None)
                    user_clusters = self.username_to_clusters[cluster.username]
                    user_clusters.pop(cluster.name)
                    if not user_clusters:
                        self.username_to_clusters.pop(cluster.username)

        return len(to_delete)

    def encrypt(self, b):
        """Encrypt bytes ``b``. If encryption is disabled this is a no-op"""
        return b if self.fernet is None else self.fernet.encrypt(b)

    def decrypt(self, b):
        """Decrypt bytes ``b``. If encryption is disabled this is a no-op"""
        return b if self.fernet is None else self.fernet.decrypt(b)

    def encode_tls_credentials(self, tls_cert, tls_key):
        return self.encrypt(b";".join((tls_cert, tls_key)))

    def decode_tls_credentials(self, data):
        return self.decrypt(data).split(b";")

    def encode_token(self, token):
        return self.encrypt(token.encode("utf8"))

    def decode_token(self, data):
        return self.decrypt(data).decode()

    def get_cluster(self, cluster_name):
        return self.name_to_cluster.get(cluster_name)

    def list_clusters(self, username=None, statuses=None):
        if statuses is None:
            select = lambda x: x.is_active()
        else:
            statuses = set(statuses)
            select = lambda x: x.model_status in statuses
        if username is None:
            return [
                cluster for cluster in self.name_to_cluster.values() if select(cluster)
            ]
        else:
            clusters = self.username_to_clusters.get(username)
            if clusters is None:
                return []
            return [cluster for cluster in clusters.values() if select(cluster)]

    def active_clusters(self):
        for cluster in self.name_to_cluster.values():
            if cluster.is_active():
                yield cluster

    def create_cluster(self, username, options, config):
        """Create a new cluster for a user"""
        cluster_name = uuid.uuid4().hex
        token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)
        # Encode the tls credentials for storing in the database
        tls_credentials = self.encode_tls_credentials(tls_cert, tls_key)
        enc_token = self.encode_token(token)

        common = {
            "name": cluster_name,
            "username": username,
            "options": options,
            "status": JobStatus.CREATED,
            "target": JobStatus.RUNNING,
            "count": 0,
            "state": {},
            "scheduler_address": "",
            "dashboard_address": "",
            "api_address": "",
            "start_time": timestamp(),
        }

        with self.db.begin() as conn:
            res = conn.execute(
                clusters.insert().values(
                    tls_credentials=tls_credentials,
                    token=enc_token,
                    config=config,
                    **common,
                )
            )
            cluster = Cluster(
                id=res.inserted_primary_key[0],
                token=token,
                tls_cert=tls_cert,
                tls_key=tls_key,
                config=FrozenAttrDict(config),
                **common,
            )
            self.id_to_cluster[cluster.id] = cluster
            self.name_to_cluster[cluster_name] = cluster
            self.username_to_clusters[username][cluster_name] = cluster

        return cluster

    def create_worker(self, cluster):
        """Create a new worker for a cluster"""
        worker_name = uuid.uuid4().hex

        common = {
            "name": worker_name,
            "status": JobStatus.CREATED,
            "target": JobStatus.RUNNING,
            "state": {},
            "start_time": timestamp(),
            "close_expected": False,
        }

        with self.db.begin() as conn:
            res = conn.execute(workers.insert().values(cluster_id=cluster.id, **common))
            worker = Worker(id=res.inserted_primary_key[0], cluster=cluster, **common)
            cluster.workers[worker.name] = worker

        return worker

    def update_cluster(self, cluster, **kwargs):
        """Update a cluster's state"""
        with self.db.begin() as conn:
            conn.execute(
                clusters.update().where(clusters.c.id == cluster.id).values(**kwargs)
            )
            for k, v in kwargs.items():
                setattr(cluster, k, v)

    def update_clusters(self, updates):
        """Update multiple clusters' states"""
        if not updates:
            return
        with self.db.begin() as conn:
            conn.execute(
                clusters.update().where(clusters.c.id == sa.bindparam("_id")),
                [{"_id": c.id, **u} for c, u in updates],
            )
            for c, u in updates:
                for k, v in u.items():
                    setattr(c, k, v)

    def update_worker(self, worker, **kwargs):
        """Update a worker's state"""
        with self.db.begin() as conn:
            conn.execute(
                workers.update().where(workers.c.id == worker.id).values(**kwargs)
            )
            for k, v in kwargs.items():
                setattr(worker, k, v)

    def update_workers(self, updates):
        """Update multiple workers' states"""
        if not updates:
            return
        with self.db.begin() as conn:
            conn.execute(
                workers.update().where(workers.c.id == sa.bindparam("_id")),
                [{"_id": w.id, **u} for w, u in updates],
            )
            for w, u in updates:
                for k, v in u.items():
                    setattr(w, k, v)


class DBBackendBase(Backend):
    """A base class for defining backends that rely on a database for managing state.

    Subclasses should define the following methods:

    - ``do_setup``
    - ``do_cleanup``
    - ``do_start_cluster``
    - ``do_stop_cluster``
    - ``do_check_clusters``
    - ``do_start_worker``
    - ``do_stop_worker``
    - ``do_check_workers``
    """

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
        keys = [
            k.strip()
            for k in os.environb.get(b"DASK_GATEWAY_ENCRYPT_KEYS", b"").split(b";")
            if k.strip()
        ]
        return self._db_encrypt_keys_validate({"value": keys})

    @validate("db_encrypt_keys")
    def _db_encrypt_keys_validate(self, proposal):
        if not proposal["value"] and not _is_in_memory_db(self.db_url):
            raise ValueError(
                "Must configure `db_encrypt_keys`/`DASK_GATEWAY_ENCRYPT_KEYS` "
                "when not using an in-memory database"
            )
        return [_normalize_encrypt_key(k) for k in proposal["value"]]

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

    stop_clusters_on_shutdown = Bool(
        True,
        help="""
        Whether to stop active clusters on gateway shutdown.

        If true, all active clusters will be stopped before shutting down the
        gateway. Set to False to leave active clusters running.
        """,
        config=True,
    )

    @validate("stop_clusters_on_shutdown")
    def _stop_clusters_on_shutdown_validate(self, proposal):
        if not proposal.value and _is_in_memory_db(self.db_url):
            raise ValueError(
                "When using an in-memory database, `stop_clusters_on_shutdown` "
                "must be True"
            )
        return proposal.value

    cluster_status_period = Float(
        30,
        help="""
        Time (in seconds) between cluster status checks.

        A smaller period will detect failed clusters sooner, but will use more
        resources. A larger period will provide slower feedback in the presence
        of failures.
        """,
        config=True,
    )

    worker_status_period = Float(
        30,
        help="""
        Time (in seconds) between worker status checks.

        A smaller period will detect failed workers sooner, but will use more
        resources. A larger period will provide slower feedback in the presence
        of failures.
        """,
        config=True,
    )

    cluster_heartbeat_period = Integer(
        15,
        help="""
        Time (in seconds) between cluster heartbeats to the gateway.

        A smaller period will detect failed workers sooner, but will use more
        resources. A larger period will provide slower feedback in the presence
        of failures.
        """,
        config=True,
    )

    cluster_heartbeat_timeout = Float(
        help="""
        Timeout (in seconds) before killing a dask cluster that's failed to heartbeat.

        This should be greater than ``cluster_heartbeat_period``.  Defaults to
        ``2 * cluster_heartbeat_period``.
        """,
        config=True,
    )

    @default("cluster_heartbeat_timeout")
    def _default_cluster_heartbeat_timeout(self):
        return self.cluster_heartbeat_period * 2

    cluster_start_timeout = Float(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask cluster.
        """,
        config=True,
    )

    worker_start_timeout = Float(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask worker.
        """,
        config=True,
    )

    check_timeouts_period = Float(
        help="""
        Time (in seconds) between timeout checks.

        This shouldn't be too small (to keep the overhead low), but should be
        smaller than ``cluster_heartbeat_timeout``, ``cluster_start_timeout``,
        and ``worker_start_timeout``.
        """,
        config=True,
    )

    @default("check_timeouts_period")
    def _default_check_timeouts_period(self):
        min_timeout = min(
            self.cluster_heartbeat_timeout,
            self.cluster_start_timeout,
            self.worker_start_timeout,
        )
        return min(20, min_timeout / 2)

    worker_start_failure_limit = Integer(
        3,
        help="""
        A limit on the number of failed attempts to start a worker before the
        cluster is marked as failed.

        Every worker that fails to start (timeouts exempt) increments a
        counter. The counter is reset if a worker successfully starts. If the
        counter ever exceeds this limit, the cluster is marked as failed and is
        shutdown.
        """,
        config=True,
    )

    parallelism = Integer(
        20,
        help="""
        Number of handlers to use for starting/stopping clusters.
        """,
        config=True,
    )

    backoff_base_delay = Float(
        0.1,
        help="""
        Base delay (in seconds) for backoff when retrying after failures.

        If an operation fails, it is retried after a backoff computed as:

        ```
        min(backoff_max_delay, backoff_base_delay * 2 ** num_failures)
        ```
        """,
        config=True,
    )

    backoff_max_delay = Float(
        300,
        help="""
        Max delay (in seconds) for backoff policy when retrying after failures.
        """,
        config=True,
    )

    api_url = Unicode(
        help="""
        The address that internal components (e.g. dask clusters)
        will use when contacting the gateway.

        Defaults to `{proxy_address}/{prefix}/api`, set manually if a different
        address should be used.
        """,
        config=True,
    )

    @default("api_url")
    def _api_url_default(self):
        proxy = self.proxy
        scheme = "https" if proxy.tls_cert else "http"
        address = normalize_address(proxy.address, resolve_host=True)
        return f"{scheme}://{address}{proxy.prefix}/api"

    async def setup(self, app):
        await super().setup(app)

        # Setup reconcilation queues
        self.queue = WorkQueue(
            backoff=Backoff(
                base_delay=self.backoff_base_delay, max_delay=self.backoff_max_delay
            )
        )
        self.reconcilers = [
            asyncio.ensure_future(self.reconciler_loop())
            for _ in range(self.parallelism)
        ]

        # Start the proxy
        self.proxy = Proxy(parent=self, log=self.log)
        await self.proxy.setup(app)

        # Load the database
        self.db = DataManager(
            url=self.db_url, echo=self.db_debug, encrypt_keys=self.db_encrypt_keys
        )

        # Start background tasks
        self.task_pool = TaskPool()
        self.task_pool.spawn(self.check_timeouts_loop())
        self.task_pool.spawn(self.check_clusters_loop())
        self.task_pool.spawn(self.check_workers_loop())
        self.task_pool.spawn(self.cleanup_db_loop())

        # Load all active clusters/workers into reconcilation queues
        for cluster in self.db.name_to_cluster.values():
            if cluster.status < JobStatus.STOPPED:
                self.queue.put(cluster)
                for worker in cluster.workers.values():
                    if worker.status < JobStatus.STOPPED:
                        self.queue.put(worker)

        # Further backend-specific setup
        await self.do_setup()

        self.log.info(
            "Backend started, clusters will contact api server at %s", self.api_url
        )

    async def cleanup(self):
        if hasattr(self, "task_pool"):
            # Stop background tasks
            await self.task_pool.close()

        if hasattr(self, "db"):
            if self.stop_clusters_on_shutdown:
                # Request all active clusters be stopped
                active = list(self.db.active_clusters())
                if active:
                    self.log.info("Stopping %d active clusters...", len(active))
                    self.db.update_clusters(
                        [(c, {"target": JobStatus.FAILED}) for c in active]
                    )
                    for c in active:
                        self.queue.put(c)

                # Wait until all clusters are shutdown
                pending_shutdown = [
                    c
                    for c in self.db.name_to_cluster.values()
                    if c.status < JobStatus.STOPPED
                ]
                if pending_shutdown:
                    await asyncio.wait([c.shutdown for c in pending_shutdown])

        # Stop reconcilation queues
        if hasattr(self, "reconcilers"):
            self.queue.close()
            await asyncio.gather(*self.reconcilers, return_exceptions=True)

        await self.do_cleanup()

        if hasattr(self, "proxy"):
            await self.proxy.cleanup()

        await super().cleanup()

    async def list_clusters(self, username=None, statuses=None):
        clusters = self.db.list_clusters(username=username, statuses=statuses)
        return [c.to_model() for c in clusters]

    async def get_cluster(self, cluster_name, wait=False):
        cluster = self.db.get_cluster(cluster_name)
        if cluster is None:
            return None
        if wait:
            try:
                await asyncio.wait_for(cluster.ready, 20)
            except asyncio.TimeoutError:
                pass
        return cluster.to_model()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)
        cluster = self.db.create_cluster(user.name, options, config.to_dict())
        self.log.info("Created cluster %s for user %s", cluster.name, user.name)
        self.queue.put(cluster)
        return cluster.name

    async def stop_cluster(self, cluster_name, failed=False):
        cluster = self.db.get_cluster(cluster_name)
        if cluster is None:
            return
        if cluster.target <= JobStatus.RUNNING:
            self.log.info("Stopping cluster %s", cluster.name)
            target = JobStatus.FAILED if failed else JobStatus.STOPPED
            self.db.update_cluster(cluster, target=target)
            self.queue.put(cluster)

    async def on_cluster_heartbeat(self, cluster_name, msg):
        cluster = self.db.get_cluster(cluster_name)
        if cluster is None or cluster.target > JobStatus.RUNNING:
            return

        cluster.last_heartbeat = timestamp()

        if cluster.status == JobStatus.RUNNING:
            cluster_update = {}
        else:
            cluster_update = {
                "api_address": msg["api_address"],
                "scheduler_address": msg["scheduler_address"],
                "dashboard_address": msg["dashboard_address"],
            }

        count = msg["count"]
        active_workers = set(msg["active_workers"])
        closing_workers = set(msg["closing_workers"])
        closed_workers = set(msg["closed_workers"])

        self.log.info(
            "Cluster %s heartbeat [count: %d, n_active: %d, n_closing: %d, n_closed: %d]",
            cluster_name,
            count,
            len(active_workers),
            len(closing_workers),
            len(closed_workers),
        )

        max_workers = cluster.config.get("cluster_max_workers")
        if max_workers is not None and count > max_workers:
            # This shouldn't happen under normal operation, but could if the
            # user does something malicious (or there's a bug).
            self.log.info(
                "Cluster %s heartbeat requested %d workers, exceeding limit of %s.",
                cluster_name,
                count,
                max_workers,
            )
            count = max_workers

        if count != cluster.count:
            cluster_update["count"] = count

        created_workers = []
        submitted_workers = []
        target_updates = []
        newly_running = []
        close_expected = []
        for worker in cluster.workers.values():
            if worker.status >= JobStatus.STOPPED:
                continue
            elif worker.name in closing_workers:
                if worker.status < JobStatus.RUNNING:
                    newly_running.append(worker)
                close_expected.append(worker)
            elif worker.name in active_workers:
                if worker.status < JobStatus.RUNNING:
                    newly_running.append(worker)
            elif worker.name in closed_workers:
                target = (
                    JobStatus.STOPPED if worker.close_expected else JobStatus.FAILED
                )
                target_updates.append((worker, {"target": target}))
            else:
                if worker.status == JobStatus.SUBMITTED:
                    submitted_workers.append(worker)
                else:
                    assert worker.status == JobStatus.CREATED
                    created_workers.append(worker)

        n_pending = len(created_workers) + len(submitted_workers)
        n_to_stop = len(active_workers) + n_pending - count
        if n_to_stop > 0:
            for w in islice(chain(created_workers, submitted_workers), n_to_stop):
                target_updates.append((w, {"target": JobStatus.STOPPED}))

        if cluster_update:
            self.db.update_cluster(cluster, **cluster_update)
            self.queue.put(cluster)

        self.db.update_workers(target_updates)
        for w, u in target_updates:
            self.queue.put(w)

        if newly_running:
            # At least one worker successfully started, reset failure count
            cluster.worker_start_failure_count = 0
            self.db.update_workers(
                [(w, {"status": JobStatus.RUNNING}) for w in newly_running]
            )
            for w in newly_running:
                self.log.info("Worker %s is running", w.name)

        self.db.update_workers([(w, {"close_expected": True}) for w in close_expected])

    async def check_timeouts_loop(self):
        while True:
            await asyncio.sleep(self.check_timeouts_period)
            try:
                await self._check_timeouts()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(
                    "Exception while checking for timed out clusters/workers",
                    exc_info=exc,
                )

    async def _check_timeouts(self):
        self.log.debug("Checking for timed out clusters/workers")
        now = timestamp()
        cluster_heartbeat_cutoff = now - self.cluster_heartbeat_timeout * 1000
        cluster_start_cutoff = now - self.cluster_start_timeout * 1000
        worker_start_cutoff = now - self.worker_start_timeout * 1000
        cluster_updates = []
        worker_updates = []
        for cluster in self.db.active_clusters():
            if cluster.status == JobStatus.SUBMITTED:
                # Check if submitted clusters have timed out
                if cluster.start_time < cluster_start_cutoff:
                    self.log.info("Cluster %s startup timed out", cluster.name)
                    cluster_updates.append((cluster, {"target": JobStatus.FAILED}))
            elif cluster.status == JobStatus.RUNNING:
                # Check if running clusters have missed a heartbeat
                if cluster.last_heartbeat < cluster_heartbeat_cutoff:
                    self.log.info("Cluster %s heartbeat timed out", cluster.name)
                    cluster_updates.append((cluster, {"target": JobStatus.FAILED}))
                else:
                    for w in cluster.workers.values():
                        # Check if submitted workers have timed out
                        if (
                            w.status == JobStatus.SUBMITTED
                            and w.target == JobStatus.RUNNING
                            and w.start_time < worker_start_cutoff
                        ):
                            self.log.info("Worker %s startup timed out", w.name)
                            worker_updates.append((w, {"target": JobStatus.FAILED}))
        self.db.update_clusters(cluster_updates)
        for c, _ in cluster_updates:
            self.queue.put(c)
        self.db.update_workers(worker_updates)
        for w, _ in worker_updates:
            self.queue.put(w)

    async def check_clusters_loop(self):
        while True:
            await asyncio.sleep(self.cluster_status_period)
            self.log.debug("Checking pending cluster statuses")
            try:
                clusters = [
                    c
                    for c in self.db.active_clusters()
                    if c.status == JobStatus.SUBMITTED
                ]
                statuses = await self.do_check_clusters(clusters)
                updates = [
                    (c, {"target": JobStatus.FAILED})
                    for c, ok in zip(clusters, statuses)
                    if not ok
                ]
                self.db.update_clusters(updates)
                for c, _ in updates:
                    self.log.info("Cluster %s failed during startup", c.name)
                    self.queue.put(c)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(
                    "Exception while checking cluster statuses", exc_info=exc
                )

    async def check_workers_loop(self):
        while True:
            await asyncio.sleep(self.worker_status_period)
            self.log.debug("Checking pending worker statuses")
            try:
                clusters = (
                    c
                    for c in self.db.active_clusters()
                    if c.status == JobStatus.RUNNING
                )
                workers = [
                    w
                    for c in clusters
                    for w in c.active_workers()
                    if w.status == JobStatus.SUBMITTED
                ]
                statuses = await self.do_check_workers(workers)
                updates = [
                    (w, {"target": JobStatus.FAILED})
                    for w, ok in zip(workers, statuses)
                    if not ok
                ]
                self.db.update_workers(updates)
                for w, _ in updates:
                    self.log.info("Worker %s failed during startup", w.name)
                    w.cluster.worker_start_failure_count += 1
                    self.queue.put(w)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning(
                    "Exception while checking worker statuses", exc_info=exc
                )

    async def cleanup_db_loop(self):
        while True:
            try:
                n = self.db.cleanup_expired(self.db_cluster_max_age)
            except Exception as exc:
                self.log.error(
                    "Error while cleaning expired database records", exc_info=exc
                )
            else:
                self.log.debug("Removed %d expired clusters from the database", n)
            await asyncio.sleep(self.db_cleanup_period)

    async def reconciler_loop(self):
        while True:
            try:
                obj = await self.queue.get()
            except WorkQueueClosed:
                return

            if isinstance(obj, Cluster):
                method = self.reconcile_cluster
                kind = "cluster"
            else:
                method = self.reconcile_worker
                kind = "worker"

            self.log.debug(
                "Reconciling %s %s, %s -> %s",
                kind,
                obj.name,
                obj.status.name,
                obj.target.name,
            )

            try:
                await method(obj)
            except Exception:
                self.log.warning(
                    "Error while reconciling %s %s", kind, obj.name, exc_info=True
                )
                self.queue.put_backoff(obj)
            else:
                self.queue.reset_backoff(obj)
            finally:
                self.queue.task_done(obj)

    async def reconcile_cluster(self, cluster):
        if cluster.status >= JobStatus.STOPPED:
            return

        if cluster.target in (JobStatus.STOPPED, JobStatus.FAILED):
            if cluster.status == JobStatus.CLOSING:
                if self.is_cluster_ready_to_close(cluster):
                    await self._cluster_to_stopped(cluster)
            else:
                await self._cluster_to_closing(cluster)
            return

        if cluster.target == JobStatus.RUNNING:
            if cluster.status == JobStatus.CREATED:
                await self._cluster_to_submitted(cluster)
                return

            if cluster.status == JobStatus.SUBMITTED and cluster.scheduler_address:
                await self._cluster_to_running(cluster)

            if cluster.status == JobStatus.RUNNING:
                await self._check_cluster_proxied(cluster)
                await self._check_cluster_scale(cluster)

    async def reconcile_worker(self, worker):
        if worker.status >= JobStatus.STOPPED:
            return

        if worker.target == JobStatus.CLOSING:
            if worker.status != JobStatus.CLOSING:
                self.db.update_worker(worker, status=JobStatus.CLOSING)
            if self.is_cluster_ready_to_close(worker.cluster):
                self.queue.put(worker.cluster)
            return

        if worker.target in (JobStatus.STOPPED, JobStatus.FAILED):
            await self._worker_to_stopped(worker)
            if self.is_cluster_ready_to_close(worker.cluster):
                self.queue.put(worker.cluster)
            elif (
                worker.cluster.target == JobStatus.RUNNING and not worker.close_expected
            ):
                self.queue.put(worker.cluster)
            return

        if worker.status == JobStatus.CREATED and worker.target == JobStatus.RUNNING:
            await self._worker_to_submitted(worker)
            return

    def is_cluster_ready_to_close(self, cluster):
        return (
            cluster.status == JobStatus.CLOSING
            and (
                self.supports_bulk_shutdown
                and cluster.all_workers_at_least(JobStatus.CLOSING)
            )
            or cluster.all_workers_at_least(JobStatus.STOPPED)
        )

    async def _cluster_to_submitted(self, cluster):
        self.log.info("Submitting cluster %s...", cluster.name)
        try:
            async with timeout(self.cluster_start_timeout):
                async for state in self.do_start_cluster(cluster):
                    self.log.debug("State update for cluster %s", cluster.name)
                    self.db.update_cluster(cluster, state=state)
            self.db.update_cluster(cluster, status=JobStatus.SUBMITTED)
            self.log.info("Cluster %s submitted", cluster.name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                self.log.info("Cluster %s startup timed out", cluster.name)
            else:
                self.log.warning(
                    "Failed to submit cluster %s", cluster.name, exc_info=exc
                )
            self.db.update_cluster(
                cluster, status=JobStatus.SUBMITTED, target=JobStatus.FAILED
            )
            self.queue.put(cluster)

    async def _cluster_to_closing(self, cluster):
        self.log.debug("Preparing to stop cluster %s", cluster.name)
        target = JobStatus.CLOSING if self.supports_bulk_shutdown else JobStatus.STOPPED
        workers = [w for w in cluster.workers.values() if w.target < target]
        self.db.update_workers([(w, {"target": target}) for w in workers])
        for w in workers:
            self.queue.put(w)
        self.db.update_cluster(cluster, status=JobStatus.CLOSING)
        if not workers:
            # If there are workers, the cluster will be enqueued after the last one closed
            # If there are no workers, requeue now
            self.queue.put(cluster)
        cluster.ready.set()

    async def _cluster_to_stopped(self, cluster):
        self.log.info("Stopping cluster %s...", cluster.name)
        if cluster.status > JobStatus.CREATED:
            try:
                await self.do_stop_cluster(cluster)
            except Exception as exc:
                self.log.warning(
                    "Exception while stopping cluster %s", cluster.name, exc_info=exc
                )
            await self.proxy.remove_route(kind="PATH", path=f"/clusters/{cluster.name}")
            await self.proxy.remove_route(kind="SNI", sni=cluster.name)
        self.log.info("Cluster %s stopped", cluster.name)
        self.db.update_workers(
            [
                (w, {"status": JobStatus.STOPPED, "target": JobStatus.STOPPED})
                for w in cluster.workers.values()
                if w.status < JobStatus.STOPPED
            ]
        )
        self.db.update_cluster(cluster, status=cluster.target, stop_time=timestamp())
        cluster.ready.set()
        cluster.shutdown.set()

    async def _cluster_to_running(self, cluster):
        self.log.info("Cluster %s is running", cluster.name)
        self.db.update_cluster(cluster, status=JobStatus.RUNNING)
        cluster.ready.set()

    async def _check_cluster_proxied(self, cluster):
        if not cluster.added_to_proxies:
            self.log.info("Adding cluster %s routes to proxies", cluster.name)
            if cluster.dashboard_address:
                await self.proxy.add_route(
                    kind="PATH",
                    path=f"/clusters/{cluster.name}",
                    target=cluster.dashboard_address,
                )
            await self.proxy.add_route(
                kind="SNI", sni=cluster.name, target=cluster.scheduler_address
            )
            cluster.added_to_proxies = True

    async def _check_cluster_scale(self, cluster):
        if cluster.worker_start_failure_count >= self.worker_start_failure_limit:
            self.log.info(
                "Cluster %s had %d consecutive workers fail to start, failing the cluster",
                cluster.name,
                cluster.worker_start_failure_count,
            )
            self.db.update_cluster(cluster, target=JobStatus.FAILED)
            self.queue.put(cluster)
            return

        active = cluster.active_workers()
        if cluster.count > len(active):
            for _ in range(cluster.count - len(active)):
                worker = self.db.create_worker(cluster)
                self.log.info(
                    "Created worker %s for cluster %s", worker.name, cluster.name
                )
                self.queue.put(worker)

    async def _worker_to_submitted(self, worker):
        self.log.info("Submitting worker %s...", worker.name)
        try:
            async with timeout(self.worker_start_timeout):
                async for state in self.do_start_worker(worker):
                    self.log.debug("State update for worker %s", worker.name)
                    self.db.update_worker(worker, state=state)
            self.db.update_worker(worker, status=JobStatus.SUBMITTED)
            self.log.info("Worker %s submitted", worker.name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, asyncio.TimeoutError):
                self.log.info("Worker %s startup timed out", worker.name)
            else:
                self.log.warning(
                    "Failed to submit worker %s", worker.name, exc_info=exc
                )
            self.db.update_worker(
                worker, status=JobStatus.SUBMITTED, target=JobStatus.FAILED
            )
            worker.cluster.worker_start_failure_count += 1
            self.queue.put(worker)

    async def _worker_to_stopped(self, worker):
        self.log.info("Stopping worker %s...", worker.name)
        if worker.status > JobStatus.CREATED:
            try:
                await self.do_stop_worker(worker)
            except Exception as exc:
                self.log.warning(
                    "Exception while stopping worker %s", worker.name, exc_info=exc
                )
        self.log.info("Worker %s stopped", worker.name)
        self.db.update_worker(worker, status=worker.target, stop_time=timestamp())

    def get_tls_paths(self, cluster):
        """Return the paths to the cert and key files for this cluster"""
        return "dask.crt", "dask.pem"

    def get_env(self, cluster):
        """Get a dict of environment variables to set for the process"""
        out = dict(cluster.config.environment)
        # Set values that dask-gateway needs to run
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_API_TOKEN": cluster.token,
                "DASK_GATEWAY_CLUSTER_NAME": cluster.name,
                "DASK_DISTRIBUTED__COMM__REQUIRE_ENCRYPTION": "True",
            }
        )
        return out

    def get_scheduler_env(self, cluster):
        env = self.get_env(cluster)
        tls_cert_path, tls_key_path = self.get_tls_paths(cluster)
        env.update(
            {
                "DASK_DISTRIBUTED__COMM__TLS__CA_FILE": tls_cert_path,
                "DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__KEY": tls_key_path,
                "DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__CERT": tls_cert_path,
            }
        )
        return env

    def get_worker_env(self, cluster):
        env = self.get_env(cluster)
        tls_cert_path, tls_key_path = self.get_tls_paths(cluster)
        env.update(
            {
                "DASK_DISTRIBUTED__COMM__TLS__CA_FILE": tls_cert_path,
                "DASK_DISTRIBUTED__COMM__TLS__WORKER__KEY": tls_key_path,
                "DASK_DISTRIBUTED__COMM__TLS__WORKER__CERT": tls_cert_path,
            }
        )
        return env

    default_host = "0.0.0.0"

    def get_scheduler_command(self, cluster):
        return cluster.config.scheduler_cmd + [
            "--protocol",
            "tls",
            "--port",
            "0",
            "--host",
            self.default_host,
            "--dashboard-address",
            f"{self.default_host}:0",
            "--preload",
            "dask_gateway.scheduler_preload",
            "--dg-api-address",
            f"{self.default_host}:0",
            "--dg-heartbeat-period",
            str(self.cluster_heartbeat_period),
            "--dg-adaptive-period",
            str(cluster.config.adaptive_period),
            "--dg-idle-timeout",
            str(cluster.config.idle_timeout),
        ]

    def worker_nthreads_memory_limit_args(self, cluster):
        return str(cluster.config.worker_threads), str(cluster.config.worker_memory)

    def get_worker_command(self, cluster, worker_name, scheduler_address=None):
        nthreads, memory_limit = self.worker_nthreads_memory_limit_args(cluster)
        if scheduler_address is None:
            scheduler_address = cluster.scheduler_address
        return cluster.config.worker_cmd + [
            scheduler_address,
            "--dashboard-address",
            f"{self.default_host}:0",
            "--name",
            worker_name,
            "--nthreads",
            nthreads,
            "--memory-limit",
            memory_limit,
        ]

    # Subclasses should implement these methods
    supports_bulk_shutdown = False

    async def do_setup(self):
        """Called when the server is starting up.

        Do any initialization here.
        """
        pass

    async def do_cleanup(self):
        """Called when the server is shutting down.

        Do any cleanup here."""
        pass

    async def do_start_cluster(self, cluster):
        """Start a cluster.

        This should do any initialization for the whole dask cluster
        application, and then start the scheduler.

        Parameters
        ----------
        cluster : Cluster
            Information on the cluster to be started.

        Yields
        ------
        cluster_state : dict
            Any state needed for further interactions with this cluster. This
            should be serializable using ``json.dumps``. If startup occurs in
            multiple stages, can iteratively yield state updates to be
            checkpointed. If an error occurs at any time, the last yielded
            state will be used when calling ``do_stop_cluster``.
        """
        raise NotImplementedError

    async def do_stop_cluster(self, cluster):
        """Stop a cluster.

        Parameters
        ----------
        cluster : Cluster
            Information on the cluster to be stopped.
        """
        raise NotImplementedError

    async def do_check_clusters(self, clusters):
        """Check the status of multiple clusters.

        This is periodically called to check the status of pending clusters.
        Once a cluster is running this will no longer be called.

        Parameters
        ----------
        clusters : List[Cluster]
            The clusters to be checked.

        Returns
        -------
        statuses : List[bool]
            The status for each cluster. Return False if the cluster has
            stopped or failed, True if the cluster is pending start or running.
        """
        raise NotImplementedError

    async def do_start_worker(self, worker):
        """Start a worker.

        Parameters
        ----------
        worker : Worker
            Information on the worker to be started.

        Yields
        ------
        worker_state : dict
            Any state needed for further interactions with this worker. This
            should be serializable using ``json.dumps``. If startup occurs in
            multiple stages, can iteratively yield state updates to be
            checkpointed. If an error occurs at any time, the last yielded
            state will be used when calling ``do_stop_worker``.
        """
        raise NotImplementedError

    async def do_stop_worker(self, worker):
        """Stop a worker.

        Parameters
        ----------
        worker : Worker
            Information on the worker to be stopped.
        """
        raise NotImplementedError

    async def do_check_workers(self, workers):
        """Check the status of multiple workers.

        This is periodically called to check the status of pending workers.
        Once a worker is running this will no longer be called.

        Parameters
        ----------
        workers : List[Worker]
            The workers to be checked.

        Returns
        -------
        statuses : List[bool]
            The status for each worker. Return False if the worker has
            stopped or failed, True if the worker is pending start or running.
        """
        raise NotImplementedError
