import asyncio
import base64
import json
import os
import time
import uuid
from collections import defaultdict
from itertools import chain, islice

import sqlalchemy as sa
from traitlets import Unicode, Bool, List, Integer, Float, validate, default
from cryptography.fernet import MultiFernet, Fernet

from .base import Backend
from .. import models
from ..tls import new_keypair
from ..utils import FrozenAttrDict


def timestamp():
    """An integer timestamp represented as milliseconds since the epoch UTC"""
    return int(time.time() * 1000)


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


class Cluster(object):
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
        self.workers = {}

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

    def worker_targets_before(self, target):
        return [w for w in self.workers.values() if w.target < target]

    @property
    def model_status(self):
        return self._status_map[self.status, self.target]

    def to_model(self):
        return models.Cluster(
            name=self.name,
            username=self.username,
            token=self.token,
            options=self.options,
            status=self.model_status,
            scheduler_address=self.scheduler_address,
            dashboard_address=self.dashboard_address,
            api_address=self.api_address,
            tls_cert=self.tls_cert,
            tls_key=self.tls_key,
            start_time=self.start_time,
            stop_time=self.stop_time,
        )


class Worker(object):
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


class DataManager(object):
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
        self.token_to_cluster = {}
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
            self.token_to_cluster[cluster.token] = cluster
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
                    del self.token_to_cluster[cluster.token]
                    del self.name_to_cluster[cluster.name]
                    del cluster.user.clusters[cluster.name]

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

    def cluster_from_token(self, token):
        """Lookup a cluster from a token"""
        return self.token_to_cluster.get(token)

    def cluster_from_name(self, name):
        """Lookup a cluster by name"""
        return self.name_to_cluster.get(name)

    def active_clusters(self):
        for user in self.username_to_user.values():
            for cluster in user.clusters.values():
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
            self.token_to_cluster[token] = cluster
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
        if len(updates) == 1:
            w, kwargs = updates[0]
            self.update_worker(w, **kwargs)
            return
        with self.db.begin() as conn:
            conn.execute(
                clusters.update().where(clusters.c.id == sa.bindparam("_id")),
                [{"_id": w.id, **u} for w, u in updates],
            )
            for w, u in updates:
                for k, v in u.items():
                    setattr(w, k, v)


class UniqueQueue(asyncio.Queue):
    """A queue that may only contain each item once."""

    def __init__(self, maxsize=0, *, loop=None):
        super().__init__(maxsize=maxsize, loop=loop)
        self._items = set()

    def _put(self, item):
        if item not in self._items:
            self._items.add(item)
            super()._put(item)

    def _get(self):
        item = super()._get()
        self._items.discard(item)
        return item


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
        return [_normalize_encrypt_key(k) for k in keys.split(b";") if k.strip()]

    @validate("db_encrypt_keys")
    def _db_encrypt_keys_validate(self, proposal):
        if not proposal.value and not _is_in_memory_db(self.db_url):
            raise ValueError(
                "Must configure `db_encrypt_keys`/`DASK_GATEWAY_ENCRYPT_KEYS` "
                "when not using an in-memory database"
            )
        return [_normalize_encrypt_key(k) for k in proposal.value]

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

    parallelism = Integer(
        20,
        help="""
        Number of handlers to use for starting/stopping clusters.
        """,
        config=True,
    )

    async def setup(self, app):
        await super().setup(app)
        self.db = DataManager(
            url=self.db_url, echo=self.db_debug, encrypt_keys=self.db_encrypt_keys
        )
        self.queues = [UniqueQueue() for _ in range(self.parallelism)]
        self.tasks = [
            asyncio.ensure_future(self.reconciler_loop(q)) for q in self.queues
        ]
        # Further backend-specific setup
        await self.handle_setup()

    async def cleanup(self):
        await self.handle_cleanup()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await super().cleanup()

    async def list_clusters(self, user=None, statuses=None):
        clusters = self.db.list_clusters(username=user.name, statuses=statuses)
        return [c.to_model() for c in clusters]

    async def get_cluster(self, cluster_name):
        cluster = self.db.get_cluster(cluster_name)
        return None if cluster is None else cluster.to_model()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)
        cluster = self.db.create_cluster(user.name, options, config)
        await self.enqueue(cluster)
        return cluster.name

    async def stop_cluster(self, cluster_name, failed=False):
        cluster = self.db.get_cluster(cluster_name)
        if cluster is None:
            return
        if cluster.target <= JobStatus.RUNNING:
            target = JobStatus.FAILED if failed else JobStatus.STOPPED
            self.db.update_cluster(cluster, target=target)
            await self.enqueue(cluster)

    async def on_cluster_heartbeat(self, cluster_name, msg):
        cluster = self.db.get_cluster(cluster_name)
        if cluster is None:
            return

        if cluster.target > JobStatus.RUNNING:
            return

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

        if count != cluster.count:
            cluster_update["count"] = count

        created_workers = []
        submitted_workers = []
        worker_updates = []
        for worker in cluster.workers.values():
            if worker.status >= JobStatus.STOPPED:
                continue

            update = {}
            if worker.name in closing_workers:
                update["close_expected"] = True
                if worker.status != JobStatus.RUNNING:
                    update["status"] = JobStatus.RUNNING
            elif worker.name in active_workers:
                if worker.status < JobStatus.RUNNING:
                    update["status"] = JobStatus.RUNNING
            elif worker.name in closed_workers:
                if worker.close_expected:
                    update["target"] = JobStatus.STOPPED
                else:
                    update["target"] = JobStatus.FAILED
            else:
                if worker.status == JobStatus.SUBMITTED:
                    submitted_workers.append(worker)
                else:
                    assert worker.status == JobStatus.CREATED
                    created_workers.append(worker)

            if update:
                worker_updates.append((worker, update))

        n_pending = len(created_workers) + len(submitted_workers)
        n_to_stop = len(active_workers) + n_pending - count
        if n_to_stop > 0:
            for w in islice(chain(created_workers, submitted_workers), n_to_stop):
                worker_updates.append((w, {"target": JobStatus.STOPPED}))

        if cluster_update:
            self.db.update_cluster(cluster, **cluster_update)
            await self.enqueue(cluster)

        if worker_updates:
            self.db.update_workers(worker_updates)
            for w, u in worker_updates:
                if "target" in u:
                    await self.enqueue(w)

    async def enqueue(self, obj):
        ind = hash(obj) % self.parallelism
        await self.queues[ind].put(obj)

    async def reconciler_loop(self, queue):
        while True:
            obj = await queue.get()
            try:
                await self.reconcile(obj)
            except Exception:
                self.log.warning("Failure in reconciler loop", exc_info=True)

    async def reconcile(self, obj):
        if isinstance(obj, Cluster):
            await self.reconcile_cluster(obj)
        else:
            await self.reconcile_worker(obj)

    async def reconcile_cluster(self, cluster):
        self.log.debug(
            "Handling [cluster: %s, status: %s, target: %s]",
            cluster.name,
            cluster.status,
            cluster.target,
        )

        if cluster.status >= JobStatus.STOPPED:
            return

        if cluster.target in (JobStatus.STOPPED, JobStatus.FAILED):
            if cluster.status == JobStatus.CLOSING:
                if self.is_cluster_ready_to_close(cluster):
                    await self._cluster_to_stopped(cluster)
                return
            else:
                target = (
                    JobStatus.CLOSING
                    if self.supports_bulk_shutdown
                    else JobStatus.STOPPED
                )
                workers = cluster.worker_targets_before(target)
                if workers:
                    self.db.update_workers([(w, {"target": target}) for w in workers])
                    for w in workers:
                        await self.enqueue(w)
                self.db.update_cluster(cluster, status=JobStatus.CLOSING)
                await self.enqueue(cluster)

        if cluster.target == JobStatus.RUNNING:
            if cluster.status == JobStatus.CREATED:
                await self._cluster_to_submitted(cluster)
                return

            if cluster.status == JobStatus.SUBMITTED and cluster.scheduler_address:
                await self._cluster_to_running(cluster)
            await self._check_cluster_scale(cluster)

    async def reconcile_worker(self, worker):
        self.log.debug(
            "Handling [worker: %s, cluster: %s, status: %s, target: %s]",
            worker.name,
            worker.cluster.name,
            worker.status,
            worker.target,
        )

        if worker.status >= JobStatus.STOPPED:
            return

        if worker.target == JobStatus.CLOSING:
            if worker.status != JobStatus.CLOSING:
                self.db.update_worker(worker, status=JobStatus.CLOSING)
            if self.is_cluster_ready_to_close(worker.cluster):
                await self.enqueue(worker.cluster)
            return

        if worker.target in (JobStatus.STOPPED, JobStatus.FAILED):
            await self._worker_to_stopped(worker)
            if self.is_cluster_ready_to_close(worker.cluster):
                await self.enqueue(worker.cluster)
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
        try:
            self.log.info(
                "Starting cluster %s for user %s...", cluster.name, cluster.username
            )

            # Walk through the startup process, saving state as updates occur
            async for state in self.handle_cluster_start(cluster):
                self.log.debug("State update for cluster %s", cluster.name)
                self.db.update_cluster(cluster, state=state)

            # Move cluster to submitted
            self.db.update_cluster(cluster, status=JobStatus.SUBMITTED)
        except Exception as exc:
            self.log.warning("Failed to submit cluster %s", cluster.name, exc_info=exc)
            self.db.update_cluster(cluster, target=JobStatus.FAILED)
            await self.enqueue(cluster)

    async def _cluster_to_stopped(self, cluster):
        self.log.info("Stopping cluster %s...", cluster.name)
        if cluster.status > JobStatus.CREATED:
            try:
                await self.handle_cluster_stop(cluster)
            except Exception as exc:
                self.log.warning(
                    "Failed to stop cluster %s", cluster.name, exc_info=exc
                )
            # TODO: prefix here
            await self.web_proxy.delete_route("/gateway/clusters/" + cluster.name)
            await self.scheduler_proxy.delete_route("/" + cluster.name)
        self.log.info("Cluster %s stopped", cluster.name)
        self.db.update_cluster(cluster, status=cluster.target)
        # TODO: update all workers to stopped as well

    async def _cluster_to_running(self, cluster):
        self.log.info("Cluster %s transitioning to RUNNING", cluster.name)
        if cluster.dashboard_address:
            # TODO: prefix here
            await self.web_proxy.add_route(
                "/gateway/clusters/" + cluster.name, cluster.dashboard_address
            )
        await self.scheduler_proxy.add_route(
            "/" + cluster.name, cluster.scheduler_address
        )
        self.db.update_cluster(cluster, status=JobStatus.RUNNING)

    async def _check_cluster_scale(self, cluster):
        active = cluster.active_workers()
        if cluster.count > len(active):
            for _ in range(cluster.count - len(active)):
                worker = self.db.create_worker(cluster)
                await self.enqueue(worker)

    async def _worker_to_submitted(self, worker):
        try:
            self.log.info(
                "Starting worker %s for cluster %s...", worker.name, worker.cluster.name
            )

            # Walk through the startup process, saving state as updates occur
            async for state in self.handle_worker_start(worker):
                self.log.debug("State update for worker %s", worker.name)
                self.db.update_worker(worker, state=state)

            # Move worker to submitted
            self.db.update_worker(worker, status=JobStatus.SUBMITTED)
        except Exception as exc:
            self.log.warning("Failed to submit worker %s", worker.name, exc_info=exc)
            self.db.update_worker(worker, target=JobStatus.FAILED)
            await self.enqueue(worker)

    async def _worker_to_stopped(self, worker):
        self.log.info("Stopping worker %s...", worker.name)
        if worker.status > JobStatus.CREATED:
            try:
                await self.handle_worker_stop(worker)
            except Exception as exc:
                self.log.warning("Failed to stop worker %s", worker.name, exc_info=exc)
        self.log.info("Worker %s stopped", worker.name)
        self.db.update_worker(worker, status=worker.target)

    def get_env(self, cluster):
        """Get a dict of environment variables to set for the process"""
        out = dict(cluster.config.environment)
        tls_cert_path, tls_key_path = self.get_tls_paths(cluster)
        # Set values that dask-gateway needs to run
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_API_TOKEN": cluster.token,
                "DASK_GATEWAY_CLUSTER_NAME": cluster.name,
                "DASK_GATEWAY_TLS_CERT": tls_cert_path,
                "DASK_GATEWAY_TLS_KEY": tls_key_path,
            }
        )
        return out

    def get_scheduler_command(self, cluster):
        return [cluster.config.scheduler_cmd]

    def get_worker_command(self, cluster):
        return [cluster.config.worker_cmd]

    # Subclasses should implement these methods
    async def handle_setup(self):
        pass

    async def handle_cleanup(self):
        pass

    supports_bulk_shutdown = False

    async def handle_cluster_start(self, cluster):
        raise NotImplementedError

    async def handle_cluster_stop(self, cluster):
        raise NotImplementedError

    async def handle_cluster_status(self, cluster):
        raise NotImplementedError

    async def handle_worker_start(self, worker):
        raise NotImplementedError

    async def handle_worker_stop(self, worker):
        raise NotImplementedError

    async def handle_worker_status(self, worker):
        raise NotImplementedError
