import asyncio
import base64
import enum
import json
import time
import uuid

from cryptography.fernet import MultiFernet, Fernet
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Float,
    Unicode,
    BINARY,
    ForeignKey,
    LargeBinary,
    TypeDecorator,
    create_engine,
    bindparam,
    select,
    event,
)
from sqlalchemy.pool import StaticPool

from .compat import get_running_loop
from .tls import new_keypair


def timestamp():
    """An integer timestamp represented as milliseconds since the epoch UTC"""
    return int(time.time() * 1000)


def normalize_encrypt_key(key):
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


class _IntEnum(enum.IntEnum):
    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError("%r is not a valid %s" % (name, cls.__name__))


class ClusterStatus(_IntEnum):
    STARTING = 1
    STARTED = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    FAILED = 6


class WorkerStatus(_IntEnum):
    STARTING = 1
    STARTED = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    FAILED = 6


class IntEnum(TypeDecorator):
    impl = Integer

    def __init__(self, enumclass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumclass = enumclass

    def process_bind_param(self, value, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        return self._enumclass(value)


class JSON(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = LargeBinary

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value).encode("utf-8")
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False, unique=True),
    Column("cookie", Unicode(32), nullable=False, unique=True),
)

clusters = Table(
    "clusters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False, unique=True),
    Column(
        "user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    ),
    Column("status", IntEnum(ClusterStatus), nullable=False),
    Column("options", JSON, nullable=False),
    Column("state", JSON, nullable=False),
    Column("token", BINARY(140), nullable=False, unique=True),
    Column("scheduler_address", Unicode(255), nullable=False),
    Column("dashboard_address", Unicode(255), nullable=False),
    Column("api_address", Unicode(255), nullable=False),
    Column("tls_credentials", LargeBinary, nullable=False),
    Column("memory", Integer, nullable=False),
    Column("cores", Float, nullable=False),
    Column("start_time", Integer, nullable=False),
    Column("stop_time", Integer, nullable=True),
)

workers = Table(
    "workers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False),
    Column("cluster_id", ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False),
    Column("status", IntEnum(WorkerStatus), nullable=False),
    Column("state", JSON, nullable=False),
    Column("memory", Integer, nullable=False),
    Column("cores", Float, nullable=False),
    Column("start_time", Integer, nullable=False),
    Column("stop_time", Integer, nullable=True),
)


def register_foreign_keys(engine):
    """register PRAGMA foreign_keys=on on connection"""

    @event.listens_for(engine, "connect")
    def connect(dbapi_con, con_record):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def is_in_memory_db(url):
    return url in ("sqlite://", "sqlite:///:memory:")


class DataManager(object):
    """Holds the internal state for a single Dask Gateway.

    Keeps the memory representation in-sync with the database.
    """

    def __init__(self, url="sqlite:///:memory:", encrypt_keys=(), **kwargs):
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}

        if is_in_memory_db(url):
            kwargs["poolclass"] = StaticPool
            self.fernet = None
        else:
            self.fernet = MultiFernet([Fernet(key) for key in encrypt_keys])

        engine = create_engine(url, **kwargs)
        if url.startswith("sqlite"):
            register_foreign_keys(engine)

        metadata.create_all(engine)

        self.db = engine

        self.username_to_user = {}
        self.cookie_to_user = {}
        self.token_to_cluster = {}
        self.name_to_cluster = {}
        self.id_to_cluster = {}

    def load_database_state(self):
        # Load all existing users into memory
        id_to_user = {}
        for u in self.db.execute(users.select()):
            user = User(id=u.id, name=u.name, cookie=u.cookie)
            self.username_to_user[user.name] = user
            self.cookie_to_user[user.cookie] = user
            id_to_user[user.id] = user

        # Next load all existing clusters into memory
        for c in self.db.execute(clusters.select()):
            user = id_to_user[c.user_id]
            tls_cert, tls_key = self.decode_tls_credentials(c.tls_credentials)
            token = self.decode_token(c.token)
            cluster = Cluster(
                id=c.id,
                name=c.name,
                user=user,
                token=token,
                options=c.options,
                status=c.status,
                state=c.state,
                scheduler_address=c.scheduler_address,
                dashboard_address=c.dashboard_address,
                api_address=c.api_address,
                tls_cert=tls_cert,
                tls_key=tls_key,
                memory=c.memory,
                cores=c.cores,
                start_time=c.start_time,
                stop_time=c.stop_time,
            )
            self.id_to_cluster[cluster.id] = cluster
            self.token_to_cluster[cluster.token] = cluster
            self.name_to_cluster[cluster.name] = cluster
            user.clusters[cluster.name] = cluster

        # Next load all existing workers into memory
        for w in self.db.execute(workers.select()):
            cluster = self.id_to_cluster[w.cluster_id]
            worker = Worker(
                id=w.id,
                name=w.name,
                status=w.status,
                cluster=cluster,
                state=w.state,
                memory=w.memory,
                cores=w.cores,
                start_time=w.start_time,
                stop_time=w.stop_time,
            )
            cluster.workers[worker.name] = worker
            if w.status == WorkerStatus.STARTING:
                cluster.pending.add(worker.name)

    def cleanup_expired(self, max_age_in_seconds):
        cutoff = timestamp() - max_age_in_seconds * 1000
        with self.db.begin() as conn:
            to_delete = conn.execute(
                select([clusters.c.id]).where(clusters.c.stop_time < cutoff)
            ).fetchall()

            if to_delete:
                to_delete = [i for i, in to_delete]

                conn.execute(
                    clusters.delete().where(clusters.c.id == bindparam("id")),
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

    def user_from_cookie(self, cookie):
        """Lookup a user from a cookie"""
        return self.cookie_to_user.get(cookie)

    def get_or_create_user(self, username):
        """Lookup a user if they exist, otherwise create a new user"""
        user = self.username_to_user.get(username)
        if user is None:
            cookie = uuid.uuid4().hex
            res = self.db.execute(users.insert().values(name=username, cookie=cookie))
            user = User(id=res.inserted_primary_key[0], name=username, cookie=cookie)
            self.cookie_to_user[cookie] = user
            self.username_to_user[username] = user
        return user

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

    def create_cluster(self, user, options, memory, cores):
        """Create a new cluster for a user"""
        cluster_name = uuid.uuid4().hex
        token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)
        # Encode the tls credentials for storing in the database
        tls_credentials = self.encode_tls_credentials(tls_cert, tls_key)
        enc_token = self.encode_token(token)

        common = {
            "name": cluster_name,
            "options": options,
            "status": ClusterStatus.STARTING,
            "state": {},
            "scheduler_address": "",
            "dashboard_address": "",
            "api_address": "",
            "memory": memory,
            "cores": cores,
            "start_time": timestamp(),
        }

        with self.db.begin() as conn:
            res = conn.execute(
                clusters.insert().values(
                    user_id=user.id,
                    tls_credentials=tls_credentials,
                    token=enc_token,
                    **common,
                )
            )
            cluster = Cluster(
                id=res.inserted_primary_key[0],
                user=user,
                token=token,
                tls_cert=tls_cert,
                tls_key=tls_key,
                **common,
            )
            self.id_to_cluster[cluster.id] = cluster
            self.token_to_cluster[token] = cluster
            self.name_to_cluster[cluster_name] = cluster
            user.clusters[cluster_name] = cluster

        return cluster

    def create_worker(self, cluster, memory, cores):
        """Create a new worker for a cluster"""
        worker_name = uuid.uuid4().hex

        common = {
            "name": worker_name,
            "status": WorkerStatus.STARTING,
            "state": {},
            "memory": memory,
            "cores": cores,
            "start_time": timestamp(),
        }

        with self.db.begin() as conn:
            res = conn.execute(workers.insert().values(cluster_id=cluster.id, **common))
            worker = Worker(id=res.inserted_primary_key[0], cluster=cluster, **common)
            cluster.pending.add(worker.name)
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


class User(object):
    def __init__(self, id=None, name=None, cookie=None):
        self.id = id
        self.name = name
        self.cookie = cookie
        self.clusters = {}

    def active_clusters(self):
        for cluster in self.clusters.values():
            if cluster.is_active():
                yield cluster


class Cluster(object):
    def __init__(
        self,
        id=None,
        name=None,
        user=None,
        token=None,
        options=None,
        status=None,
        state=None,
        scheduler_address="",
        dashboard_address="",
        api_address="",
        tls_cert=b"",
        tls_key=b"",
        memory=None,
        cores=None,
        start_time=None,
        stop_time=None,
    ):
        self.id = id
        self.name = name
        self.user = user
        self.token = token
        self.options = options
        self.status = status
        self.state = state
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.memory = memory
        self.cores = cores
        self.start_time = start_time
        self.stop_time = stop_time

        self.pending = set()
        self.workers = {}

        # The cluster manager instance
        self.manager = None

        loop = get_running_loop()
        self.lock = asyncio.Lock(loop=loop)
        self._start_future = loop.create_future()
        self._connect_future = loop.create_future()
        self._status_monitor = None
        if status >= ClusterStatus.RUNNING:
            # Already running, create finished futures to mark
            self._start_future.set_result(True)
            self._connect_future.set_result(None)

    def active_workers(self):
        return [w for w in self.workers.values() if w.is_active()]

    def is_active(self):
        return self.status < ClusterStatus.STOPPING


class Worker(object):
    def __init__(
        self,
        id=None,
        name=None,
        cluster=None,
        status=None,
        state=None,
        memory=None,
        cores=None,
        start_time=None,
        stop_time=None,
    ):
        self.id = id
        self.name = name
        self.cluster = cluster
        self.status = status
        self.state = state
        self.memory = memory
        self.cores = cores
        self.start_time = start_time
        self.stop_time = stop_time

        loop = get_running_loop()

        self._start_future = loop.create_future()
        self._connect_future = loop.create_future()

        if status >= WorkerStatus.RUNNING:
            # Already running, create finished future to mark
            self._start_future.set_result(True)
            self._connect_future.set_result(None)

    def is_active(self):
        return self.status < WorkerStatus.STOPPING
