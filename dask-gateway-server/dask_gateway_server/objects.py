import asyncio
import enum
import json
import uuid

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    Unicode,
    ForeignKey,
    LargeBinary,
    TypeDecorator,
    create_engine,
)
from sqlalchemy.pool import StaticPool

from .tls import new_keypair


class _EnumMixin(object):
    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError("%r is not a valid %s" % (name, cls.__name__))


class ClusterStatus(_EnumMixin, enum.IntEnum):
    STARTING = 1
    STARTED = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    FAILED = 6


class WorkerStatus(_EnumMixin, enum.IntEnum):
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
    Column("state", JSON, nullable=False),
    Column("token", Unicode(32), nullable=False, unique=True),
    Column("scheduler_address", Unicode(255), nullable=False),
    Column("dashboard_address", Unicode(255), nullable=False),
    Column("api_address", Unicode(255), nullable=False),
    Column("tls_cert", LargeBinary, nullable=False),
    Column("tls_key", LargeBinary, nullable=False),
)

workers = Table(
    "workers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Unicode(255), nullable=False),
    Column("cluster_id", ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False),
    Column("status", IntEnum(WorkerStatus), nullable=False),
    Column("state", JSON, nullable=False),
)


class DataManager(object):
    """Holds the internal state for a single Dask Gateway.

    Keeps the memory representation in-sync with the database.
    """

    def __init__(self, url="sqlite:///:memory:", **kwargs):
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}

        if url.endswith(":memory:"):
            kwargs["poolclass"] = StaticPool

        engine = create_engine(url, **kwargs)

        metadata.create_all(engine)

        self.db = engine

        self.username_to_user = {}
        self.cookie_to_user = {}
        self.token_to_cluster = {}

        # Load state from database
        self._init_from_database()

    def _init_from_database(self):
        # Load all existing users into memory
        id_to_user = {}
        for u in self.db.execute(users.select()):
            user = User(id=u.id, name=u.name, cookie=u.cookie)
            self.username_to_user[user.name] = user
            self.cookie_to_user[user.cookie] = user
            id_to_user[user.id] = user

        # Next load all existing clusters into memory
        id_to_cluster = {}
        for c in self.db.execute(clusters.select()):
            user = id_to_user[c.user_id]
            cluster = Cluster(
                id=c.id,
                name=c.name,
                user=user,
                token=c.token,
                status=c.status,
                state=c.state,
                scheduler_address=c.scheduler_address,
                dashboard_address=c.dashboard_address,
                api_address=c.api_address,
                tls_cert=c.tls_cert,
                tls_key=c.tls_key,
            )
            user.clusters[cluster.name] = cluster
            self.token_to_cluster[cluster.token] = cluster
            id_to_cluster[cluster.id] = cluster

        # Next load all existing workers into memory
        for w in self.db.execute(workers.select()):
            cluster = id_to_cluster[w.cluster_id]
            worker = Worker(
                id=w.id, name=w.name, status=w.status, cluster=cluster, state=w.state
            )
            cluster.workers[worker.name] = worker
            if w.status == WorkerStatus.STARTING:
                cluster.pending.add(worker.name)

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

    def active_clusters(self):
        for user in self.username_to_user.values():
            for cluster in user.clusters.values():
                if cluster.is_active():
                    yield cluster

    def create_cluster(self, user):
        """Create a new cluster for a user"""
        cluster_name = uuid.uuid4().hex
        token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)

        common = {
            "name": cluster_name,
            "token": token,
            "status": ClusterStatus.STARTING,
            "state": {},
            "scheduler_address": "",
            "dashboard_address": "",
            "api_address": "",
            "tls_cert": tls_cert,
            "tls_key": tls_key,
        }

        with self.db.begin() as conn:
            res = conn.execute(clusters.insert().values(user_id=user.id, **common))
            cluster = Cluster(id=res.inserted_primary_key[0], user=user, **common)
            user.clusters[cluster_name] = cluster
            self.token_to_cluster[token] = cluster

        return cluster

    def create_worker(self, cluster):
        """Create a new worker for a cluster"""
        worker_name = uuid.uuid4().hex

        common = {"name": worker_name, "status": WorkerStatus.STARTING, "state": {}}

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


class ClusterInfo(object):
    """Public Information about a cluster.

    This object is passed to methods in the ``ClusterManager`` interface.
    """

    __slots__ = ("username", "cluster_name", "api_token", "tls_cert", "tls_key")

    def __init__(self, username, cluster_name, api_token, tls_cert, tls_key):
        self.username = username
        self.cluster_name = cluster_name
        self.api_token = api_token
        self.tls_cert = tls_cert
        self.tls_key = tls_key


class Cluster(object):
    def __init__(
        self,
        id=None,
        name=None,
        user=None,
        token=None,
        status=None,
        state=None,
        scheduler_address="",
        dashboard_address="",
        api_address="",
        tls_cert=b"",
        tls_key=b"",
    ):
        self.id = id
        self.name = name
        self.user = user
        self.token = token
        self.status = status
        self.state = state
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.pending = set()
        self.workers = {}

        loop = asyncio.get_running_loop()
        self.lock = asyncio.Lock(loop=loop)
        self._start_future = loop.create_future()
        self._connect_future = loop.create_future()
        if status >= ClusterStatus.RUNNING:
            # Already running, create finished futures to mark
            self._start_future.set_result(True)
            self._connect_future.set_result(None)

    @property
    def active_workers(self):
        return [w for w in self.workers.values() if w.is_active()]

    def is_active(self):
        return self.status < ClusterStatus.STOPPING

    @property
    def info(self):
        return ClusterInfo(
            username=self.user.name,
            cluster_name=self.name,
            api_token=self.token,
            tls_cert=self.tls_cert,
            tls_key=self.tls_key,
        )


class Worker(object):
    def __init__(self, id=None, name=None, cluster=None, status=None, state=None):
        self.id = id
        self.name = name
        self.cluster = cluster
        self.status = status
        self.state = state

        loop = asyncio.get_running_loop()

        self._start_future = loop.create_future()
        self._connect_future = loop.create_future()

        if status >= WorkerStatus.RUNNING:
            # Already running, create finished future to mark
            self._start_future.set_result(True)
            self._connect_future.set_result(None)

    def is_active(self):
        return self.status < WorkerStatus.STOPPING
