import asyncio
import enum

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
    Column("state", LargeBinary, nullable=False),
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
    Column("id", Integer, primary_key=True),
    Column("name", Unicode(255), nullable=False),
    Column("cluster_id", ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False),
    Column("status", IntEnum(WorkerStatus), nullable=False),
    Column("state", LargeBinary, nullable=False),
)


def make_engine(url="sqlite:///:memory:", **kwargs):
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}

    if url.endswith(":memory:"):
        kwargs["poolclass"] = StaticPool

    engine = create_engine(url, **kwargs)

    metadata.create_all(engine)

    return engine


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
        active_workers=0,
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
        self.active_workers = active_workers
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
