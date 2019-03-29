import asyncio
from sqlalchemy import (MetaData, Table, Column, Integer, Unicode, ForeignKey,
                        LargeBinary, Enum, create_engine)
from sqlalchemy.pool import StaticPool


metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', Unicode(255), nullable=False, unique=True),
    Column('cookie', Unicode(32), nullable=False, unique=True)
)

clusters = Table(
    'clusters',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', Unicode(255), nullable=False, unique=True),
    Column('user_id', Integer, ForeignKey("users.id", ondelete="CASCADE"),
           nullable=False),
    Column('state', LargeBinary, nullable=False),
    Column('token', Unicode(32), nullable=False, unique=True),
    Column('scheduler_address', Unicode(255), nullable=False),
    Column('dashboard_address', Unicode(255), nullable=False),
    Column('api_address', Unicode(255), nullable=False),
    Column('tls_cert', LargeBinary, nullable=False),
    Column('tls_key', LargeBinary, nullable=False),
    Column('requested_workers', Integer, nullable=False)
)

workers = Table(
    'workers',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', Unicode(255), nullable=False),
    Column('cluster_id', ForeignKey('clusters.id', ondelete="CASCADE"), nullable=False),
    Column('status', Enum('PENDING', 'RUNNING'), nullable=False)
)


def make_engine(url="sqlite:///:memory:", **kwargs):
    if url.startswith('sqlite'):
        kwargs['connect_args'] = {'check_same_thread': False}

    if url.endswith(':memory:'):
        kwargs['poolclass'] = StaticPool

    engine = create_engine(url, **kwargs)

    metadata.create_all(engine)

    return engine


class User(object):
    def __init__(self, id=None, name=None, cookie=None):
        self.id = id
        self.name = name
        self.cookie = cookie
        self.clusters = {}


class Cluster(object):
    def __init__(self, id=None, name=None, user=None, token=None,
                 manager=None, scheduler_address='', dashboard_address='',
                 api_address='', tls_cert=None, tls_key=None,
                 requested_workers=0):
        self.id = id
        self.name = name
        self.user = user
        self.token = token
        self.manager = manager
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.requested_workers = requested_workers
        self.workers = {}
        self.lock = asyncio.Lock()


class Worker(object):
    def __init__(self, id=None, name=None, cluster=None, status=None):
        self.id = id
        self.name = name
        self.cluster = cluster
        self.status = status
