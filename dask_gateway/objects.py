from sqlalchemy import (MetaData, Table, Column, Integer, Unicode, ForeignKey,
                        LargeBinary, create_engine)
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
    Column('cluster_id', Unicode(255), nullable=False, unique=True),
    Column('user_id', Integer, ForeignKey("users.id"), nullable=False),
    Column('state', LargeBinary, nullable=False)
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
    def __init__(self, id=None, cluster_id=None, user=None, manager=None):
        self.id = id
        self.cluster_id = cluster_id
        self.user = user
        self.manager = manager
