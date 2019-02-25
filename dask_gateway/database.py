import uuid
from collections import namedtuple

from sqlalchemy import MetaData, Table, Column, Integer, Unicode, create_engine
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool


metadata = MetaData()


def new_cookie():
    """Create a new cookie"""
    return uuid.uuid4().hex


users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', Unicode(255), nullable=False, unique=True),
    Column('cookie', Unicode(32), default=new_cookie, nullable=False, unique=True)
)


User = namedtuple('User', ['name', 'cookie'])


_cookie_cache = {}


def username_from_cookie(db, cookie):
    """Get a username from a cookie, or raise KeyError if no match"""
    if cookie not in _cookie_cache:
        out = db.execute(
            select([users.c.name]).where(users.c.cookie == cookie)
        ).first()
        if out is None:
            raise KeyError(cookie)
        _cookie_cache[cookie] = out.name
    return _cookie_cache[cookie]


def get_or_create_user(db, username):
    """Returns a user object from a username, or creates a new one if no
    user exists with that name"""
    cookie = new_cookie()
    stmt = users.insert().values(name=username, cookie=cookie)
    try:
        db.execute(stmt)
        _cookie_cache[cookie] = username
    except IntegrityError:
        out = db.execute(
            select([users]).where(users.c.name == username)
        ).first()
        cookie = out.cookie
    return User(name=username, cookie=cookie)


def make_engine(url="sqlite:///:memory:", **kwargs):
    if url.startswith('sqlite'):
        kwargs['connect_args'] = {'check_same_thread': False}

    if url.endswith(':memory:'):
        kwargs['poolclass'] = StaticPool

    engine = create_engine(url, **kwargs)

    metadata.create_all(engine)

    return engine
