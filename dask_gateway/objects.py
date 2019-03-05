import uuid

from sqlalchemy import MetaData, Table, Column, Integer, Unicode, create_engine
from sqlalchemy.pool import StaticPool


metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', Unicode(255), nullable=False, unique=True),
    Column('cookie', Unicode(32), nullable=False, unique=True)
)


def new_cookie():
    """Create a new cookie"""
    return uuid.uuid4().hex


def make_engine(url="sqlite:///:memory:", **kwargs):
    if url.startswith('sqlite'):
        kwargs['connect_args'] = {'check_same_thread': False}

    if url.endswith(':memory:'):
        kwargs['poolclass'] = StaticPool

    engine = create_engine(url, **kwargs)

    metadata.create_all(engine)

    return engine


class User(object):
    def __init__(self, name, cookie=None, clusters=()):
        self.name = name
        self.cookie = cookie
        self.clusters = list(clusters)


class StateManager(object):
    """Manages consistency between in-memory and database state"""
    def __init__(self, db_url=None, db_debug=False):
        self.db = make_engine(url=db_url, echo=db_debug)
        self.username_to_user = {}
        self.cookie_to_user = {}

        self.load_from_database()

    def load_from_database(self):
        # Load all existing users into memory
        for dbuser in self.db.execute(users.select()):
            user = User(name=dbuser.name, cookie=dbuser.cookie)
            self.username_to_user[user.name] = user
            self.cookie_to_user[user.cookie] = user

    def user_from_cookie(self, cookie):
        return self.cookie_to_user.get(cookie)

    def get_or_create_user(self, username):
        user = self.username_to_user.get(username)
        if user is None:
            cookie = new_cookie()
            self.db.execute(users.insert().values(name=username, cookie=cookie))
            user = User(name=username, cookie=cookie)
            self.cookie_to_user[cookie] = user
            self.username_to_user[username] = user
        return user
