import uuid
import functools

from tornado import web
from tornado.log import app_log


DASK_GATEWAY_COOKIE = 'dask-gateway'


def authenticated(method):
    """Ensure this method is authenticated"""
    @functools.wraps(method)
    def inner(self, *args, **kwargs):
        # Trigger authentication mechanism
        if self.current_user is None:
            raise web.HTTPError(403)
        return method(self, *args, **kwargs)
    return inner


class CookieStore(object):
    """TODO: replace with database later."""
    def __init__(self):
        self.db = {}

    def lookup_cookie(self, cookie):
        return self.db.get(cookie, None)

    def new_cookie(self, user):
        cookie = uuid.uuid4().hex
        self.db[cookie] = user
        return cookie

    def delete_cookie(self, cookie):
        return self.db.pop(cookie, None) is not None


class BaseHandler(web.RequestHandler):
    cookie_store = CookieStore()

    @property
    def authenticator(self):
        return self.settings.get('authenticator')

    @property
    def cookie_max_age_days(self):
        return self.settings.get('cookie_max_age_days')

    @property
    def log(self):
        return self.settings.get('log', app_log)

    def get_current_user(self):
        cookie = self.get_secure_cookie(
            DASK_GATEWAY_COOKIE,
            max_age_days=self.cookie_max_age_days
        )
        if cookie is not None:
            cookie = cookie.decode('utf-8', 'replace')
            user = self.cookie_store.lookup_cookie(cookie)
            if user is not None:
                return user
            else:
                self.clear_cookie(DASK_GATEWAY_COOKIE)
        elif self.get_cookie(DASK_GATEWAY_COOKIE):
            self.clear_cookie(DASK_GATEWAY_COOKIE)

        user = self.authenticator.authenticate(self)
        cookie = self.cookie_store.new_cookie(user)
        self.set_secure_cookie(
            DASK_GATEWAY_COOKIE,
            cookie,
            expires_days=self.cookie_max_age_days
        )
        return user


class RootHandler(BaseHandler):
    @authenticated
    def get(self):
        self.write("Hello %s!" % self.current_user)


default_handlers = [
    ("/", RootHandler)
]
