import functools

from tornado import web
from tornado.log import app_log

from .database import get_or_create_user, username_from_cookie


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


class BaseHandler(web.RequestHandler):
    @property
    def authenticator(self):
        return self.settings.get('authenticator')

    @property
    def cookie_max_age_days(self):
        return self.settings.get('cookie_max_age_days')

    @property
    def log(self):
        return self.settings.get('log', app_log)

    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = self.settings.get('engine').connect()
        return self._db

    def on_finish(self):
        if hasattr(self, '_db'):
            self._db.close()
        super().on_finish()

    def get_current_user(self):
        cookie = self.get_secure_cookie(
            DASK_GATEWAY_COOKIE,
            max_age_days=self.cookie_max_age_days
        )
        if cookie is not None:
            cookie = cookie.decode('utf-8', 'replace')
            try:
                return username_from_cookie(self.db, cookie)
            except KeyError:
                self.clear_cookie(DASK_GATEWAY_COOKIE)
        elif self.get_cookie(DASK_GATEWAY_COOKIE):
            self.clear_cookie(DASK_GATEWAY_COOKIE)

        username = self.authenticator.authenticate(self)
        user = get_or_create_user(self.db, username)
        self.set_secure_cookie(
            DASK_GATEWAY_COOKIE,
            user.cookie,
            expires_days=self.cookie_max_age_days
        )
        return user.name


class RootHandler(BaseHandler):
    @authenticated
    def get(self):
        self.write("Hello %s!" % self.current_user)


default_handlers = [
    ("/", RootHandler)
]
