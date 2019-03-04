import functools
import json

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


class BaseHandler(web.RequestHandler):
    @property
    def authenticator(self):
        return self.settings.get('authenticator')

    @property
    def cluster_class(self):
        return self.settings.get('cluster_class')

    @property
    def cookie_max_age_days(self):
        return self.settings.get('cookie_max_age_days')

    @property
    def log(self):
        return self.settings.get('log', app_log)

    @property
    def app_state(self):
        return self.settings.get('app_state')

    def get_current_user(self):
        cookie = self.get_secure_cookie(
            DASK_GATEWAY_COOKIE,
            max_age_days=self.cookie_max_age_days
        )
        if cookie is not None:
            cookie = cookie.decode('utf-8', 'replace')
            user = self.app_state.user_from_cookie(cookie)
            if user is not None:
                return user.name
            self.clear_cookie(DASK_GATEWAY_COOKIE)
        elif self.get_cookie(DASK_GATEWAY_COOKIE):
            self.clear_cookie(DASK_GATEWAY_COOKIE)

        username = self.authenticator.authenticate(self)
        user = self.app_state.get_or_create_user(username)
        self.set_secure_cookie(
            DASK_GATEWAY_COOKIE,
            user.cookie,
            expires_days=self.cookie_max_age_days
        )
        # cache user object for use in response
        self.dask_gateway_user = user
        return user.name


class RootHandler(BaseHandler):
    @authenticated
    def get(self):
        self.write("Hello %s!" % self.current_user)


class ClustersHandler(BaseHandler):
    def prepare(self):
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json = json.loads(self.request.body)
        else:
            self.json = None

    @authenticated
    async def post(self):
        self.write("Hello %s!" % self.dask_gateway_user.name)


default_handlers = [
    ("/", RootHandler),
    ("/api/clusters", ClustersHandler)
]
