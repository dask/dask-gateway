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
    def prepare(self):
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json_data = json.loads(self.request.body)
        else:
            self.json_data = None

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
    def gateway(self):
        return self.settings.get('gateway')

    def get_current_user(self):
        cookie = self.get_secure_cookie(
            DASK_GATEWAY_COOKIE,
            max_age_days=self.cookie_max_age_days
        )
        if cookie is not None:
            cookie = cookie.decode('utf-8', 'replace')
            user = self.gateway.user_from_cookie(cookie)
            if user is not None:
                return user.name
            self.clear_cookie(DASK_GATEWAY_COOKIE)
        elif self.get_cookie(DASK_GATEWAY_COOKIE):
            self.clear_cookie(DASK_GATEWAY_COOKIE)

        username = self.authenticator.authenticate(self)
        user = self.gateway.get_or_create_user(username)
        self.set_secure_cookie(
            DASK_GATEWAY_COOKIE,
            user.cookie,
            expires_days=self.cookie_max_age_days
        )
        # cache user object for use in response
        self.dask_user = user
        return user.name


def cluster_model(cluster):
    return {'cluster_id': cluster.cluster_id,
            'user': cluster.user.name}


class ClustersHandler(BaseHandler):
    @authenticated
    async def post(self, cluster_id):
        # Only accept urls of the form /api/clusters/
        if cluster_id:
            raise web.HTTPError(405)

        cluster = self.gateway.create_cluster(self.dask_user)

        # First initialize the cluster manager to check that the cluster
        # request is valid.
        try:
            cluster.manager.initialize(self.json_data)
        except Exception as exc:
            self.log.warning("Invalid cluster request:", exc_info=exc)
            raise web.HTTPError(400, reason=str(exc))

        # The cluster is valid, launch the start task to run in the background
        self.gateway.start_cluster(cluster)

        # Return the cluster id, to be used in future requests
        self.write({'cluster_id': cluster.cluster_id})
        self.set_status(201)

    @authenticated
    async def get(self, cluster_id):
        if not cluster_id:
            out = {k: cluster_model(v) for k, v in self.dask_user.clusters.items()}
            self.write(out)
            return

        cluster = self.dask_user.clusters.get(cluster_id)
        if cluster is None:
            raise web.HTTPError(404, reason="Cluster %s does not exist" % cluster_id)

        self.write(cluster_model(cluster))

    @authenticated
    async def delete(self, cluster_id):
        # Only accept urls of the form /api/clusters/{cluster_id}
        if not cluster_id:
            raise web.HTTPError(405)

        if cluster_id in self.dask_user.clusters:
            cluster = self.dask_user.clusters[cluster_id]
            self.gateway.stop_cluster(cluster)
        self.set_status(204)


default_handlers = [
    ("/api/clusters/([a-zA-Z0-9]*)", ClustersHandler)
]
