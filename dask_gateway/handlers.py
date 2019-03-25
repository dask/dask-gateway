import functools
import json
from urllib.parse import urlparse

from tornado import web
from tornado.log import app_log


DASK_GATEWAY_COOKIE = 'dask-gateway'


def user_authenticated(method):
    """Ensure this method is authenticated via a user"""
    @functools.wraps(method)
    def inner(self, *args, **kwargs):
        # Trigger authentication mechanism
        if self.current_user is None:
            raise web.HTTPError(401)
        return method(self, *args, **kwargs)
    return inner


def token_authenticated(method):
    """Ensure this method is authenticated via a token"""
    @functools.wraps(method)
    def inner(self, *args, **kwargs):
        username = self.get_current_user_from_token()
        if username is None:
            raise web.HTTPError(401)
        self.current_user = username
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

    def get_current_user_from_token(self):
        auth_header = self.request.headers.get('Authorization')
        if auth_header:
            auth_type, auth_key = auth_header.split(" ", 1)
            if auth_type == 'token':
                cluster = self.gateway.cluster_from_token(auth_key)
                if cluster is not None:
                    self.dask_cluster = cluster
                    self.dask_user = cluster.user
                    return cluster.user.name
        return None

    def get_current_user(self):
        cookie = self.get_secure_cookie(
            DASK_GATEWAY_COOKIE,
            max_age_days=self.cookie_max_age_days
        )
        if cookie is not None:
            cookie = cookie.decode('utf-8', 'replace')
            user = self.gateway.user_from_cookie(cookie)
            if user is not None:
                self.dask_cluster = None
                self.dask_user = user
                return user.name
            self.clear_cookie(DASK_GATEWAY_COOKIE)
        elif self.get_cookie(DASK_GATEWAY_COOKIE):
            self.clear_cookie(DASK_GATEWAY_COOKIE)

        # Finally, fall back to using the authenticator
        username = self.authenticator.authenticate(self)
        user = self.gateway.get_or_create_user(username)
        self.set_secure_cookie(
            DASK_GATEWAY_COOKIE,
            user.cookie,
            expires_days=self.cookie_max_age_days
        )
        self.dask_cluster = None
        self.dask_user = user
        return user.name


def cluster_model(gateway, cluster, full=True):
    if cluster.scheduler_address:
        scheduler = 'gateway://%s/%s' % (urlparse(gateway.gateway_url).netloc,
                                         cluster.name)
        dashboard = ('%s/gateway/clusters/%s' % (gateway.public_url, cluster.name))
    else:
        scheduler = dashboard = ''
    out = {'cluster_name': cluster.name,
           'scheduler_address': scheduler,
           'dashboard_address': dashboard}
    if full:
        out['tls_cert'] = cluster.tls_cert.decode()
        out['tls_key'] = cluster.tls_key.decode()
    return out


class ClustersHandler(BaseHandler):
    @user_authenticated
    async def post(self, cluster_name):
        # Only accept urls of the form /api/clusters/
        if cluster_name:
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
        self.write({'cluster_name': cluster.name})
        self.set_status(201)

    @user_authenticated
    async def get(self, cluster_name):
        if not cluster_name:
            out = {k: cluster_model(self.gateway, v, full=False)
                   for k, v in self.dask_user.clusters.items()}
            self.write(out)
            return

        cluster = self.dask_user.clusters.get(cluster_name)
        if cluster is None:
            raise web.HTTPError(404, reason="Cluster %s does not exist" % cluster_name)

        self.write(cluster_model(self.gateway, cluster, full=True))

    @user_authenticated
    async def delete(self, cluster_name):
        # Only accept urls of the form /api/clusters/{cluster_name}
        if not cluster_name:
            raise web.HTTPError(405)

        if cluster_name in self.dask_user.clusters:
            cluster = self.dask_user.clusters[cluster_name]
            self.gateway.stop_cluster(cluster)
        self.set_status(204)


class ClusterRegistrationHandler(BaseHandler):
    def check_cluster(self, cluster_name):
        # only authorize access to the cluster the token is associated with
        if self.dask_cluster.name != cluster_name:
            raise web.HTTPError(403)

    @token_authenticated
    async def put(self, cluster_name):
        self.check_cluster(cluster_name)
        try:
            scheduler = self.json_data['scheduler_address']
            dashboard = self.json_data['dashboard_address']
        except (TypeError, KeyError):
            raise web.HTTPError(405)
        await self.gateway.register_cluster(
            self.dask_cluster, scheduler, dashboard
        )

    @token_authenticated
    async def get(self, cluster_name):
        self.check_cluster(cluster_name)
        msg = {'scheduler_address': self.dask_cluster.scheduler_address,
               'dashboard_address': self.dask_cluster.dashboard_address}
        self.write(msg)


default_handlers = [
    ("/api/clusters/([a-zA-Z0-9]*)", ClustersHandler),
    ("/api/clusters/([a-zA-Z0-9]*)/addresses", ClusterRegistrationHandler)
]
