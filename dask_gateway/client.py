import getpass
import json
import os
import re
import ssl
import tempfile
import weakref
from base64 import b64encode
from datetime import timedelta
from threading import get_ident
from urllib.parse import urlparse

from distributed import Client
from distributed.security import Security
from distributed.utils import (LoopRunner, sync, thread_state, format_bytes,
                               log_errors)
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError
from tornado.httputil import HTTPHeaders

from .cookiejar import CookieJar

# Register gateway protocol
from . import comm
del comm

__all__ = ('Gateway', 'BasicAuth', 'KerberosAuth', 'DaskGatewayCluster')


class GatewayAuth(object):
    def pre_request(self, req, resp):
        pass

    def post_response(self, req, resp, context):
        pass


class BasicAuth(GatewayAuth):
    """Attaches HTTP Basic Authentication to the given Request object."""

    def __init__(self, username=None, password=None):
        if username is None:
            username = getpass.getuser()
        if password is None:
            password = ''
        self._username = username
        self._password = password
        data = b':'.join((self._username.encode('latin1'),
                          self._password.encode('latin1')))
        self._auth = 'Basic ' + b64encode(data).decode()

    def pre_request(self, req, resp):
        req.headers['Authorization'] = self._auth
        return None


class KerberosAuth(GatewayAuth):
    """Authenticate with kerberos"""
    auth_regex = re.compile('(?:.*,)*\s*Negotiate\s*([^,]*),?', re.I)  # noqa

    def pre_request(self, req, resp):
        # TODO: convert errors to some common error class
        import kerberos
        hostname = urlparse(resp.effective_url).hostname
        _, context = kerberos.authGSSClientInit(
            "HTTP@%s" % hostname,
            gssflags=kerberos.GSS_C_MUTUAL_FLAG | kerberos.GSS_C_SEQUENCE_FLAG
        )
        kerberos.authGSSClientStep(context, "")
        response = kerberos.authGSSClientResponse(context)
        req.headers['Authorization'] = "Negotiate " + response
        return context

    def post_response(self, req, resp, context):
        import kerberos
        www_auth = resp.headers.get('www-authenticate', None)
        token = None
        if www_auth:
            match = self.auth_regex.search(www_auth)
            if match:
                token = match.group(1)
        if not token:
            raise ValueError("Kerberos negotiation failed")
        kerberos.authGSSClientStep(context, token)


class GatewaySecurity(Security):
    """A security implementation that temporarily stores credentials on disk.

    The normal ``Security`` class assumes credentials already exist on disk,
    but we our credentials exist only in memory. Since Python's SSLContext
    doesn't support directly loading credentials from memory, we write them
    temporarily to disk when creating the context, then delete them
    immediately.
    """
    def __init__(self, tls_key, tls_cert):
        self.tls_key = tls_key
        self.tls_cert = tls_cert

    def __repr__(self):
        return 'GatewaySecurity<...>'

    def get_connection_args(self, role):
        with tempfile.TemporaryDirectory() as tempdir:
            key_path = os.path.join(tempdir, 'dask.pem')
            cert_path = os.path.join(tempdir, 'dask.crt')
            with open(key_path, 'w') as f:
                f.write(self.tls_key)
            with open(cert_path, 'w') as f:
                f.write(self.tls_cert)
            ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH,
                                             cafile=cert_path)
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = False
            ctx.load_cert_chain(cert_path, key_path)
        return {'ssl_context': ctx}


class Gateway(object):
    """A client for a Dask Gateway Server.

    Parameters
    ----------
    address : str
        The address to the gateway server.
    auth : GatewayAuth, optional
        The authentication method to use.
    asynchronous : bool, optional
        If true, starts the client in asynchronous mode, where it can be used
        in other async code.
    loop : IOLoop, optional
        The IOLoop instance to use. Defaults to the current loop in
        asynchronous mode, otherwise a background loop is started.
    """
    def __init__(self, address=None, auth=None, asynchronous=False, loop=None):
        self.address = address
        self._auth = auth or BasicAuth()
        self._cookie_jar = CookieJar()

        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self._loop_runner.start()
        if self._asynchronous:
            self._started = self._start()
        else:
            self.sync(self._start)

    async def _start(self):
        self._http_client = AsyncHTTPClient()

    def close(self):
        """Close this gateway client"""
        if not self.asynchronous:
            self._loop_runner.stop()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __await__(self):
        return self._started.__await__()

    async def __aenter__(self):
        await self._started
        return self

    async def __aexit__(self, typ, value, traceback):
        await self._close()

    def __repr__(self):
        return 'Gateway<%s>' % self.address

    @property
    def asynchronous(self):
        return (
            self._asynchronous or
            getattr(thread_state, 'asynchronous', False) or
            (hasattr(self.loop, '_thread_identity') and
                self.loop._thread_identity == get_ident())
        )

    def sync(self, func, *args, **kwargs):
        if kwargs.pop('asynchronous', None) or self.asynchronous:
            callback_timeout = kwargs.pop('callback_timeout', None)
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = gen.with_timeout(timedelta(seconds=callback_timeout),
                                          future)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    async def _fetch(self, req, raise_error=True):
        self._cookie_jar.pre_request(req)
        resp = await self._http_client.fetch(req, raise_error=False)
        if resp.code == 401:
            context = self._auth.pre_request(req, resp)
            resp = await self._http_client.fetch(req, raise_error=False)
            self._auth.post_response(req, resp, context)
        self._cookie_jar.post_response(resp)
        if raise_error:
            resp.rethrow()
        return resp

    async def _clusters(self):
        url = "%s/gateway/api/clusters/" % self.address
        req = HTTPRequest(url=url)
        resp = await self._fetch(req)
        return list(json.loads(resp.body))

    def list_clusters(self, **kwargs):
        """List all running clusters for this user."""
        return self.sync(self._clusters, **kwargs)

    async def _submit(self):
        url = "%s/gateway/api/clusters/" % self.address
        req = HTTPRequest(url=url, method="POST", body=json.dumps({}),
                          headers=HTTPHeaders({'Content-type': 'application/json'}))
        resp = await self._fetch(req)
        data = json.loads(resp.body)
        return data['cluster_name']

    def submit(self, **kwargs):
        """Submit a new cluster to be started.

        This returns quickly with a ``cluster_name``, which can later be used
        to connect to the cluster.

        Returns
        -------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._submit, **kwargs)

    async def _cluster_info(self, cluster_name):
        url = "%s/gateway/api/clusters/%s" % (self.address, cluster_name)
        req = HTTPRequest(url=url)
        resp = await self._fetch(req)
        return json.loads(resp.body)

    async def _connect(self, cluster_name):
        while True:
            try:
                info = await self._cluster_info(cluster_name)
            except HTTPError as exc:
                if exc.code == 404:
                    raise Exception("Unknown cluster %r" % cluster_name)
                else:
                    raise
            status = info['status']
            if status == 'RUNNING':
                return DaskGatewayCluster(self, info)
            elif status == 'FAILED':
                raise Exception("Cluster %r failed to start, see logs for "
                                "more information" % cluster_name)
            elif status == 'STOPPED':
                raise Exception("Cluster %r is already stopped" % cluster_name)
            # Not started yet, try again later
            await gen.sleep(2)

    def connect(self, cluster_name, **kwargs):
        """Connect to a submitted cluster.

        Returns
        -------
        cluster : DaskGatewayCluster
        """
        return self.sync(self._connect, cluster_name, **kwargs)

    async def _new_cluster(self, **kwargs):
        cluster_name = await self._submit(**kwargs)
        try:
            return await self._connect(cluster_name)
        except BaseException:
            # Ensure cluster is stopped on error
            await self._stop_cluster(cluster_name)
            raise

    def new_cluster(self, **kwargs):
        """Submit a new cluster to the gateway, and wait for it to be started.

        Same as calling ``submit`` and ``connect`` in one go.

        Returns
        -------
        cluster : DaskGatewayCluster
        """
        return self.sync(self._new_cluster, **kwargs)

    async def _stop_cluster(self, cluster_name):
        url = "%s/gateway/api/clusters/%s" % (self.address, cluster_name)
        req = HTTPRequest(url=url, method="DELETE")
        await self._fetch(req)

    def stop_cluster(self, cluster_name, **kwargs):
        """Stop a cluster.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._stop_cluster, cluster_name, **kwargs)

    async def _scale_cluster(self, cluster_name, n):
        url = "%s/gateway/api/clusters/%s/workers" % (self.address, cluster_name)
        req = HTTPRequest(url=url,
                          method="PUT",
                          body=json.dumps({'worker_count': n}),
                          headers=HTTPHeaders({'Content-type': 'application/json'}))
        try:
            await self._fetch(req)
        except HTTPError as exc:
            if exc.code == 409:
                raise Exception("Cluster %r is not running" % cluster_name)
            raise

    def scale_cluster(self, cluster_name, n, **kwargs):
        """Scale a cluster to n workers.

        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
        return self.sync(self._scale_cluster, cluster_name, n, **kwargs)


_widget_status_template = """
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table style="text-align: right;">
    <tr><th>Workers</th> <td>%d</td></tr>
    <tr><th>Cores</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
</table>
</div>
"""


class DaskGatewayCluster(object):
    def __init__(self, gateway, cluster_info):
        self._gateway = gateway
        self.cluster_name = cluster_info['cluster_name']
        self.scheduler_address = cluster_info['scheduler_address']
        self.dashboard_address = cluster_info['dashboard_address'] or None
        self.security = GatewaySecurity(tls_key=cluster_info['tls_key'],
                                        tls_cert=cluster_info['tls_cert'])
        self._internal_client = None
        # XXX: Ensure client is closed. Currently disconnected clients try to
        # reconnect forever, which spams the schedulerproxy. Once this bug is
        # fixed, we should be able to remove this.
        self._clients = weakref.WeakSet()

    def __repr__(self):
        return 'DaskGatewayCluster<%s>' % self.cluster_name

    async def __aenter__(self):
        return self

    async def __aexit__(self):
        await self.shutdown()

    def __enter__(self):
        return self

    def __exit__(self):
        self.shutdown()

    def shutdown(self, **kwargs):
        """Shutdown this cluster."""
        if self._internal_client is not None:
            self._internal_client.close()
            self._internal_client = None
        for client in list(self._clients):
            client.close()
            self._clients.discard(client)
        return self._gateway.stop_cluster(self.cluster_name, **kwargs)

    def close(self, **kwargs):
        """Shutdown this cluster, alias for ``shutdown``"""
        return self.shutdown(**kwargs)

    def get_client(self, set_as_default=True):
        """Get a ``Client`` for this cluster.

        Returns
        -------
        client : Client
        """
        client = Client(
            self.scheduler_address,
            security=self.security,
            set_as_default=set_as_default,
            asynchronous=self._gateway.asynchronous,
            loop=self._gateway.loop,
        )
        if not self._gateway.asynchronous:
            self._clients.add(client)
        return client

    def scale(self, n, **kwargs):
        """Scale the cluster to ``n`` workers.

        Parameters
        ----------
        n : int
            The number of workers to scale to.
        """
        return self._gateway.scale_cluster(self.cluster_name, n, **kwargs)

    def _widget_status(self):
        if self._internal_client is None:
            return None
        try:
            workers = self._internal_client._scheduler_identity['workers']
        except KeyError:
            if self._internal_client.status in ('closing', 'closed'):
                return None
        else:
            n_workers = len(workers)
            cores = sum(w['ncores'] for w in workers.values())
            memory = sum(w['memory_limit'] for w in workers.values())

            return _widget_status_template % (
                n_workers, cores, format_bytes(memory)
            )

    async def _widget_updater(self, widget):
        while True:
            value = self._widget_status()
            if value is None:
                break
            widget.value = value
            await gen.sleep(2)

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        if self._gateway.asynchronous:
            return None

        try:
            from ipywidgets import Layout, VBox, HBox, IntText, Button, HTML
        except ImportError:
            self._cached_widget = None
            return None

        try:
            self._internal_client = self.get_client(set_as_default=False)
        except Exception:
            return None

        layout = Layout(width='150px')

        title = HTML('<h2>DaskGatewayCluster</h2>')

        status = HTML(self._widget_status(), layout=Layout(min_width='150px'))

        request = IntText(0, description='Workers', layout=layout)
        scale = Button(description='Scale', layout=layout)

        @scale.on_click
        def scale_cb(b):
            with log_errors():
                self.scale(request.value)

        elements = [title,
                    HBox([status, request, scale])]

        if self.dashboard_address is not None:
            link_address = '%s/status' % self.dashboard_address
            link = HTML('<p><b>Dashboard: </b><a href="%s" target="_blank">%s'
                        '</a></p>\n' % (link_address, link_address))
            elements.append(link)

        self._cached_widget = box = VBox(elements)

        self._internal_client.loop.add_callback(self._widget_updater, status)

        return box

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        print(self)
