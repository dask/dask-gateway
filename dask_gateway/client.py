import getpass
import json
import os
import re
import ssl
import tempfile
from base64 import b64encode
from datetime import timedelta
from threading import get_ident
from urllib.parse import urlparse

from distributed import Client
from distributed.security import Security
from distributed.utils import LoopRunner, sync, thread_state
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from .cookiejar import CookieJar

# Register gateway protocol
from . import comm
del comm


class AuthBase(object):
    def pre_request(self, req, resp):
        pass

    def post_response(self, req, resp, context):
        pass


class BasicAuth(AuthBase):
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


class KerberosAuth(AuthBase):
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
        return json.loads(resp.body)

    def clusters(self, **kwargs):
        return self.sync(self._clusters, **kwargs)

    async def _start_cluster(self):
        url = "%s/gateway/api/clusters/" % self.address
        req = HTTPRequest(url=url, method="POST", body='{}')
        resp = await self._fetch(req)
        data = json.loads(resp.body)
        return data['cluster_name']

    def start_cluster(self, **kwargs):
        return self.sync(self._start_cluster, **kwargs)

    async def _stop_cluster(self, cluster_name):
        url = "%s/gateway/api/clusters/%s" % (self.address, cluster_name)
        req = HTTPRequest(url=url, method="DELETE")
        await self._fetch(req)

    def stop_cluster(self, cluster_name, **kwargs):
        return self.sync(self._stop_cluster, cluster_name, **kwargs)

    async def _get_cluster(self, cluster_name):
        url = "%s/gateway/api/clusters/%s" % (self.address, cluster_name)
        req = HTTPRequest(url=url)
        resp = await self._fetch(req)
        return json.loads(resp.body)

    def get_cluster(self, cluster_name, **kwargs):
        return self.sync(self._get_cluster, cluster_name, **kwargs)


class Cluster(object):
    def __init__(self, gateway, cluster_info):
        self._gateway = gateway
        self.scheduler_address = cluster_info['scheduler_address']
        self.dashboard_address = cluster_info['dashboard_address']
        self.security = GatewaySecurity(tls_key=cluster_info['tls_key'],
                                        tls_cert=cluster_info['tls_cert'])

    def connect(self, asynchronous=False, loop=None):
        return Client(self.scheduler_address,
                      security=self.security,
                      asynchronous=asynchronous,
                      loop=(loop or self._gateway.loop))
