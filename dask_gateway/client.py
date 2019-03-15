import getpass
import re
from base64 import b64encode
from urllib.parse import urlparse

from tornado.httpclient import AsyncHTTPClient

from .cookiejar import CookieJar


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


class Gateway(object):
    def __init__(self, address=None, auth=None):
        self.address = address
        self._http_client = AsyncHTTPClient()
        self._auth = auth or BasicAuth()
        self._cookie_jar = CookieJar()

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
