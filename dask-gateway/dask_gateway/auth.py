import getpass
import re
from base64 import b64encode
from urllib.parse import urlparse

__all__ = ("GatewayAuth", "BasicAuth", "KerberosAuth")


class GatewayAuth(object):
    """Base class for authenticating clients in dask-gateway"""

    def pre_request(self, req, resp):
        """Authentication hook before client request is sent.

        This is only called after a 401 response from the server.

        Parameters
        ----------
        req : HTTPRequest
            The request to authenticate
        resp : HTTPResponse
            The 401 HTTPResponse that triggered the authentication step.

        Returns
        -------
        context : object
            If necessary, may optionally return an opaque object storing
            additional context needed to complete the authentication process.
            This will be passed to ``post_response``.
        """
        pass

    def post_response(self, req, resp, context=None):
        """Authentication hook after server response is received.

        Parameters
        ----------
        req : HTTPRequest
            The submitted HTTPRequest
        resp : HTTPResponse
            The server response
        context : object or None
            The return value of ``pre_request``, providing any additional
            context needed to complete the authentication process.
        """
        pass


class BasicAuth(GatewayAuth):
    """Attaches HTTP Basic Authentication to the given Request object."""

    def __init__(self, username=None, password=None):
        if username is None:
            username = getpass.getuser()
        if password is None:
            password = ""
        self._username = username
        self._password = password
        data = b":".join(
            (self._username.encode("latin1"), self._password.encode("latin1"))
        )
        self._auth = "Basic " + b64encode(data).decode()

    def pre_request(self, req, resp):
        req.headers["Authorization"] = self._auth


class KerberosAuth(GatewayAuth):
    """Authenticate with kerberos"""

    auth_regex = re.compile("(?:.*,)*\s*Negotiate\s*([^,]*),?", re.I)  # noqa

    def pre_request(self, req, resp):
        # TODO: convert errors to some common error class
        import kerberos

        hostname = urlparse(resp.effective_url).hostname
        _, context = kerberos.authGSSClientInit(
            "HTTP@%s" % hostname,
            gssflags=kerberos.GSS_C_MUTUAL_FLAG | kerberos.GSS_C_SEQUENCE_FLAG,
        )
        kerberos.authGSSClientStep(context, "")
        response = kerberos.authGSSClientResponse(context)
        req.headers["Authorization"] = "Negotiate " + response
        return context

    def post_response(self, req, resp, context):
        import kerberos

        www_auth = resp.headers.get("www-authenticate", None)
        token = None
        if www_auth:
            match = self.auth_regex.search(www_auth)
            if match:
                token = match.group(1)
        if not token:
            raise ValueError("Kerberos negotiation failed")
        kerberos.authGSSClientStep(context, token)
