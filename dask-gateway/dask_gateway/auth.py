import getpass
import os
import re
from base64 import b64encode
from urllib.parse import urlparse

import dask


__all__ = ("GatewayAuth", "BasicAuth", "KerberosAuth", "JupyterHubAuth", "get_auth")


def _import_object(name):
    parts = name.rsplit(".", 1)
    if len(parts) == 2:
        package, obj = parts
        mod = __import__(package, fromlist=[obj])
        try:
            return getattr(mod, obj)
        except AttributeError:
            raise ImportError("Failed to import %s" % name)
    else:
        return __import__(name)


def get_auth(auth=None):
    """Get a ``GatewayAuth`` instance.

    Creates an authenticator depending on the given parameters and gateway
    configuration.
    """
    if isinstance(auth, GatewayAuth):
        return auth

    if auth is None:
        auth = dask.config.get("gateway.auth.type", None)

    if isinstance(auth, str):
        if auth == "kerberos":
            auth = KerberosAuth
        elif auth == "basic":
            auth = BasicAuth
        elif auth == "jupyterhub":
            auth = JupyterHubAuth
        else:
            auth = _import_object(auth)
    elif not callable(auth):
        raise TypeError("Unknown auth value %r" % auth)

    auth_kwargs = dask.config.get("gateway.auth.kwargs", None) or {}
    out = auth(**auth_kwargs)

    if not isinstance(out, GatewayAuth):
        raise TypeError("auth must be instance of GatewayAuth, got %r" % out)
    return out


class GatewayAuth(object):
    """Base class for authenticating clients in dask-gateway"""

    def __init__(self, **kwargs):
        pass

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
        self.username = username
        self.password = password

    def pre_request(self, req, resp):
        data = b":".join(
            (self.username.encode("latin1"), self.password.encode("latin1"))
        )
        req.headers["Authorization"] = "Basic " + b64encode(data).decode()


class KerberosAuth(GatewayAuth):
    """Authenticate with kerberos"""

    auth_regex = re.compile(r"(?:.*,)*\s*Negotiate\s*([^,]*),?", re.I)  # noqa

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
            raise Exception("Kerberos negotiation failed")
        kerberos.authGSSClientStep(context, token)


class JupyterHubAuth(GatewayAuth):
    """Uses JupyterHub API tokens to authenticate"""

    def __init__(self, api_token=None):
        if api_token is None:
            api_token = os.environ.get("JUPYTERHUB_API_TOKEN")
            if api_token is None:
                raise ValueError(
                    "No JupyterHub API token provided, and JUPYTERHUB_API_TOKEN "
                    "environment variable not found"
                )
        self.api_token = api_token

    def pre_request(self, req, resp):
        req.headers["Authorization"] = "jupyterhub " + self.api_token
