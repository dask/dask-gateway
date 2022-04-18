import getpass
import os
import re
from base64 import b64encode

import dask

from .utils import format_template

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
    auth_kwargs = {k: format_template(v) for k, v in auth_kwargs.items()}
    out = auth(**auth_kwargs)

    if not isinstance(out, GatewayAuth):
        raise TypeError("auth must be instance of GatewayAuth, got %r" % out)
    return out


class GatewayAuth:
    """Base class for authenticating clients in dask-gateway"""

    def __init__(self, **kwargs):
        pass

    def pre_request(self, resp):
        """Authentication hook before client request is sent.

        This is only called after a 401 response from the server.

        Parameters
        ----------
        resp : aiohttp.ClientResponse
            The 401 response that triggered the authentication step.

        Returns
        -------
        headers : dict
            Any headers to add to the outgoing request.
        context : object or None
            If necessary, may optionally return an opaque object storing
            additional context needed to complete the authentication process.
            This will be passed to ``post_response``.
        """
        pass

    def post_response(self, resp, context=None):
        """Authentication hook after server response is received.

        Parameters
        ----------
        resp : aiohttp.ClientResponse
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

    def pre_request(self, resp):
        data = b":".join(
            (self.username.encode("latin1"), self.password.encode("latin1"))
        )
        headers = {"Authorization": "Basic " + b64encode(data).decode()}
        return headers, None


class KerberosAuth(GatewayAuth):
    """Authenticate with kerberos"""

    auth_regex = re.compile(r"(?:.*,)*\s*Negotiate\s*([^,]*),?", re.I)  # noqa

    def pre_request(self, resp):
        # TODO: convert errors to some common error class
        import kerberos

        _, context = kerberos.authGSSClientInit(
            "HTTP@%s" % resp.url.host,
            gssflags=kerberos.GSS_C_MUTUAL_FLAG | kerberos.GSS_C_SEQUENCE_FLAG,
        )
        kerberos.authGSSClientStep(context, "")
        response = kerberos.authGSSClientResponse(context)
        headers = {"Authorization": "Negotiate " + response}
        return headers, context

    def post_response(self, resp, context):
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

    def pre_request(self, resp):
        return {"Authorization": "jupyterhub " + self.api_token}, None
