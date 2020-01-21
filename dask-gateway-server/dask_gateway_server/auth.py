import os
import base64
from urllib.parse import quote

import aiohttp
from aiohttp import web
from traitlets import Unicode, default
from traitlets.config import LoggingConfigurable

__all__ = (
    "Authenticator",
    "SimpleAuthenticator",
    "KerberosAuthenticator",
    "JupyterHubAuthenticator",
)


class Authenticator(LoggingConfigurable):
    """Base class for authenticators"""

    async def on_startup(self):
        """Called when the application starts up.

        Do any initialization here."""
        pass

    async def on_shutdown(self):
        """Called when the application shutsdown.

        Do any cleanup here."""
        pass

    def authenticate(self, request):
        """Perform the authentication process.

        Parameters
        ----------
        request : aiohttp.web.Request
            The current request.

        Returns
        -------
        user : dict
            A dict with "name", "groups", and "admin" fields corresponding to
            the authenticating user.
        """
        pass


def unauthorized(kind):
    return web.HTTPUnauthorized(
        headers={"WWW-Authenticate": kind}, reason="Authentication required"
    )


class SimpleAuthenticator(Authenticator):
    """A simple authenticator that uses Basic Auth.

    This is highly insecure, use only for testing!!!
    """

    password = Unicode(
        None,
        allow_none=True,
        help="""
        If set, a global password that all users must provide.

        If unset (default), the password field is completely ignored.
        """,
        config=True,
    )

    def authenticate(self, request):
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise unauthorized("Basic")

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Basic":
            raise unauthorized("Basic")

        auth_key = base64.b64decode(auth_key).decode("ascii")
        user, password = auth_key.split(":", 1)

        if self.password and password != self.password:
            raise unauthorized("Basic")

        return {"name": user, "groups": [], "admin": False}


class KerberosAuthenticator(Authenticator):
    """An authenticator using kerberos"""

    service_name = Unicode(
        "HTTP",
        help="""The service's kerberos principal name.

        This is almost always "HTTP" (the default)""",
        config=True,
    )

    keytab = Unicode(
        "dask_gateway.keytab", help="The path to the keytab file", config=True
    )

    async def on_startup(self):
        os.environ["KRB5_KTNAME"] = self.keytab

    def raise_auth_error(self, err):
        self.log.error("Kerberos failure: %s", err)
        raise web.HTTPInternalServerError(reason="Error during kerberos authentication")

    def authenticate(self, request):
        import kerberos

        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise unauthorized("Negotiate")

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Negotiate":
            raise unauthorized("Negotiate")

        gss_context = None
        try:
            # Initialize kerberos context
            rc, gss_context = kerberos.authGSSServerInit(self.service_name)

            # NOTE: Per the pykerberos documentation, the return code should be
            # checked after each step. However, after reading the pykerberos
            # code no method used here will ever return anything but
            # AUTH_GSS_COMPLETE (all other cases will raise an exception).  We
            # keep these checks in just in case pykerberos changes its behavior
            # to match its docs, but they likely never will trigger.

            if rc != kerberos.AUTH_GSS_COMPLETE:
                self.raise_auth_error("GSS server init failed, return code = %r" % rc)

            # Challenge step
            rc = kerberos.authGSSServerStep(gss_context, auth_key)
            if rc != kerberos.AUTH_GSS_COMPLETE:
                self.raise_auth_error("GSS server step failed, return code = %r" % rc)
            gss_key = kerberos.authGSSServerResponse(gss_context)

            # Retrieve user name
            fulluser = kerberos.authGSSServerUserName(gss_context)
            user = fulluser.split("@", 1)[0]

            # Complete the protocol by responding with the Negotiate header
            request["auth-headers"] = {"WWW-Authenticate": "Negotiate %s" % gss_key}
        except kerberos.GSSError as err:
            self.raise_auth_error(err)
        finally:
            if gss_context is not None:
                kerberos.authGSSServerClean(gss_context)

        return {"name": user, "groups": [], "admin": False}


class JupyterHubAuthenticator(Authenticator):
    """An authenticator that uses JupyterHub to perform authentication"""

    jupyterhub_api_token = Unicode(
        help="""
        Dask Gateway's JupyterHub API Token, used for authenticating the
        gateway's API requests to JupyterHub.

        By default this is determined from the ``JUPYTERHUB_API_TOKEN``
        environment variable.
        """,
        config=True,
    )

    @default("jupyterhub_api_token")
    def _default_jupyterhub_api_token(self):
        out = os.environ.get("JUPYTERHUB_API_TOKEN")
        if not out:
            raise ValueError("JUPYTERHUB_API_TOKEN must be set")
        return out

    jupyterhub_api_url = Unicode(
        help="""
        The API URL for the JupyterHub server.

        By default this is determined from the ``JUPYTERHUB_API_URL``
        environment variable.
        """,
        config=True,
    )

    @default("jupyterhub_api_url")
    def _default_jupyterhub_api_url(self):
        out = os.environ.get("JUPYTERHUB_API_URL")
        if not out:
            raise ValueError("JUPYTERHUB_API_URL must be set")
        return out

    tls_key = Unicode(
        "",
        help="""
        Path to TLS key file for making API requests to JupyterHub.

        When setting this, you should also set tls_cert.
        """,
        config=True,
    )

    tls_cert = Unicode(
        "",
        help="""
        Path to TLS certficate file for making API requests to JupyterHub.

        When setting this, you should also set tls_cert.
        """,
        config=True,
    )

    tls_ca = Unicode(
        "",
        help="""
        Path to TLS CA file for verifying API requests to JupyterHub.

        When setting this, you should also set tls_key and tls_cert.
        """,
        config=True,
    )

    async def on_startup(self):
        self.session = aiohttp.ClientSession()
        if self.tls_cert and self.tls_key:
            import ssl

            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_cert_chain(self.tls_cert, keyfile=self.tls_key)
            if self.tls_ca:
                ssl_context.load_verify_locations(self.tls_ca)
        else:
            ssl_context = None
        self.ssl_context = ssl_context

    async def on_shutdown(self):
        if hasattr(self, "session"):
            await self.session.close()

    def get_token(self, request):
        auth_header = request.headers.get("Authorization")
        if auth_header:
            auth_type, auth_key = auth_header.split(" ", 1)
            if auth_type == "jupyterhub":
                return auth_key
        return None

    async def authenticate(self, request):
        token = self.get_token(request)
        if token is None:
            raise unauthorized("jupyterhub")

        url = "%s/authorizations/token/%s" % (
            self.jupyterhub_api_url,
            quote(token, safe=""),
        )

        kwargs = {
            "headers": {"Authorization": "token %s" % self.jupyterhub_api_token},
            "ssl_context": self.ssl_context,
        }

        resp = await self.session.get(url, **kwargs)

        if resp.code < 400:
            data = await resp.json()
            return {
                "name": data["name"],
                "groups": data["groups"],
                "admin": data["admin"],
            }
        elif resp.code == 404:
            self.log.debug("Token for non-existant user requested")
            raise unauthorized("jupyterhub")
        else:
            if resp.code == 403:
                err = web.HTTPInternalServerError(
                    reason="Permission failure verifying user's JupyterHub API token"
                )
            elif resp.code >= 500:
                err = web.HTTPBadGateway(
                    reason="Upstream failure verifying user's JupyterHub API token"
                )
            else:
                err = web.HTTPInternalServerError(
                    reason="Failure verifying user's JupyterHub API token"
                )

            self.log.error(
                "%s - code: %s, reason: %s", err.reason, resp.code, resp.reason
            )
            raise err
