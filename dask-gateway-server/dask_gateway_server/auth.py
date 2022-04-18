import base64
import os
import time
import uuid
from urllib.parse import quote

import aiohttp
from aiohttp import web
from traitlets import Instance, Integer, Unicode, default
from traitlets.config import LoggingConfigurable

from .models import User

__all__ = (
    "Authenticator",
    "SimpleAuthenticator",
    "KerberosAuthenticator",
    "JupyterHubAuthenticator",
)


class UserCache:
    def __init__(self, max_age):
        self.max_age = max_age
        self.name_to_cookie = {}
        self.cookie_to_user = {}

    def get(self, cookie):
        user = self.cookie_to_user.get(cookie)
        if user is None:
            return None
        timestamp, user = self.cookie_to_user[cookie]
        if timestamp + self.max_age < time.monotonic():
            del self.cookie_to_user[cookie]
            del self.name_to_cookie[user.name]
            return None
        return user

    def put(self, user):
        cookie = self.name_to_cookie.get(user.name)
        if cookie is None:
            cookie = uuid.uuid4().hex
            now = time.monotonic()
            self.name_to_cookie[user.name] = cookie
            self.cookie_to_user[cookie] = (now, user)
        return cookie


class Authenticator(LoggingConfigurable):
    """Base class for authenticators.

    An authenticator manages authenticating user API requests.

    Subclasses must define ``authenticate``, and may optionally also define
    ``setup``, ``cleanup`` and ``pre_response``.
    """

    cookie_name = Unicode(
        help="The cookie name to use for caching authentication information.",
        config=True,
    )

    @default("cookie_name")
    def _default_cookie_name(self):
        return "dask-gateway-%s" % uuid.uuid4().hex

    cache_max_age = Integer(
        300,
        help="""The maximum time in seconds to cache authentication information.

        Helps reduce load on the backing authentication service by caching
        responses between requests. After this time the user will need to be
        reauthenticated before making additional requests (note this is usually
        transparent to the user).
        """,
        config=True,
    )

    cache = Instance(UserCache)

    @default("cache")
    def _default_cache(self):
        return UserCache(max_age=self.cache_max_age)

    async def authenticate_and_handle(self, request, handler):
        # Try to authenticate with the cookie first
        cookie = request.cookies.get(self.cookie_name)
        if cookie is not None:
            user = self.cache.get(cookie)
            if user is not None:
                request["user"] = user
                return await handler(request)

        # Otherwise go through full authentication process
        user = await self.authenticate(request)
        if type(user) is tuple:
            user, context = user
        else:
            context = None

        request["user"] = user
        response = await handler(request)

        await self.pre_response(request, response, context)
        cookie = self.cache.put(user)
        response.set_cookie(self.cookie_name, cookie, max_age=self.cache_max_age * 2)

        return response

    async def setup(self, app):
        """Called when the server is starting up.

        Do any initialization here.

        Parameters
        ----------
        app : aiohttp.web.Application
            The aiohttp application. Can be used to add additional routes if
            needed.
        """
        pass

    async def cleanup(self):
        """Called when the server is shutting down.

        Do any cleanup here."""
        pass

    async def authenticate(self, request):
        """Perform the authentication process.

        Parameters
        ----------
        request : aiohttp.web.Request
            The current request.

        Returns
        -------
        user : User
            The authenticated user.
        context : object, optional
            If necessary, may optionally return an opaque object storing
            additional context needed to complete the authentication process.
            This will be passed to ``pre_response``.
        """
        raise NotImplementedError

    async def pre_response(self, request, response, context=None):
        """Called before returning a response.

        Allows modifying the outgoing response in-place to add additional
        headers, etc...

        Note that this is only called if ``authenticate`` was applied for this
        request.

        Parameters
        ----------
        request : aiohttp.web.Request
            The current request.
        response : aiohttp.web.Response
            The current response. May be modified in-place.
        context : object or None
            If present, the extra return value of ``authenticate``, providing
            any additional context needed to complete the authentication
            process.
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

    async def authenticate(self, request):
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

        return User(user)


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

    async def setup(self, app):
        os.environ["KRB5_KTNAME"] = self.keytab

    def raise_auth_error(self, err):
        self.log.error("Kerberos failure: %s", err)
        raise web.HTTPInternalServerError(reason="Error during kerberos authentication")

    async def authenticate(self, request):
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
        except kerberos.GSSError as err:
            self.raise_auth_error(err)
        finally:
            if gss_context is not None:
                kerberos.authGSSServerClean(gss_context)

        return User(user), gss_key

    async def pre_response(self, request, response, context):
        response.headers["WWW-Authenticate"] = "Negotiate %s" % context


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

    async def setup(self, app):
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

    async def cleanup(self):
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

        url = "{}/authorizations/token/{}".format(
            self.jupyterhub_api_url,
            quote(token, safe=""),
        )

        kwargs = {
            "headers": {"Authorization": "token %s" % self.jupyterhub_api_token},
            "ssl": self.ssl_context,
        }

        resp = await self.session.get(url, **kwargs)

        if resp.status < 400:
            data = await resp.json()
            # "groups" attribute doesn't exists in case of a service
            return User(
                data["name"],
                groups=data.get("groups", []),
                admin=data.get("admin", False),
            )
        elif resp.status == 404:
            self.log.debug("Token for non-existent user requested")
            raise unauthorized("jupyterhub")
        else:
            if resp.status == 403:
                err = web.HTTPInternalServerError(
                    reason="Permission failure verifying user's JupyterHub API token"
                )
            elif resp.status >= 500:
                err = web.HTTPBadGateway(
                    reason="Upstream failure verifying user's JupyterHub API token"
                )
            else:
                err = web.HTTPInternalServerError(
                    reason="Failure verifying user's JupyterHub API token"
                )

            self.log.error(
                "%s - code: %s, reason: %s", err.reason, resp.status, resp.reason
            )
            raise err
