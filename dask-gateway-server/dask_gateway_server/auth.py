import os
import base64

from tornado import web
from traitlets import Unicode
from traitlets.config import LoggingConfigurable

__all__ = ("Authenticator", "KerberosAuthenticator", "DummyAuthenticator")


class Authenticator(LoggingConfigurable):
    """Base class for authenticators"""

    def authenticate(self, handler):
        """Perform the authentication process.

        Parameters
        ----------
        handler : tornado.web.RequestHandler
            The current request handler.

        Returns
        -------
        user : str
            The user name.
        """
        pass


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        os.environ["KRB5_KTNAME"] = self.keytab

    def raise_auth_error(self, err):
        self.log.error("Kerberos failure: %s", err)
        raise web.HTTPError(500, "Error during kerberos authentication")

    def raise_auth_required(self, handler):
        handler.set_status(401)
        handler.write("Authentication required")
        handler.set_header("WWW-Authenticate", "Negotiate")
        raise web.Finish()

    def authenticate(self, handler):
        import kerberos

        auth_header = handler.request.headers.get("Authorization")
        if not auth_header:
            return self.raise_auth_required(handler)

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Negotiate":
            return self.raise_auth_required(handler)

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
                return self.raise_auth_error(
                    "GSS server init failed, return code = %r" % rc
                )

            # Challenge step
            rc = kerberos.authGSSServerStep(gss_context, auth_key)
            if rc != kerberos.AUTH_GSS_COMPLETE:
                return self.raise_auth_error(
                    "GSS server step failed, return code = %r" % rc
                )
            gss_key = kerberos.authGSSServerResponse(gss_context)

            # Retrieve user name
            fulluser = kerberos.authGSSServerUserName(gss_context)
            user = fulluser.split("@", 1)[0]

            # Complete the protocol by responding with the Negotiate header
            handler.set_header("WWW-Authenticate", "Negotiate %s" % gss_key)
        except kerberos.GSSError as err:
            return self.raise_auth_error(err)
        finally:
            if gss_context is not None:
                kerberos.authGSSServerClean(gss_context)

        return user


class DummyAuthenticator(Authenticator):
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

    def raise_auth_required(self, handler):
        handler.set_status(401)
        handler.write("Authentication required")
        handler.set_header("WWW-Authenticate", "Basic")
        raise web.Finish()

    def authenticate(self, handler):
        auth_header = handler.request.headers.get("Authorization")
        if not auth_header:
            self.raise_auth_required(handler)

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Basic":
            self.raise_auth_required(handler)

        auth_key = base64.b64decode(auth_key).decode("ascii")
        user, password = auth_key.split(":", 1)

        if self.password and password != self.password:
            self.raise_auth_required(handler)

        return user
