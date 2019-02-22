import os

from tornado import web
from traitlets import Unicode
from traitlets.config import LoggingConfigurable


class Authenticator(LoggingConfigurable):
    """Base class for authenticators"""

    def authenticate(self, handler):
        """Perform the authentication process.

        Parameters
        ----------
        handler : tornado.web.RequestHandler
            The current request handler

        Returns
        -------
        user : str
            The user name
        """
        pass


class KerberosAuthenticator(Authenticator):
    """An authenticator using kerberos"""

    service_name = Unicode(
        "HTTP",
        help="""The service's kerberos principal name.

        This is almost always "HTTP" (the default)""",
        config=True
    )

    keytab = Unicode(
        "dask_gateway.keytab",
        help="The path to the keytab file",
        config=True
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        os.environ['KRB5_KTNAME'] = self.keytab

    def raise_auth_error(self, err):
        self.log.error("Kerberos failure: %s", err)
        raise web.HTTPError(500, "Error during kerberos authentication")

    def raise_auth_required(self, handler):
        handler.set_status(401)
        handler.write('Authentication required')
        handler.set_header("WWW-Authenticate", "Negotiate")
        raise web.Finish()

    def authenticate(self, handler):
        import kerberos

        auth_header = handler.request.headers.get('Authorization')
        if not auth_header:
            return self.raise_auth_required(handler)

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != 'Negotiate':
            return self.raise_auth_required(handler)

        gss_context = None
        try:
            # Initialize kerberos context
            rc, gss_context = kerberos.authGSSServerInit(self.service_name)

            if rc != kerberos.AUTH_GSS_COMPLETE:
                return self.raise_auth_error(
                    "GSS server Init failed, return code = %r" % rc
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
            handler.set_header('WWW-Authenticate', "Negotiate %s" % gss_key)
        except kerberos.GSSError as err:
            return self.raise_auth_error(err)
        finally:
            if gss_context is not None:
                kerberos.authGSSServerClean(gss_context)

        return user
