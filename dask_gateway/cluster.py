from traitlets import Unicode, Integer, Dict
from traitlets.config import LoggingConfigurable


class ClusterManager(LoggingConfigurable):
    """Base class for dask cluster managers"""

    ip = Unicode(
        '',
        help="""
        The IP address (or hostname) the dask scheduler should listen on.

        The Dask Gateway Proxy should be able to access this ip.
        """,
        config=True
    )

    port = Integer(
        0,
        help="""
        The port the dask scheduler should connect to.

        Defaults to `0`, which uses a randomly allocated port number each time.

        If set to a non-zero value, all dask clusters will use the same port,
        which only makes sense if each server is on a different address (e.g.
        in containers).
        """,
        config=True
    )

    environment = Dict(
        help="""
        Environment variables to set for both the worker and scheduler processes.
        """,
        config=True
    )

    start_timeout = Integer(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask cluster.
        """
    )

    def get_state(self):
        """Return all state that is needed to reconnect to this cluster-manager
        instance after a gateway restart."""
        return {}

    def load_state(self, state):
        """Restore cluster manager from stored state.

        Parameters
        ----------
        state : dict
        """
        pass

    def initialize(self, request):
        """Initialize the cluster manager with a cluster request.

        This method has two purposes:
        - Check that a given request is valid
        - Initialize the cluster manager with any user-provided settings

        If the request is invalid, this method should error with an informative
        error message to be sent back to the user. Otherwise it should store
        whatever parameters it needs from the request before ``start`` is
        called.

        Parameters
        ----------
        request : dict
            The request for a cluster, loaded from the cluster request json body.
        """
        return request

    async def start(self):
        """Start a new cluster"""
        pass

    async def is_running(self):
        """Check if the cluster is running"""
        pass

    async def stop(self):
        """Stop the cluster"""
        pass
