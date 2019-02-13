from traitlets import Unicode, Integer
from traitlets.config import LoggingConfigurable


class ClusterSpawner(LoggingConfigurable):
    """Base class for spawning dask clusters"""

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

    start_timeout = Integer(
        60,
        help="""
        Timeout (in seconds) before giving up on a starting dask cluster.
        """
    )

    def get_state(self):
        """Return all state that is needed to reconnect to this cluster-spawner
        instance after a gateway restart."""
        return {}

    def load_state(self, state):
        """Restore cluster spawner from stored state.

        Parameters
        ----------
        state : dict
        """
        pass

    async def start(self):
        """Start a new cluster"""
        pass

    async def is_running(self):
        """Check if the cluster is running"""
        pass

    async def stop(self):
        """Stop the cluster"""
        pass
