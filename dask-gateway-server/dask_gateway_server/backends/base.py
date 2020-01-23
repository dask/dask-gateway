from traitlets import Instance
from traitlets.config import LoggingConfigurable

from ..options import Options


class Backend(LoggingConfigurable):
    cluster_options = Instance(
        Options,
        args=(),
        help="""
        User options for configuring an individual cluster.

        Allows users to specify configuration overrides when creating a new
        cluster. See the documentation for more information:

        :doc:`cluster-options`.
        """,
        config=True,
    )

    async def on_startup(self):
        """Called when the server is starting up.

        Do any setup tasks in this method"""
        pass

    async def on_shutdown(self):
        """Called when the server is shutting down.

        Do any cleanup tasks in this method"""
        pass

    async def process_cluster_options(self, user, cluster_options):
        options = await self.get_cluster_options(user)
        return options.parse_options(cluster_options)

    async def get_cluster_options(self, user):
        """Get cluster options available to this user.

        Parameters
        ----------
        user : str
            The user making the request.

        Returns
        -------
        options_spec : dict
        """
        return self.cluster_options

    async def user_has_cluster_permissions(self, user, cluster_id):
        """Check if a user has permissions to interact with a cluster.

        Parameters
        ----------
        user : User
            The user to check.
        cluster_id : str
            The cluster to check

        Returns
        -------
        has_permissions : bool
        """
        raise NotImplementedError

    async def list_clusters(self, user=None, statuses=None):
        """List known clusters.

        Parameters
        ----------
        user : str, optional
            A user name to filter on. If not provided, defaults to
            all users.
        statuses : list, optional
            A list of statuses to filter on. If not provided, defaults to all
            running and pending clusters.

        Returns
        -------
        clusters : List[Cluster]
        """
        raise NotImplementedError

    async def get_cluster(self, cluster_id):
        """Get information about a cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID.

        Returns
        -------
        cluster : Cluster
        """
        raise NotImplementedError

    async def start_cluster(self, user, cluster_options):
        """Start a new cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_options : dict
            Any additional options provided by the user.

        Returns
        -------
        cluster_id : str
        """
        raise NotImplementedError

    async def stop_cluster(self, user, cluster_id):
        """Stop a cluster.

        No-op if the cluster is already stopped.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        """
        raise NotImplementedError

    async def scale_cluster(self, user, cluster_id, n):
        """Scale a cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        n : int
            The number of workers to scale to.
        """
        raise NotImplementedError

    async def adapt_cluster(
        self, user, cluster_id, minimum=None, maximum=None, active=True
    ):
        """Adaptively scale a cluster.

        Parameters
        ----------
        user : str
            The user making the request.
        cluster_id : str
            The cluster ID.
        minimum : int, optional
            The minimum number of workers to adaptively scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to adaptively scale to. Defaults to infinity.
        active : bool, optional
            Set to False to disable adaptive scaling.
        """
        raise NotImplementedError
