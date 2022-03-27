import enum

__all__ = ("User", "Cluster", "ClusterStatus")


class IntEnum(enum.IntEnum):
    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError(f"{name!r} is not a valid {cls.__name__}")


class ClusterStatus(IntEnum):
    """A cluster's status.

    Attributes
    ----------
    PENDING : ClusterStatus
        The cluster is pending start.
    RUNNING : ClusterStatus
        The cluster is running.
    STOPPING : ClusterStatus
        The cluster is stopping, but not fully stopped.
    STOPPED : ClusterStatus
        The cluster was shutdown by user or admin request.
    FAILED : ClusterStatus
        A failure occurred during any of the above states.
    """

    PENDING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    FAILED = 5


class User:
    """A user record.

    Parameters
    ----------
    name : str
        The username
    groups : sequence, optional
        A set of groups the user belongs to. Default is no groups.
    admin : bool, optional
        Whether the user is an admin user. Default is False.
    """

    def __init__(self, name, groups=(), admin=False):
        self.name = name
        self.groups = set(groups)
        self.admin = bool(admin)

    def has_permissions(self, cluster):
        """Check if the user has permissions to view the cluster"""
        return self.name == cluster.username


class Cluster:
    """A cluster record.

    Parameters
    ----------
    name : str
        The cluster name. An unique string that can be used to identify the
        cluster in the gateway.
    username : str
        The username that started this cluster.
    token : str
        The API token associated with this cluster. Used to authenticate the
        cluster with the gateway.
    options : dict
        The normalized set of configuration options provided when starting this
        cluster. These values are user-facing, and don't necessarily correspond
        with the ``ClusterConfig`` options on the backend.
    config : dict
        The serialized version of ``ClusterConfig`` for this cluster.
    status : ClusterStatus
        The status of the cluster.
    scheduler_address : str
        The scheduler address. The empty string if the cluster is not running.
    dashboard_address : str
        The dashboard address. The empty string if the cluster is not running,
        or no dashboard is running on the cluster.
    api_address : str
        The cluster's api address. The empty string if the cluster is not running.
    tls_cert : bytes
        The TLS cert credentials associated with the cluster.
    tls_key : bytes
        The TLS key credentials associated with the cluster.
    start_time : int or None
        Start time in ms since the epoch, or None if not started.
    stop_time : int or None
        Stop time in ms since the epoch, or None if not stopped.
    """

    def __init__(
        self,
        name,
        username,
        token,
        options,
        config,
        status,
        scheduler_address="",
        dashboard_address="",
        api_address="",
        tls_cert=b"",
        tls_key=b"",
        start_time=None,
        stop_time=None,
    ):
        self.name = name
        self.username = username
        self.token = token
        self.options = options
        self.config = config
        self.status = status
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.start_time = start_time
        self.stop_time = stop_time

    def to_dict(self, full=True):
        if self.status == ClusterStatus.RUNNING and self.dashboard_address:
            dashboard = "/clusters/%s/status" % self.name
        else:
            dashboard = None
        out = {
            "name": self.name,
            "dashboard_route": dashboard,
            "status": self.status.name,
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "options": self.options,
        }
        if full:
            if self.status == ClusterStatus.RUNNING:
                tls_cert = self.tls_cert.decode()
                tls_key = self.tls_key.decode()
            else:
                tls_cert = tls_key = None
            out["tls_cert"] = tls_cert
            out["tls_key"] = tls_key
        return out
