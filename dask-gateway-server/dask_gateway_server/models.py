import enum


class IntEnum(enum.IntEnum):
    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError("%r is not a valid %s" % (name, cls.__name__))


class ClusterStatus(IntEnum):
    STARTING = 1
    STARTED = 2
    RUNNING = 3
    STOPPING = 4
    STOPPED = 5
    FAILED = 6


class User(object):
    def __init__(self, name, groups=(), admin=False):
        self.name = name
        self.groups = set(groups)
        self.admin = bool(admin)


class Cluster(object):
    def __init__(
        self,
        name,
        user,
        options,
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
        self.user = user
        self.options = options
        self.status = status
        self.scheduler_address = scheduler_address
        self.dashboard_address = dashboard_address
        self.api_address = api_address
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self.start_time = start_time
        self.stop_time = stop_time
