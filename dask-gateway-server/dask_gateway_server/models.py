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
    PENDING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    FAILED = 5


class User(object):
    def __init__(self, name, groups=(), admin=False):
        self.name = name
        self.groups = set(groups)
        self.admin = bool(admin)

    def has_permissions(self, cluster):
        return self.name == cluster.username


class Cluster(object):
    def __init__(
        self,
        name,
        username,
        token,
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
        self.username = username
        self.token = token
        self.options = options
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
