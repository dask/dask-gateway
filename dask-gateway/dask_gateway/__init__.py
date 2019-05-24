from .client import Gateway, GatewayCluster
from .auth import KerberosAuth, BasicAuth

# Load configuration
from . import config

del config

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
