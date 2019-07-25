"""Utility methods for use in dask_gateway_config.py"""

from functools import lru_cache
import os

import yaml

CONFIG_MAP_PATH = "/etc/dask-gateway/config/values.yaml"


@lru_cache()
def _load_config(path):
    """Load configuration from disk"""
    cfg = {}
    if os.path.exists(path):
        print("Loading %s" % path)
        with open(path) as f:
            values = yaml.safe_load(f)
        cfg = _merge(cfg, values)
    else:
        print("No config at %s" % path)
    return cfg


def _merge(a, b):
    """Merge two dictionaries recursively."""
    out = a.copy()
    for key in b:
        if key in a and isinstance(a[key], dict) and isinstance(b[key], dict):
            out[key] = _merge(a[key], b[key])
        else:
            out[key] = b[key]
    return out


def get_property(key, default=None, path=CONFIG_MAP_PATH):
    """Read a property from the configured helm values."""
    value = _load_config(path)
    for key2 in key.split("."):
        if not isinstance(value, dict) or key2 not in value:
            return default
        value = value[key2]
    return value
