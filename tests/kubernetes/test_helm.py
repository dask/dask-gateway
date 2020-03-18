import os
import subprocess
import tempfile
import yaml

import pytest

try:
    subprocess.check_call(["helm", "version"])
except Exception:
    pytest.skip("Helm not available", allow_module_level=True)

CHART_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "resources", "helm", "dask-gateway"
    )
)


def helm_template(name, values):
    with tempfile.NamedTemporaryFile() as fil:
        fil.write(values)
        out = subprocess.check_output(
            ["helm", "template", name, CHART_PATH, "-f", fil.name]
        )
        return list(yaml.safe_load_all(out))


def test_helm_lint():
    return subprocess.check_call(["helm", "lint", CHART_PATH])


def test_render_default():
    helm_template("foo", b"")
