import os
import subprocess
import tempfile
import yaml

import pytest

pytestmark = pytest.mark.kubernetes

try:
    subprocess.check_call(["helm", "version"])
except Exception:
    pytest.skip("Helm not available", allow_module_level=True)

CHART_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "resources", "helm", "dask-gateway"
    )
)


def helm_install_dry_run(name, values):
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8") as fil:
        fil.write(values)
        res = subprocess.run(
            ["helm", "install", "--dry-run", name, CHART_PATH, "-f", fil.name],
            capture_output=True,
        )
        stdout = res.stdout.decode()
        if res.returncode != 0:
            raise Exception("helm install failed:\n%s" % stdout)
        start = stdout.index("---\n") + 4
        end = stdout.index("NOTES:")
        rendered = stdout[start:end]
        return list(yaml.safe_load_all(rendered))


def test_helm_lint():
    return subprocess.check_call(["helm", "lint", CHART_PATH])


def test_render_default():
    helm_install_dry_run("foo", "")


values_with_affinity = """
gateway:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/e2e-az-name
            operator: In
            values:
            - e2e-az1

controller:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/e2e-az-name
            operator: In
            values:
            - e2e-az1

traefik:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/e2e-az-name
            operator: In
            values:
            - e2e-az1
"""


def test_render_with_affinity():
    helm_install_dry_run("foo", values_with_affinity)
