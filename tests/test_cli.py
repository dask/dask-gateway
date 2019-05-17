import os
import pytest

from dask_gateway_server import __version__ as VERSION
from dask_gateway_server.app import DaskGateway
from dask_gateway.dask_cli import worker, scheduler


def test_generate_config(tmpdir, capfd):
    cfg_file = str(tmpdir.join("jupyterhub_config.py"))
    orig_text = "c.foo = 'bar'"

    with open(cfg_file, "w") as f:
        f.write(orig_text)

    with pytest.raises(SystemExit) as exc:
        DaskGateway.launch_instance(["generate-config", "--output", cfg_file])
    DaskGateway.clear_instance()
    assert "already exists" in exc.value.code
    out, err = capfd.readouterr()
    assert not out
    assert not err

    assert os.path.exists(cfg_file)
    with open(cfg_file) as f:
        cfg_text = f.read()
    assert cfg_text == orig_text

    DaskGateway.launch_instance(["generate-config", "--force", "--output", cfg_file])
    DaskGateway.clear_instance()
    out, err = capfd.readouterr()
    assert cfg_file in out
    assert not err

    with open(cfg_file) as f:
        cfg_text = f.read()

    assert "DaskGateway.cluster_manager_class" in cfg_text
    assert "ClusterManager.worker_connect_timeout" in cfg_text


@pytest.mark.parametrize("cmd", [scheduler, worker])
def test_dask_gateway_dask_cli(cmd, capfd):
    with pytest.raises(SystemExit) as exc:
        cmd(["--help"])
    assert not exc.value.code
    out, err = capfd.readouterr()
    assert out
    assert not err

    with pytest.raises(SystemExit) as exc:
        cmd(["--version"])
    assert not exc.value.code
    out, err = capfd.readouterr()
    assert VERSION in out
    assert not err
