import os
import pytest

from dask_gateway_server import __version__ as VERSION
from dask_gateway_server.app import DaskGateway
from dask_gateway_server.proxy.core import SchedulerProxyApp, WebProxyApp, _PROXY_EXE
from dask_gateway.dask_cli import worker, scheduler


def test_generate_config(tmpdir, capfd):
    cfg_file = str(tmpdir.join("dask_gateway_config.py"))
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


@pytest.mark.parametrize("kind", ["web", "scheduler"])
def test_proxy_cli(kind, tmpdir, monkeypatch):
    cfg_file = str(tmpdir.join("dask_gateway_config.py"))

    text = (
        "c.DaskGateway.gateway_url = 'tls://127.0.0.1:8786'\n"
        "c.DaskGateway.public_url = 'http://127.0.0.1:8888'\n"
        "c.SchedulerProxy.api_url = 'http://127.0.0.1:8866'\n"
        "c.WebProxy.api_url = 'http://127.0.0.1:8867'\n"
        "c.ProxyBase.log_level = 'debug'\n"
        "c.ProxyBase.auth_token = 'abcde'"
    )
    with open(cfg_file, "w") as f:
        f.write(text)

    if kind == "web":
        address = "127.0.0.1:8888"
        api_address = "127.0.0.1:8867"
    else:
        address = "127.0.0.1:8786"
        api_address = "127.0.0.1:8866"

    called_with = []

    def mock_execle(*args):
        called_with.extend(args)

    monkeypatch.setattr(os, "execle", mock_execle)
    DaskGateway.launch_instance([kind + "-proxy", "-f", cfg_file])
    DaskGateway.clear_instance()
    SchedulerProxyApp.clear_instance()
    WebProxyApp.clear_instance()

    assert called_with
    env = called_with.pop()

    assert called_with == [
        _PROXY_EXE,
        "dask-gateway-proxy",
        kind,
        "-address",
        address,
        "-api-address",
        api_address,
        "-log-level",
        "debug",
    ]

    assert "DASK_GATEWAY_PROXY_TOKEN" in env


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
