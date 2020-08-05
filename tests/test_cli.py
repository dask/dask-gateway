import os
import pytest

from dask_gateway_server.app import DaskGateway
from dask_gateway_server.proxy.core import ProxyApp, _PROXY_EXE


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

    assert "DaskGateway.backend_class" in cfg_text
    assert "Backend.cluster_options" in cfg_text


def test_proxy_cli(tmpdir, monkeypatch):
    cfg_file = str(tmpdir.join("dask_gateway_config.py"))

    text = (
        "c.DaskGateway.address = '127.0.0.1:8888'\n"
        "c.Proxy.address = '127.0.0.1:8866'\n"
        "c.Proxy.tcp_address = '127.0.0.1:8867'\n"
        "c.Proxy.log_level = 'debug'\n"
        "c.Proxy.api_token = 'abcde'"
    )
    with open(cfg_file, "w") as f:
        f.write(text)

    called_with = []

    def mock_execle(*args):
        called_with.extend(args)

    monkeypatch.setattr(os, "execle", mock_execle)
    DaskGateway.launch_instance(["proxy", "-f", cfg_file, "--log-level", "warn"])
    DaskGateway.clear_instance()
    ProxyApp.clear_instance()

    assert called_with
    env = called_with.pop()

    assert called_with == [
        _PROXY_EXE,
        "dask-gateway-proxy",
        "-address",
        "127.0.0.1:8866",
        "-tcp-address",
        "127.0.0.1:8867",
        "-api-url",
        "http://127.0.0.1:8888/api/v1/routes",
        "-log-level",
        "warn",
    ]

    assert "DASK_GATEWAY_PROXY_TOKEN" in env
