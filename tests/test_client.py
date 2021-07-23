import time

import aiohttp
import yarl
import pytest
import dask
from dask_gateway.auth import get_auth, BasicAuth, KerberosAuth, JupyterHubAuth
from dask_gateway.client import Gateway, GatewayCluster, cleanup_lingering_clusters
from dask_gateway_server.compat import get_running_loop

from .utils_test import temp_gateway


def test_get_auth():
    # Pass through existing auth objects
    auth = BasicAuth()
    assert get_auth(auth) is auth

    # Auth by keyword name
    auth = get_auth("basic")
    assert isinstance(auth, BasicAuth)

    auth = get_auth("kerberos")
    assert isinstance(auth, KerberosAuth)

    # Auth from config
    config = {"gateway": {"auth": {"type": "basic", "kwargs": {}}}}
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)

    # Auth from config with import path
    config = {
        "gateway": {"auth": {"type": "dask_gateway.auth.BasicAuth", "kwargs": {}}}
    }
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)

    # Auth from config with kwargs
    config = {"gateway": {"auth": {"type": "basic", "kwargs": {"username": "bruce"}}}}
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)
        assert auth.username == "bruce"

    # Errors
    with pytest.raises(TypeError):
        get_auth(1)

    with pytest.raises(TypeError):
        get_auth(lambda: 1)

    with pytest.raises(ImportError):
        get_auth("dask_gateway.auth.Foo")

    with pytest.raises(ImportError):
        get_auth("not_a_real_module_name_foo_barrr")


def test_config_auth_kwargs_template_environment_vars(monkeypatch):
    monkeypatch.setenv("TEST_USER", "bruce")
    config = {
        "gateway": {"auth": {"type": "basic", "kwargs": {"username": "{TEST_USER}"}}}
    }
    with dask.config.set(config):
        auth = get_auth()
        assert isinstance(auth, BasicAuth)
        assert auth.username == "bruce"


def test_jupyterhub_auth(monkeypatch):
    with pytest.raises(ValueError) as exc:
        get_auth("jupyterhub")
    assert "JUPYTERHUB_API_TOKEN" in str(exc.value)

    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "abcde")
    auth = get_auth("jupyterhub")
    assert auth.api_token == "abcde"
    assert isinstance(auth, JupyterHubAuth)

    # Parameters override environment variable
    assert JupyterHubAuth(api_token="other").api_token == "other"


def test_client_init():
    config = {
        "gateway": {
            "address": "http://127.0.0.1:8888",
            "public-address": None,
            "proxy-address": 8786,
            "auth": {"type": "basic", "kwargs": {"username": "bruce"}},
            "http-client": {"proxy": None},
        }
    }

    with dask.config.set(config):
        # Defaults
        gateway = Gateway()
        assert gateway.address == "http://127.0.0.1:8888"
        assert gateway._public_address == "http://127.0.0.1:8888"
        assert gateway.proxy_address == "gateway://127.0.0.1:8786"
        assert gateway.auth.username == "bruce"

        # Address override
        gateway = Gateway(address="http://127.0.0.1:9999")
        assert gateway.address == "http://127.0.0.1:9999"

        # Proxy address override
        gateway = Gateway(proxy_address="gateway://123.4.5.6:9999")
        assert gateway.proxy_address == "gateway://123.4.5.6:9999"

        # Auth override
        gateway = Gateway(auth="kerberos")
        assert isinstance(gateway.auth, KerberosAuth)

    config = {
        "gateway": {
            "address": None,
            "public-address": None,
            "proxy-address": 8786,
            "auth": {"type": "basic", "kwargs": {}},
            "http-client": {"proxy": None},
        }
    }

    with dask.config.set(config):
        # No address provided
        with pytest.raises(ValueError):
            Gateway()

    config["gateway"]["address"] = "http://127.0.0.1:8888"
    config["gateway"]["proxy-address"] = None
    config["gateway"]["public-address"] = "https://public.com/dask"

    with dask.config.set(config):
        g = Gateway()
        assert g.proxy_address == "gateway://127.0.0.1:8888"
        assert g._public_address == "https://public.com/dask"


def test_gateway_proxy_address_infer_port():
    with dask.config.set(gateway__proxy_address=None):
        g = Gateway("http://localhost")
        assert g.proxy_address == "gateway://localhost:80"

        g = Gateway("https://localhost")
        assert g.proxy_address == "gateway://localhost:443"


def test_gateway_addresses_template_environment_vars(monkeypatch):
    monkeypatch.setenv("TEST", "foobar")

    with dask.config.set(
        gateway__address="http://{TEST}:80",
        gateway__proxy_address=8785,
        gateway__public_address="/{TEST}/foo/",
    ):
        g = Gateway()
    assert g.address == "http://foobar:80"
    assert g.proxy_address == "gateway://foobar:8785"
    assert g._public_address == "/foobar/foo"

    with dask.config.set(gateway__proxy_address="gateway://{TEST}:8787"):
        g = Gateway("http://test.com")
    assert g.address == "http://test.com"
    assert g.proxy_address == "gateway://foobar:8787"


def test_http_client_proxy_false(monkeypatch):
    with dask.config.set(gateway__http_client__proxy=False):
        monkeypatch.setenv("http_proxy", "http://alice:password@host:80/path")

        # http_proxy environment variable ignored
        g = Gateway("http://myhost:80")
        assert g._request_kwargs == {"proxy": None, "proxy_auth": None}


def test_http_client_proxy_true(monkeypatch):
    http_proxy = "http://alice:password@host:80/path"
    proxy_sol = yarl.URL("http://host:80/path")
    proxy_auth_sol = aiohttp.BasicAuth("alice", "password")

    with dask.config.set(gateway__http_client__proxy=True):
        with monkeypatch.context() as m:
            for k in ["http_proxy", "https_proxy"]:
                m.delenv(k, raising=False)
                m.delenv(k.upper(), raising=False)

            with m.context() as m2:
                m2.setenv("http_proxy", http_proxy)

                # Properly inferred from environment
                g = Gateway("http://myhost:80")
                assert g._request_kwargs["proxy"] == proxy_sol
                assert g._request_kwargs["proxy_auth"] == proxy_auth_sol

                # No HTTPS proxy set
                g = Gateway("https://myhost:80")
                assert g._request_kwargs == {"proxy": None, "proxy_auth": None}

            # No HTTP proxy set
            g = Gateway("http://myhost:80")
            assert g._request_kwargs == {"proxy": None, "proxy_auth": None}


def test_http_client_proxy_explicit(monkeypatch):
    http_proxy = "http://alice:password@host:80/path"
    proxy_sol = yarl.URL("http://host:80/path")
    proxy_auth_sol = aiohttp.BasicAuth("alice", "password")

    with dask.config.set(gateway__http_client__proxy=http_proxy):
        with monkeypatch.context() as m:
            m.setenv("http_proxy", "http://bob:foobar@otherhost:90/path")

            # Loaded from config, not environment variables
            for scheme in ["http", "https"]:
                g = Gateway(f"{scheme}://myhost:80")
                assert g._request_kwargs["proxy"] == proxy_sol
                assert g._request_kwargs["proxy_auth"] == proxy_auth_sol


@pytest.mark.asyncio
async def test_get_versions():
    from dask_gateway_server import __version__ as server_version
    from dask_gateway import __version__ as client_version

    async with temp_gateway() as g:
        async with g.gateway_client() as gateway:
            versions = await gateway.get_versions()
            assert versions["client"]["version"] == client_version
            assert versions["server"]["version"] == server_version


@pytest.mark.asyncio
async def test_client_reprs():
    async with temp_gateway() as g:
        async with g.gateway_client() as gateway:
            cluster = await gateway.new_cluster()

            # Plain repr
            assert cluster.name in repr(cluster)

            # HTML repr with dashboard
            cluster.dashboard_link = f"{g.address}/clusters/{cluster.name}/status"
            assert cluster.name in cluster._repr_html_()
            assert cluster.dashboard_link in cluster._repr_html_()

            # Client dashboard link
            client = cluster.get_client()
            assert client.dashboard_link == cluster.dashboard_link

            # HTML repr with no dashboard
            cluster.dashboard_link = None
            assert "Not Available" in cluster._repr_html_()
            await cluster.shutdown()


@pytest.mark.asyncio
async def test_cluster_widget():
    pytest.importorskip("ipywidgets")

    def test():
        with GatewayCluster(
            address=g.address, proxy_address=g.proxy_address
        ) as cluster:
            # Smoke test widget
            cluster._widget()

            template = "<tr><th>Workers</th> <td>%d</td></tr>"
            assert (template % 0) in cluster._widget_status()

            cluster.scale(1)
            timeout = time.time() + 30
            while time.time() < timeout:
                if cluster.scheduler_info.get("workers"):
                    break
                time.sleep(0.25)
            else:
                assert False, "didn't scale up in time"

            assert (template % 1) in cluster._widget_status()

    async with temp_gateway() as g:
        loop = get_running_loop()
        await loop.run_in_executor(None, test)


@pytest.mark.asyncio
async def test_dashboard_link_from_public_address():
    pytest.importorskip("bokeh")

    async with temp_gateway() as g:
        with dask.config.set(
            gateway__address=g.address,
            gateway__public_address="/services/dask-gateway/",
            gateway__proxy_address=g.proxy_address,
        ):
            async with Gateway(asynchronous=True) as gateway:
                assert gateway._public_address == "/services/dask-gateway"

                cluster = await gateway.new_cluster()

                sol = "/services/dask-gateway/clusters/%s/status" % cluster.name
                assert cluster.dashboard_link == sol

                clusters = await gateway.list_clusters()
                for c in clusters:
                    assert c.dashboard_link.startswith("/services/dask-gateway")


@pytest.mark.asyncio
async def test_create_cluster_with_GatewayCluster_constructor():
    async with temp_gateway() as g:
        async with GatewayCluster(
            address=g.address, proxy_address=g.proxy_address, asynchronous=True
        ) as cluster:

            # Cluster is now present in list
            clusters = await cluster.gateway.list_clusters()
            assert len(clusters)
            assert clusters[0].name == cluster.name

            await cluster.scale(1)

            async with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

        assert cluster.status == "closed"

        async with g.gateway_client() as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters


@pytest.mark.asyncio
async def test_sync_constructors():
    def test():
        with g.gateway_client(asynchronous=False) as gateway:
            with gateway.new_cluster() as cluster:
                cluster.scale(1)
                client = cluster.get_client()
                res = client.submit(lambda x: x + 1, 1).result()
                assert res == 2

                with gateway.connect(cluster.name) as cluster2:
                    client2 = cluster2.get_client()
                    res = client2.submit(lambda x: x + 1, 1).result()
                    assert res == 2

    async with temp_gateway() as g:
        loop = get_running_loop()
        await loop.run_in_executor(None, test)


@pytest.mark.asyncio
async def test_GatewayCluster_shutdown_on_close():
    async with temp_gateway() as g:

        def test():
            cluster = GatewayCluster(address=g.address, proxy_address=g.proxy_address)
            assert cluster.shutdown_on_close
            assert cluster in GatewayCluster._instances

        loop = get_running_loop()
        await loop.run_in_executor(None, test)

        assert len(GatewayCluster._instances) == 0

        async with g.gateway_client() as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters


@pytest.mark.asyncio
async def test_GatewayCluster_client_error_doesnt_prevent_cleanup():
    """Check that an error on closing clients doesn't prevent cluster shutdown"""
    async with temp_gateway() as g:

        class BadGatewayCluster(GatewayCluster):
            async def _stop_async(self):
                await super()._stop_async()
                raise ValueError("OH NO")

        def test():
            cluster = BadGatewayCluster(
                address=g.address, proxy_address=g.proxy_address
            )
            assert cluster in GatewayCluster._instances

        loop = get_running_loop()
        await loop.run_in_executor(None, test)

        assert len(GatewayCluster._instances) == 0

        async with g.gateway_client() as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters


@pytest.mark.asyncio
async def test_GatewayCluster_cleanup_atexit():
    async with temp_gateway() as g:

        def test():
            return GatewayCluster(address=g.address, proxy_address=g.proxy_address)

        loop = get_running_loop()
        cluster = await loop.run_in_executor(None, test)

        assert len(GatewayCluster._instances) == 1

        def test_cleanup():
            # No warnings raised by cleanup function
            with pytest.warns(None) as rec:
                cleanup_lingering_clusters()
            for r in rec:
                assert not issubclass(r.category, UserWarning)

            # Cluster is now closed
            assert cluster.status == "closed"

            # No harm in double running
            with pytest.warns(None) as rec:
                cleanup_lingering_clusters()
            for r in rec:
                assert not issubclass(r.category, UserWarning)

        await loop.run_in_executor(None, test_cleanup)

        async with g.gateway_client() as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters
