import asyncio
import time

import pytest
import dask
from dask_gateway.auth import get_auth, BasicAuth, KerberosAuth, JupyterHubAuth
from dask_gateway.client import Gateway, GatewayCluster, cleanup_lingering_clusters
from dask_gateway_server.compat import get_running_loop
from dask_gateway_server.managers.inprocess import InProcessClusterManager
from dask_gateway_server.utils import random_port
from tornado import web
from tornado.httpclient import HTTPRequest

from .utils import temp_gateway


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
        }
    }

    with dask.config.set(config):
        # No address provided
        with pytest.raises(ValueError):
            Gateway()

    config["gateway"]["address"] = "http://127.0.0.1:8888"
    config["gateway"]["proxy-address"] = None

    with dask.config.set(config):
        # No proxy-address provided
        with pytest.raises(ValueError):
            Gateway()


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


class SlowHandler(web.RequestHandler):
    async def get(self):
        self.waiter = asyncio.ensure_future(asyncio.sleep(30))
        self.settings["task_set"].add(self.waiter)
        try:
            await self.waiter
        except asyncio.CancelledError:
            return
        self.write("Hello world")

    def on_connection_close(self):
        if hasattr(self, "waiter"):
            self.waiter.cancel()


class slow_server(object):
    def __init__(self):
        self.port = random_port()
        self.address = "http://127.0.0.1:%d" % self.port
        self.tasks = set()
        self.app = web.Application([(r"/", SlowHandler)], task_set=self.tasks)

    async def __aenter__(self):
        self.server = self.app.listen(self.port)
        return self

    async def __aexit__(self, *args):
        self.server.stop()
        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_client_fetch_timeout():
    async with slow_server() as server:
        gateway = Gateway(server.address, auth=BasicAuth("alice"))
        with pytest.raises(TimeoutError):
            await gateway._fetch(HTTPRequest(url=server.address, request_timeout=1))


@pytest.mark.asyncio
async def test_client_reprs(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            cluster = await gateway.new_cluster()

            # Plain repr
            assert cluster.name in repr(cluster)

            # HTML repr with dashboard
            cluster.dashboard_link = "%s/gateway/clusters/%s/status" % (
                gateway_proc.public_url,
                cluster.name,
            )
            assert cluster.name in cluster._repr_html_()
            assert cluster.dashboard_link in cluster._repr_html_()

            # HTML repr with no dashboard
            cluster.dashboard_link = None
            assert "Not Available" in cluster._repr_html_()


@pytest.mark.asyncio
async def test_cluster_widget(tmpdir):
    pytest.importorskip("ipywidgets")

    def test():
        with GatewayCluster(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
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

    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        loop = get_running_loop()
        await loop.run_in_executor(None, test)


@pytest.mark.asyncio
async def test_dashboard_link_from_public_address(tmpdir):
    pytest.importorskip("bokeh")

    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        with dask.config.set(
            gateway__address=gateway_proc.public_urls.connect_url,
            gateway__public_address="/services/dask-gateway/",
            gateway__proxy_address=gateway_proc.gateway_urls.connect_url,
        ):
            async with Gateway(asynchronous=True) as gateway:
                assert gateway._public_address == "/services/dask-gateway"

                cluster = await gateway.new_cluster()

                sol = "/services/dask-gateway/gateway/clusters/%s/status" % cluster.name
                assert cluster.dashboard_link == sol

                clusters = await gateway.list_clusters()
                for c in clusters:
                    assert c.dashboard_link.startswith("/services/dask-gateway")


@pytest.mark.asyncio
async def test_create_cluster_with_GatewayCluster_constructor(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:
        async with GatewayCluster(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as cluster:

            # Cluster is now present in list
            clusters = await cluster.gateway.list_clusters()
            assert len(clusters)
            assert clusters[0].name == cluster.name

            await cluster.scale(1)

            with cluster.get_client(set_as_default=False) as client:
                res = await client.submit(lambda x: x + 1, 1)
                assert res == 2

        assert cluster.status == "closed"

        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters


@pytest.mark.asyncio
async def test_GatewayCluster_shutdown_on_close(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        def test():
            cluster = GatewayCluster(
                address=gateway_proc.public_urls.connect_url,
                proxy_address=gateway_proc.gateway_urls.connect_url,
            )
            assert cluster.shutdown_on_close
            assert cluster in GatewayCluster._instances

        loop = get_running_loop()
        await loop.run_in_executor(None, test)

        assert len(GatewayCluster._instances) == 0

        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters


@pytest.mark.asyncio
async def test_GatewayCluster_cleanup_atexit(tmpdir):
    async with temp_gateway(
        cluster_manager_class=InProcessClusterManager,
        temp_dir=str(tmpdir.join("dask-gateway")),
    ) as gateway_proc:

        def test():
            return GatewayCluster(
                address=gateway_proc.public_urls.connect_url,
                proxy_address=gateway_proc.gateway_urls.connect_url,
            )

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

        async with Gateway(
            address=gateway_proc.public_urls.connect_url,
            proxy_address=gateway_proc.gateway_urls.connect_url,
            asynchronous=True,
        ) as gateway:
            # No cluster running
            clusters = await gateway.list_clusters()
            assert not clusters
