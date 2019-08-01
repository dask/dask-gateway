from __future__ import print_function, division, absolute_import

import argparse
import json
import logging
import os
import sys
from urllib.parse import urlparse, quote

from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, TimeoutError

from distributed import Scheduler, Worker, Nanny
from distributed.security import Security
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)
from distributed.diagnostics.plugin import SchedulerPlugin

from . import __version__ as VERSION


logger = logging.getLogger(__name__)


class BaseHandler(web.RequestHandler):
    @property
    def gateway_service(self):
        return self.settings.get("gateway_service")

    @property
    def auth_token(self):
        return self.settings.get("auth_token")

    def prepare(self):
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json_data = json.loads(self.request.body)
        else:
            self.json_data = None

    def get_current_user(self):
        auth_header = self.request.headers.get("Authorization")
        if auth_header:
            auth_type, auth_token = auth_header.split(" ", 1)
            if auth_type == "token" and auth_token == self.auth_token:
                return "gateway"
        return None


class ScaleDownHandler(BaseHandler):
    @web.authenticated
    async def post(self):
        try:
            remove_count = self.json_data["remove_count"]
        except (TypeError, KeyError):
            raise web.HTTPError(405)
        result = await self.gateway_service.scale_down(remove_count)
        self.write(result)
        self.set_status(201)


class StatusHandler(BaseHandler):
    @web.authenticated
    async def get(self):
        result = self.gateway_service.status()
        self.write(result)
        self.set_status(201)


class GatewaySchedulerService(object):
    def __init__(self, scheduler, io_loop=None, plugin=None):
        self.scheduler = scheduler
        self.loop = io_loop or scheduler.loop
        routes = [("/api/scale_down", ScaleDownHandler), ("/api/status", StatusHandler)]
        self.app = web.Application(
            routes, gateway_service=self, auth_token=plugin.gateway.token
        )
        self.server = None
        self.plugin = plugin

    def listen(self, address):
        ip, port = address
        self.server = self.app.listen(address=ip, port=port)
        ports = {s.getsockname()[1] for s in self.server._sockets.values()}
        assert len(ports) == 1, "Only a single port allowed"
        self.port = ports.pop()

    def stop(self):
        if self.server is not None:
            self.server.stop()
            self.server = None

    async def scale_down(self, remove_count):
        n_workers = len(self.scheduler.workers)
        n_to_close = min(n_workers, remove_count)
        logger.info(
            "Scale down [remove_count: %d, n_workers: %d]", remove_count, n_workers
        )
        if n_to_close > 0:
            to_close = self.scheduler.workers_to_close(n=n_to_close)
            self.plugin.shutdown_requested.update(to_close)
            closed = await self.scheduler.retire_workers(
                workers=to_close, remove=True, close_workers=True
            )
            logger.info(
                "Closed %d workers, of %d suggested", len(closed), len(to_close)
            )
            workers_closed = [v["name"] for v in closed.values()]
        else:
            workers_closed = []
        return {"workers_closed": workers_closed, "n_workers": n_workers}

    def status(self):
        workers = [
            ws.name for ws in self.scheduler.workers.values() if ws.status != "closed"
        ]
        return {"workers": workers}


class GatewaySchedulerPlugin(SchedulerPlugin):
    """A plugin to notify the gateway when workers are added or removed"""

    def __init__(self, gateway, loop):
        self.gateway = gateway
        # A mapping from name to WorkerState. All workers have a specified
        # unique name that will be consistent between worker restarts. The
        # worker address may not be consistent though, so we track intermittent
        # connection failures from the worker name only.
        self.workers = {}
        # A set of worker *addresses* whose shutdown is requested
        self.shutdown_requested = set()
        self.timeouts = {}
        self.loop = loop

    def add_worker(self, scheduler, worker):
        ws = scheduler.workers[worker]
        logger.debug("Worker added [address: %r, name: %r]", ws.address, ws.name)
        timeout = self.timeouts.pop(ws.name, None)
        if timeout is not None:
            # Existing timeout running for this worker, cancel it
            self.loop.remove_timeout(timeout)
        self.workers[worker] = ws
        self.loop.add_callback(self.gateway.notify_worker_added, ws)

    def remove_worker(self, scheduler, worker):
        ws = self.workers.pop(worker)
        logger.debug("Worker removed [address: %r, name: %r]", ws.address, ws.name)

        try:
            self.shutdown_requested.remove(ws.address)
            # This worker was expected to exit, no need to notify
            return
        except KeyError:
            # Unexpected worker shutdown
            pass

        # Start a timer to notify the gateway that the worker is permanently
        # gone. This allows for short-lived communication failures, while
        # still detecting worker failures.
        async def callback():
            logger.debug("Notifying worker %s removed", ws.name)
            await self.gateway.notify_worker_removed(ws)
            self.timeouts.pop(ws.name, None)

        self.timeouts[ws.name] = self.loop.call_later(5, callback)


class GatewayClient(object):
    def __init__(self, cluster_name, api_token, api_url):
        self.cluster_name = cluster_name
        self.token = api_token
        self.api_url = api_url

    async def send_addresses(self, scheduler, dashboard, api):
        client = AsyncHTTPClient()
        body = json.dumps(
            {
                "scheduler_address": scheduler,
                "dashboard_address": dashboard,
                "api_address": api,
            }
        )
        url = "%s/clusters/%s/addresses" % (self.api_url, self.cluster_name)
        req = HTTPRequest(
            url,
            method="PUT",
            headers={
                "Authorization": "token %s" % self.token,
                "Content-type": "application/json",
            },
            body=body,
        )
        await client.fetch(req)

    async def get_scheduler_address(self):
        client = AsyncHTTPClient()
        url = "%s/clusters/%s/addresses" % (self.api_url, self.cluster_name)
        req = HTTPRequest(
            url, method="GET", headers={"Authorization": "token %s" % self.token}
        )
        resp = await client.fetch(req)
        data = json.loads(resp.body.decode("utf8", "replace"))
        return data["scheduler_address"]

    async def notify_worker_added(self, ws):
        client = AsyncHTTPClient()
        body = json.dumps({"name": ws.name, "address": ws.address})
        url = "%s/clusters/%s/workers/%s" % (
            self.api_url,
            self.cluster_name,
            quote(ws.name),
        )
        req = HTTPRequest(
            url,
            method="PUT",
            headers={
                "Authorization": "token %s" % self.token,
                "Content-type": "application/json",
            },
            body=body,
        )
        await client.fetch(req)

    async def notify_worker_removed(self, ws):
        client = AsyncHTTPClient()
        url = "%s/clusters/%s/workers/%s" % (
            self.api_url,
            self.cluster_name,
            quote(ws.name),
        )
        req = HTTPRequest(
            url, method="DELETE", headers={"Authorization": "token %s" % self.token}
        )
        await client.fetch(req)


scheduler_parser = argparse.ArgumentParser(
    prog="dask-gateway-scheduler", description="Start a dask-gateway scheduler"
)
scheduler_parser.add_argument("--version", action="version", version=VERSION)


def getenv(key):
    out = os.environ.get(key)
    if not out:
        logger.error("Environment variable %r not found, shutting down", key)
        sys.exit(1)
    return out


def make_security(tls_cert=None, tls_key=None):
    tls_cert = tls_cert or getenv("DASK_GATEWAY_TLS_CERT")
    tls_key = tls_key or getenv("DASK_GATEWAY_TLS_KEY")

    return Security(
        tls_ca_file=tls_cert,
        tls_scheduler_cert=tls_cert,
        tls_scheduler_key=tls_key,
        tls_worker_cert=tls_cert,
        tls_worker_key=tls_key,
    )


def make_gateway_client(cluster_name=None, api_url=None, api_token=None):
    cluster_name = cluster_name or getenv("DASK_GATEWAY_CLUSTER_NAME")
    api_url = api_url or getenv("DASK_GATEWAY_API_URL")
    api_token = api_token or getenv("DASK_GATEWAY_API_TOKEN")
    return GatewayClient(cluster_name, api_token, api_url)


def scheduler(argv=None):
    scheduler_parser.parse_args(argv)

    gateway = make_gateway_client()
    security = make_security()

    loop = IOLoop.current()

    install_signal_handlers(loop)
    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    async def run():
        scheduler = await start_scheduler(gateway, security)
        await scheduler.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


async def start_scheduler(gateway, security, exit_on_failure=True):
    loop = IOLoop.current()
    plugin = GatewaySchedulerPlugin(gateway, loop)

    services = {("gateway", 0): (GatewaySchedulerService, {"plugin": plugin})}
    dashboard = False
    with ignoring(ImportError):
        from distributed.dashboard.scheduler import BokehScheduler

        services[("dashboard", 0)] = (BokehScheduler, {})
        dashboard = True

    scheduler = Scheduler(loop=loop, services=services, security=security)
    scheduler.add_plugin(plugin)
    await scheduler

    host = urlparse(scheduler.address).hostname
    gateway_port = scheduler.services["gateway"].port
    api_address = "http://%s:%d" % (host, gateway_port)

    if dashboard:
        dashboard_port = scheduler.services["dashboard"].port
        dashboard_address = "http://%s:%d" % (host, dashboard_port)
    else:
        dashboard_address = ""

    try:
        await gateway.send_addresses(scheduler.address, dashboard_address, api_address)
    except Exception as exc:
        logger.error("Failed to send addresses to gateway", exc_info=exc)
        if exit_on_failure:
            sys.exit(1)

    return scheduler


worker_parser = argparse.ArgumentParser(
    prog="dask-gateway-worker", description="Start a dask-gateway worker"
)

worker_parser.add_argument("--version", action="version", version=VERSION)
worker_parser.add_argument(
    "--nthreads", type=int, default=1, help="The number of threads to use"
)
worker_parser.add_argument(
    "--memory-limit", default="auto", help="The maximum amount of memory to allow"
)
worker_parser.add_argument("--name", default=None, help="The worker name")


async def start_worker(
    gateway,
    security,
    worker_name,
    nthreads=1,
    memory_limit="auto",
    local_directory="",
    nanny=True,
):
    loop = IOLoop.current()

    scheduler = await gateway.get_scheduler_address()

    typ = Nanny if nanny else Worker

    worker = typ(
        scheduler,
        loop=loop,
        nthreads=nthreads,
        memory_limit=memory_limit,
        security=security,
        name=worker_name,
        local_directory=local_directory,
    )

    if nanny:

        async def close(signalnum):
            await worker.close(timeout=2)

        install_signal_handlers(loop, cleanup=close)

    await worker
    return worker


def worker(argv=None):
    args = worker_parser.parse_args(argv)

    worker_name = args.name or getenv("DASK_GATEWAY_WORKER_NAME")
    nthreads = args.nthreads
    memory_limit = args.memory_limit

    gateway = make_gateway_client()
    security = make_security()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    loop = IOLoop.current()

    async def run():
        worker = await start_worker(
            gateway, security, worker_name, nthreads, memory_limit
        )
        await worker.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
