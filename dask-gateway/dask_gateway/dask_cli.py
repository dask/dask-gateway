from __future__ import print_function, division, absolute_import

import argparse
import json
import logging
import os
import sys
from urllib.parse import urlparse, quote

from tornado import gen, web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, TimeoutError
from distributed import Scheduler, Nanny
from distributed.security import Security
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers, uri_from_host_port
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)
from distributed.diagnostics.plugin import SchedulerPlugin

from . import __version__ as VERSION


def init_logger(log_level="INFO"):
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level.upper())
    logger.handlers[:] = []
    logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = init_logger()


class ScaleDownHandler(web.RequestHandler):
    def initialize(self, service):
        self.service = service

    def prepare(self):
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json_data = json.loads(self.request.body)
        else:
            self.json_data = None

    async def post(self):
        try:
            remove_count = self.json_data["remove_count"]
        except (TypeError, KeyError):
            raise web.HTTPError(405)
        result = await self.service.scale_down(remove_count)
        self.write(result)
        self.set_status(201)


class GatewaySchedulerService(object):
    def __init__(self, scheduler, io_loop=None, plugin=None):
        self.scheduler = scheduler
        self.loop = io_loop or scheduler.loop
        routes = [("/api/scale_down", ScaleDownHandler, {"service": self})]
        self.app = web.Application(routes)
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
    def __init__(self):
        self.api_url = os.environ.get("DASK_GATEWAY_API_URL")
        self.cluster_name = os.environ.get("DASK_GATEWAY_CLUSTER_NAME")
        self.token = os.environ.get("DASK_GATEWAY_API_TOKEN")

        tls_cert = os.environ.get("DASK_GATEWAY_TLS_CERT")
        tls_key = os.environ.get("DASK_GATEWAY_TLS_KEY")
        self.security = Security(
            tls_ca_file=tls_cert,
            tls_scheduler_cert=tls_cert,
            tls_scheduler_key=tls_key,
            tls_worker_cert=tls_cert,
            tls_worker_key=tls_key,
        )

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


def scheduler():
    _ = scheduler_parser.parse_args()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    gateway = GatewayClient()
    loop = IOLoop.current()
    plugin = GatewaySchedulerPlugin(gateway, loop)

    services = {("gateway", 0): (GatewaySchedulerService, {"plugin": plugin})}
    bokeh = False
    with ignoring(ImportError):
        from distributed.bokeh.scheduler import BokehScheduler

        services[("bokeh", 0)] = (BokehScheduler, {})
        bokeh = True

    addr = uri_from_host_port("tls://", None, 0)
    scheduler = Scheduler(loop=loop, services=services, security=gateway.security)
    scheduler.add_plugin(plugin)
    scheduler.start(addr)

    install_signal_handlers(loop)

    host = urlparse(scheduler.address).hostname
    gateway_port = scheduler.services["gateway"].port
    api_address = "http://%s:%d" % (host, gateway_port)

    if bokeh:
        bokeh_port = scheduler.services["bokeh"].port
        bokeh_address = "http://%s:%d" % (host, bokeh_port)
    else:
        bokeh_address = ""

    loop.run_sync(
        lambda: gateway.send_addresses(scheduler.address, bokeh_address, api_address)
    )

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()


worker_parser = argparse.ArgumentParser(
    prog="dask-gateway-worker", description="Start a dask-gateway worker"
)

worker_parser.add_argument("--version", action="version", version=VERSION)
worker_parser.add_argument(
    "--nthreads", type=int, default=1, help="The number of threads to use"
)
worker_parser.add_argument(
    "--memory-limit", default=None, help="The maximum amount of memory to allow"
)
worker_parser.add_argument("--name", default=None, help="The worker name")


def worker():
    args = worker_parser.parse_args()

    nthreads = args.nthreads
    memory_limit = args.memory_limit
    name = args.name or os.environ.get("DASK_GATEWAY_WORKER_NAME")

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    loop = IOLoop.current()

    gateway = GatewayClient()
    scheduler = loop.run_sync(gateway.get_scheduler_address)

    worker = Nanny(
        scheduler,
        ncores=nthreads,
        loop=loop,
        memory_limit=memory_limit,
        worker_port=0,
        security=gateway.security,
        name=name,
    )

    @gen.coroutine
    def close(signalnum):
        worker._close(timeout=2)

    install_signal_handlers(loop, cleanup=close)

    @gen.coroutine
    def run():
        yield worker._start(None)
        while worker.status != "closed":
            yield gen.sleep(0.2)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
