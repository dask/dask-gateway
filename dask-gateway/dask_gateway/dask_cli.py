from __future__ import print_function, division, absolute_import

import asyncio
import argparse
import collections
import json
import logging
import os
import sys
from urllib.parse import urlparse

from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, TimeoutError

from distributed import Scheduler, Worker, Nanny
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.security import Security
from distributed.scheduler import logger as scheduler_logger
from distributed.cli.utils import install_signal_handlers
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

from . import __version__ as VERSION
from .utils import cancel_task


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True
logger.parent = scheduler_logger


class Waiter(object):
    def __init__(self):
        self.task = None
        self.timer = None

    async def wait(self, t):
        self.triggered = False
        self.task = asyncio.ensure_future(asyncio.sleep(t))
        try:
            await self.task
        except asyncio.CancelledError:
            if not self.triggered:
                raise
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None
        return

    async def interrupt(self):
        if self.task is not None:
            self.triggered = True
            await cancel_task(self.task)
            self.task = None

    def interrupt_soon(self):
        if self.timer is not None:
            return
        self.timer = asyncio.ensure_future(self._interrupt_soon())

    async def _interrupt_soon(self):
        await asyncio.sleep(3)
        await self.interrupt()


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


class CommHandler(BaseHandler):
    @web.authenticated
    async def post(self):
        msg = self.json_data
        op = msg.pop("op", None)
        if op == "scale":
            count = msg.get("count", 0)
            await self.gateway_service.scale(count)
        elif op == "adapt":
            minimum = msg.get("minimum")
            maximum = msg.get("maximum")
            active = msg.get("active", True)
            await self.gateway_service.adapt(
                minimum=minimum, maximum=maximum, active=active
            )
        else:
            raise web.HTTPError(422, reason="op must be one of {scale, adapt}")
        self.set_status(200)


class HealthHandler(web.RequestHandler):
    async def get(self):
        self.set_status(200)


class GatewayPlugin(SchedulerPlugin):
    def __init__(self, service):
        self.service = service

    def add_worker(self, scheduler, worker):
        self.service.worker_added(worker)

    def remove_worker(self, scheduler, worker):
        self.service.worker_removed(worker)


class GatewaySchedulerService(object):
    def __init__(
        self,
        scheduler,
        io_loop=None,
        gateway=None,
        adaptive_period=3,
        adaptive_window=3,
        target_duration=5,
        idle_timeout=0,
        heartbeat_period=15,
    ):
        self.scheduler = scheduler
        self.gateway = gateway
        self.loop = io_loop or scheduler.loop
        self.count = 0
        self.heartbeat_period = max(0, heartbeat_period)
        if self.heartbeat_period == 0:
            # Liveness not tracked by heartbeats, message only on state changes
            self.heartbeat_max = 3600
            self.heartbeat_initial = 3600
        else:
            self.heartbeat_max = heartbeat_period
            self.heartbeat_initial = 0.25
        self.waiter = Waiter()
        self.idle_timeout = max(0, idle_timeout)
        self.check_idle_task = None
        self.heartbeat_task = None

        # adaptive
        self.minimum = None
        self.maximum = None
        self.target_duration = target_duration
        self.adaptive_period = adaptive_period
        self.adaptive_window = adaptive_window
        self.adapt_task = None

        # internal state
        self.address_to_worker = {}
        self.active_workers = set()
        self.closing_workers = set()
        self.closed_workers = set()
        self.scheduler.add_plugin(GatewayPlugin(self))

        routes = [("/api/comm", CommHandler), ("/api/health", HealthHandler)]
        self.app = web.Application(
            routes, gateway_service=self, auth_token=self.gateway.token
        )
        self.server = None

    def worker_added(self, worker_address):
        ws = self.scheduler.workers[worker_address]
        self.address_to_worker[worker_address] = ws
        self.active_workers.add(ws.name)
        self.closing_workers.discard(ws.name)
        self.closed_workers.discard(ws.name)

        if len(self.active_workers) > self.count:
            self.waiter.interrupt_soon()

    def worker_removed(self, worker_address):
        ws = self.address_to_worker.pop(worker_address)
        self.active_workers.discard(ws.name)
        self.closed_workers.add(ws.name)
        if ws.name in self.closing_workers:
            self.closing_workers.discard(ws.name)
        else:
            # Unexpected failure.
            if self.heartbeat_period:
                # Liveness tracked by heartbeats, notify gateway soon
                self.waiter.interrupt_soon()

    @property
    def dashboard_address(self):
        try:
            return self._dashboard_address
        except AttributeError:
            try:
                host = urlparse(self.scheduler.address).hostname
                port = self.scheduler.services["dashboard"].port
                self._dashboard_address = "http://%s:%d" % (host, port)
            except KeyError:
                self._dashboard_address = ""
        return self._dashboard_address

    def listen(self, address):
        ip, port = address
        self.server = self.app.listen(address=ip, port=port)
        ports = {s.getsockname()[1] for s in self.server._sockets.values()}
        assert len(ports) == 1, "Only a single port allowed"
        self.port = ports.pop()

        host = urlparse(self.scheduler.address).hostname
        self.api_address = f"http://{host}:{self.port}"

        if self.idle_timeout > 0:
            self.check_idle_task = asyncio.ensure_future(self.check_idle())
        self.heartbeat_task = asyncio.ensure_future(self.heartbeat_loop())

    def stop(self):
        if self.server is not None:
            self.server.stop()
            self.server = None
        if self.check_idle_task is not None:
            self.check_idle_task.cancel()
        if self.heartbeat_task is not None:
            self.heartbeat_task.cancel()
        if self.adapt_task is not None:
            self.adapt_task.cancel()

    async def check_idle(self):
        while True:
            await asyncio.sleep(self.idle_timeout / 4)
            if any(ws.processing for ws in self.scheduler.workers.values()):
                return
            if self.scheduler.unrunnable:
                return

            last_action = (
                self.scheduler.transition_log[-1][-1]
                if self.scheduler.transition_log
                else self.scheduler.time_started
            )

            if self.loop.time() - last_action > self.idle_timeout:
                logger.warning(
                    "Cluster has been idle for %.2f seconds, shutting down",
                    self.idle_timeout,
                )
                await self.gateway.shutdown()

    async def heartbeat(self):
        if self.count < len(self.active_workers):
            closing_workers = self.scheduler.workers_to_close(
                target=self.count, attribute="name"
            )
            active_workers = list(self.active_workers.difference(closing_workers))
        else:
            closing_workers = []
            active_workers = list(self.active_workers)

        msg = {
            "scheduler_address": self.scheduler.address,
            "dashboard_address": self.dashboard_address,
            "api_address": self.api_address,
            "count": self.count,
            "active_workers": active_workers,
            "closing_workers": closing_workers,
            "closed_workers": list(self.closed_workers),
        }

        await self.gateway.heartbeat(msg)

        if closing_workers:
            self.closing_workers.update(closing_workers)
            try:
                await self.scheduler.retire_workers(
                    names=closing_workers, remove=True, close_workers=True
                )
            except Exception:
                self.closing_workers.difference_update(closing_workers)
                raise

    async def heartbeat_loop(self):
        base_delay = 0.25
        backoff = base_delay
        period = self.heartbeat_initial
        while True:
            await self.waiter.wait(period)
            try:
                await self.heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Failure. Backoff and try again
                logger.warning("Failed to send heartbeat", exc_info=exc)
                backoff = min(backoff * 2, self.heartbeat_max)
                period = backoff
            else:
                # Success. Reset backoff, heartbeat lazily
                backoff = base_delay
                period = self.heartbeat_max

    async def scale(self, count):
        # When scale is called explicitly, disable adaptive scaling
        await self.adapt(active=False)
        await self._scale(count)

    async def _scale(self, count):
        if count != self.count:
            logger.info("Requesting scale to %s workers from %s", count, self.count)
            self.count = count
            await self.waiter.interrupt()

    async def adapt(self, minimum=None, maximum=None, active=True):
        if active:
            logger.info(
                "Enabling adaptive scaling, minimum=%r, maximum=%r", minimum, maximum
            )
            self.minimum = minimum
            self.maximum = maximum
            if self.adapt_task is None:
                self.adapt_task = asyncio.ensure_future(self.adapt_loop())
        else:
            if self.adapt_task is not None:
                logger.info("Disabling adaptive scaling")
                await cancel_task(self.adapt_task)
                self.adapt_task = None

    async def adapt_loop(self):
        window = collections.deque(maxlen=self.adaptive_window)
        while True:
            try:
                target = self.scheduler.adaptive_target(
                    target_duration=self.target_duration
                )
                if self.minimum is not None:
                    target = max(self.minimum, target)
                if self.maximum is not None:
                    target = min(self.maximum, target)

                window.append(target)

                if target > self.count:
                    await self._scale(target)
                elif target < self.count:
                    max_window = max(window)
                    if max_window < self.count:
                        await self._scale(max_window)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Error in adaptive loop", exc_info=exc)

            await asyncio.sleep(self.adaptive_period)


class GatewayClient(object):
    def __init__(self, cluster_name, api_token, api_url):
        self.cluster_name = cluster_name
        self.token = api_token
        self.api_url = api_url

    async def heartbeat(self, msg):
        client = AsyncHTTPClient()
        req = HTTPRequest(
            method="POST",
            url=f"{self.api_url}/v1/clusters/{self.cluster_name}/heartbeat",
            headers={
                "Authorization": "token %s" % self.token,
                "Content-type": "application/json",
            },
            body=json.dumps(msg),
        )
        await client.fetch(req)

    async def shutdown(self):
        client = AsyncHTTPClient()
        url = f"{self.api_url}/v1/clusters/{self.cluster_name}"
        req = HTTPRequest(
            url, method="DELETE", headers={"Authorization": "token %s" % self.token}
        )
        await client.fetch(req)

    async def get_scheduler_address(self):
        client = AsyncHTTPClient()
        url = f"{self.api_url}/v1/clusters/{self.cluster_name}/addresses"
        req = HTTPRequest(
            url, method="GET", headers={"Authorization": "token %s" % self.token}
        )
        resp = await client.fetch(req)
        data = json.loads(resp.body.decode("utf8", "replace"))
        return data["scheduler_address"]


scheduler_parser = argparse.ArgumentParser(
    prog="dask-gateway-scheduler", description="Start a dask-gateway scheduler"
)
scheduler_parser.add_argument("--version", action="version", version=VERSION)
scheduler_parser.add_argument(
    "--adaptive-period",
    type=float,
    default=3,
    help="Period (in seconds) between adaptive scaling calls",
)
scheduler_parser.add_argument(
    "--heartbeat-period",
    type=float,
    default=15,
    help=(
        "Period (in seconds) between heartbeat calls. Set to 0 to send "
        "heartbeats only when scaling."
    ),
)
scheduler_parser.add_argument(
    "--idle-timeout",
    type=float,
    default=0,
    help="Idle timeout (in seconds) before shutting down the cluster",
)
scheduler_parser.add_argument(
    "--scheduler-address",
    type=str,
    default="tls://:0",
    help="The address the scheduler should listen at. Defaults to `:0`",
)
scheduler_parser.add_argument(
    "--dashboard-address",
    type=str,
    default=":0",
    help="The address the dashboard should listen at. Defaults to `:0`",
)
scheduler_parser.add_argument(
    "--api-address",
    type=str,
    default=":0",
    help="The address the api should listen at. Defaults to `:0`",
)


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
        require_encryption=True,
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
    if "/" in api_token:
        with open(api_token) as f:
            api_token = f.read()
    return GatewayClient(cluster_name, api_token, api_url)


def scheduler(argv=None):
    args = scheduler_parser.parse_args(argv)

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
        scheduler = await start_scheduler(
            gateway,
            security,
            adaptive_period=args.adaptive_period,
            heartbeat_period=args.heartbeat_period,
            idle_timeout=args.idle_timeout,
            scheduler_address=args.scheduler_address,
            dashboard_address=args.dashboard_address,
            api_address=args.api_address,
        )
        await scheduler.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


async def start_scheduler(
    gateway,
    security,
    adaptive_period=3,
    heartbeat_period=15,
    idle_timeout=0,
    scheduler_address="tls://:0",
    dashboard_address=":0",
    api_address=":0",
    exit_on_failure=True,
):
    loop = IOLoop.current()
    services = {
        ("gateway", api_address or 0): (
            GatewaySchedulerService,
            {
                "gateway": gateway,
                "adaptive_period": adaptive_period,
                "heartbeat_period": heartbeat_period,
                "idle_timeout": idle_timeout,
            },
        )
    }
    scheduler = Scheduler(
        host=scheduler_address,
        loop=loop,
        services=services,
        security=security,
        dashboard_address=dashboard_address,
    )
    return await scheduler


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
worker_parser.add_argument(
    "--scheduler-address", default=None, help="The scheduler address"
)


async def start_worker(
    gateway,
    security,
    worker_name,
    nthreads=1,
    memory_limit="auto",
    scheduler_address=None,
    local_directory="",
    nanny=True,
):
    loop = IOLoop.current()

    if not scheduler_address:
        scheduler_address = await gateway.get_scheduler_address()

    typ = Nanny if nanny else Worker

    worker = typ(
        scheduler_address,
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
    scheduler_address = args.scheduler_address

    gateway = make_gateway_client()
    security = make_security()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    loop = IOLoop.current()

    async def run():
        worker = await start_worker(
            gateway, security, worker_name, nthreads, memory_limit, scheduler_address
        )
        await worker.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
