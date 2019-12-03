from __future__ import print_function, division, absolute_import

import asyncio
import argparse
import collections
import json
import logging
import os
import sys
from urllib.parse import urlparse, quote

from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, TimeoutError, PeriodicCallback

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
            target = self.json_data["target"]
        except (TypeError, KeyError):
            raise web.HTTPError(422)
        self.gateway_service.adapt(active=False)
        result = await self.gateway_service.scale_down(target)
        self.write(result)
        self.set_status(201)


class PendingWorkersHandler(BaseHandler):
    @web.authenticated
    async def post(self):
        try:
            op = self.json_data["op"]
            workers = self.json_data["workers"]
        except (TypeError, KeyError):
            raise web.HTTPError(422)
        if op == "add":
            self.gateway_service.adapt(active=False)
            self.gateway_service.add_pending_workers(workers)
        elif op == "remove":
            self.gateway_service.remove_pending_workers(workers)
        self.set_status(204)


class StatusHandler(BaseHandler):
    @web.authenticated
    async def get(self):
        result = self.gateway_service.status()
        self.write(result)
        self.set_status(201)


class AdaptHandler(BaseHandler):
    @web.authenticated
    async def post(self):
        try:
            minimum = self.json_data.get("minimum", None)
            maximum = self.json_data.get("maximum", None)
            active = self.json_data.get("active", True)
        except (TypeError, KeyError):
            raise web.HTTPError(422)
        self.gateway_service.adapt(minimum=minimum, maximum=maximum, active=active)
        self.set_status(204)


class Adaptive(object):
    def __init__(
        self,
        gateway_service,
        minimum=None,
        maximum=None,
        target_duration=5,
        wait_count=3,
        period=3,
    ):
        self.gateway_service = gateway_service
        self.target_duration = target_duration
        self.minimum = minimum
        self.maximum = maximum
        self.wait_count = wait_count
        self.period = period
        self.periodic_callback = PeriodicCallback(self.adapt, self.period * 1000)
        self.gateway_service.scheduler.periodic_callbacks[
            "adaptive-callback"
        ] = self.periodic_callback

        # internal state
        self.close_counts = collections.defaultdict(int)
        self._adapting = False

    @property
    def loop(self):
        return self.gateway_service.loop

    @property
    def plan(self):
        return self.gateway_service.plan

    @property
    def observed(self):
        return self.gateway_service.observed

    def start(self, minimum=None, maximum=None):
        self.minimum = minimum
        self.maximum = maximum
        self.periodic_callback.start()

    def stop(self):
        self.periodic_callback.stop()

    async def target(self):
        target = self.gateway_service.scheduler.adaptive_target(
            target_duration=self.target_duration
        )
        if self.minimum is not None:
            target = max(self.minimum, target)
        if self.maximum is not None:
            target = min(self.maximum, target)
        return target

    def workers_to_close(self, target):
        return self.gateway_service.scheduler.workers_to_close(
            target=target, attribute="name"
        )

    async def scale_down(self, workers):
        await self.gateway_service.remove_workers(workers)

    async def scale_up(self, n):
        await self.gateway_service.scale_up(n)

    def recommendations(self, target):
        current = len(self.plan)
        if target == current:
            self.close_counts.clear()
            return {"status": "same"}

        elif target > current:
            self.close_counts.clear()
            return {"status": "up", "n": target}

        elif target < current:
            pending = self.plan - self.observed
            to_close = set()
            if pending:
                to_close.update(list(pending)[: current - target])

            if target < current - len(to_close):
                to_close.update(self.workers_to_close(target))

            firmly_close = set()
            for w in to_close:
                self.close_counts[w] += 1
                if self.close_counts[w] >= self.wait_count:
                    firmly_close.add(w)

            for k in list(self.close_counts):  # clear out unseen keys
                if k in firmly_close or k not in to_close:
                    del self.close_counts[k]

            if firmly_close:
                return {"status": "down", "workers": list(firmly_close)}
            else:
                return {"status": "same"}

    async def adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return
        self._adapting = True

        try:
            target = await self.target()
            recommendations = self.recommendations(target)

            status = recommendations.pop("status")
            if status == "same":
                return
            if status == "up":
                await self.scale_up(**recommendations)
            if status == "down":
                await self.scale_down(**recommendations)
        except OSError:
            self.stop()
        finally:
            self._adapting = False


class GatewaySchedulerService(object):
    def __init__(
        self, scheduler, io_loop=None, gateway=None, adaptive_period=3, idle_timeout=0
    ):
        self.scheduler = scheduler
        self.adaptive = Adaptive(self, period=adaptive_period)
        self.gateway = gateway
        self.loop = io_loop or scheduler.loop
        self.idle_timeout = max(0, idle_timeout)
        self.check_idle_task = None

        routes = [
            ("/api/scale_down", ScaleDownHandler),
            ("/api/adapt", AdaptHandler),
            ("/api/pending_workers", PendingWorkersHandler),
            ("/api/status", StatusHandler),
        ]
        self.app = web.Application(
            routes, gateway_service=self, auth_token=self.gateway.token
        )
        self.server = None

        self.plugin = GatewaySchedulerPlugin(self)
        self.scheduler.add_plugin(self.plugin)

        # A set of worker names that are active (pending or running)
        self.plan = set()
        # A mapping of running worker *addresses* -> WorkerState
        self.workers = {}
        # A mapping of worker name -> TimerHandle. When a worker is found
        # down, we hold off on notifying the gateway for a period in case this
        # is a temporary connection failure.
        self.timeouts = {}
        # A set of all intentionally closing worker names.
        self.closing = set()

    @property
    def observed(self):
        """A set of all running worker names"""
        return {ws.name for ws in self.workers.values()}

    def listen(self, address):
        ip, port = address
        self.server = self.app.listen(address=ip, port=port)
        ports = {s.getsockname()[1] for s in self.server._sockets.values()}
        assert len(ports) == 1, "Only a single port allowed"
        self.port = ports.pop()

        if self.idle_timeout > 0:
            self.check_idle_task = asyncio.ensure_future(self.check_idle())

    def stop(self):
        if self.server is not None:
            self.server.stop()
            self.server = None
        if self.check_idle_task is not None:
            self.check_idle_task.cancel()

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

    def add_pending_workers(self, workers):
        self.plan.update(workers)

    def remove_pending_workers(self, workers):
        self.plan.difference_update(workers)

    async def scale_up(self, target):
        workers = await self.gateway.scale_up(target)
        self.add_pending_workers(workers)

    async def remove_workers(self, workers):
        self.plan.difference_update(workers)
        self.closing.update(set(workers).union(self.observed))
        await self.scheduler.retire_workers(
            names=workers, remove=True, close_workers=True
        )
        await self.gateway.remove_workers(workers)

    async def scale_down(self, target):
        n_to_close = len(self.plan) - target
        closed = []
        if n_to_close > 0:
            pending = self.plan.difference(self.observed)
            closed.extend(list(pending)[:n_to_close])
            self.plan.difference_update(closed)

            if len(closed) < n_to_close:
                workers = self.scheduler.workers_to_close(
                    target=target, attribute="name"
                )
                self.closing.update(workers)
                await self.scheduler.retire_workers(
                    names=workers, remove=True, close_workers=True
                )
                closed.extend(workers)

        return {"workers_closed": closed}

    def adapt(self, minimum=None, maximum=None, active=True):
        if active:
            self.adaptive.start(minimum=minimum, maximum=maximum)
        else:
            self.adaptive.stop()

    def worker_added(self, worker_address):
        ws = self.scheduler.workers[worker_address]
        logger.debug("Worker added [address: %r, name: %r]", ws.address, ws.name)
        timeout = self.timeouts.pop(ws.name, None)
        if timeout is not None:
            # Existing timeout running for this worker, cancel it
            self.loop.remove_timeout(timeout)
        else:
            self.loop.add_callback(self.gateway.notify_worker_added, ws)
        self.workers[worker_address] = ws

    def worker_removed(self, worker_address):
        ws = self.workers.pop(worker_address)
        logger.debug("Worker removed [address: %r, name: %r]", ws.address, ws.name)

        try:
            self.closing.remove(ws.name)
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

    def status(self):
        workers = [
            ws.name for ws in self.scheduler.workers.values() if ws.status != "closed"
        ]
        return {"workers": workers}


class GatewaySchedulerPlugin(SchedulerPlugin):
    """A plugin to notify the gateway when workers are added or removed"""

    def __init__(self, service):
        self.service = service

    def add_worker(self, scheduler, worker):
        self.service.worker_added(worker)

    def remove_worker(self, scheduler, worker):
        self.service.worker_removed(worker)


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

    async def remove_workers(self, workers):
        client = AsyncHTTPClient()
        body = json.dumps({"remove_workers": workers})
        url = "%s/clusters/%s/scale" % (self.api_url, self.cluster_name)
        req = HTTPRequest(
            url,
            method="POST",
            headers={
                "Authorization": "token %s" % self.token,
                "Content-type": "application/json",
            },
            body=body,
        )
        await client.fetch(req)

    async def scale_up(self, n):
        client = AsyncHTTPClient()
        body = json.dumps({"worker_count": n, "is_adapt": True})
        url = "%s/clusters/%s/scale" % (self.api_url, self.cluster_name)
        req = HTTPRequest(
            url,
            method="POST",
            headers={
                "Authorization": "token %s" % self.token,
                "Content-type": "application/json",
            },
            body=body,
        )
        resp = await client.fetch(req)
        data = json.loads(resp.body.decode("utf8", "replace"))
        return data["added"]

    async def shutdown(self):
        client = AsyncHTTPClient()
        url = "%s/clusters/%s" % (self.api_url, self.cluster_name)
        req = HTTPRequest(
            url, method="DELETE", headers={"Authorization": "token %s" % self.token}
        )
        await client.fetch(req)


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
    "--idle-timeout",
    type=float,
    default=0,
    help="Idle timeout (in seconds) before shutting down the cluster",
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
            idle_timeout=args.idle_timeout,
        )
        await scheduler.finished()

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass


async def start_scheduler(
    gateway, security, adaptive_period=3, idle_timeout=0, exit_on_failure=True
):
    loop = IOLoop.current()
    services = {
        ("gateway", 0): (
            GatewaySchedulerService,
            {
                "gateway": gateway,
                "adaptive_period": adaptive_period,
                "idle_timeout": idle_timeout,
            },
        )
    }
    dashboard = False
    with ignoring(ImportError):
        from distributed.dashboard.scheduler import BokehScheduler

        services[("dashboard", 0)] = (BokehScheduler, {})
        dashboard = True

    scheduler = Scheduler(loop=loop, services=services, security=security)
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
