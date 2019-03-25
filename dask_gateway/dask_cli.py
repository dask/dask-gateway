from __future__ import print_function, division, absolute_import

import argparse
import json
import os
import sys
from urllib.parse import urlparse

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, TimeoutError
from distributed import Scheduler, Nanny
from distributed.security import Security
from distributed.utils import ignoring
from distributed.cli.utils import install_signal_handlers, uri_from_host_port
from distributed.proctitle import (enable_proctitle_on_children,
                                   enable_proctitle_on_current)

from . import __version__ as VERSION


def get_url():
    api_url = os.environ.get('DASK_GATEWAY_API_URL', '')
    cluster_name = os.environ.get('DASK_GATEWAY_CLUSTER_NAME', '')
    return '%s/clusters/%s/addresses' % (api_url, cluster_name)


def get_token():
    return os.environ.get('DASK_GATEWAY_API_TOKEN', '')


def get_security():
    tls_cert = os.environ.get('DASK_GATEWAY_TLS_CERT', '')
    tls_key = os.environ.get('DASK_GATEWAY_TLS_KEY', '')
    return Security(tls_ca_file=tls_cert,
                    tls_scheduler_cert=tls_cert,
                    tls_scheduler_key=tls_key,
                    tls_worker_cert=tls_cert,
                    tls_worker_key=tls_key)


async def send_addresses(scheduler, dashboard):
    client = AsyncHTTPClient()
    body = json.dumps({'scheduler_address': scheduler,
                       'dashboard_address': dashboard})
    req = HTTPRequest(get_url(),
                      method='PUT',
                      headers={'Authorization': 'token %s' % get_token(),
                               'Content-type': 'application/json'},
                      body=body)
    await client.fetch(req)


async def get_scheduler_address():
    client = AsyncHTTPClient()
    req = HTTPRequest(get_url(),
                      method='GET',
                      headers={'Authorization': 'token %s' % get_token()})
    resp = await client.fetch(req)
    data = json.loads(resp.body.decode('utf8', 'replace'))
    return data['scheduler_address']


scheduler_parser = argparse.ArgumentParser(
    prog="dask-gateway-scheduler",
    description="Start a dask-gateway scheduler"
)
scheduler_parser.add_argument("--version", action="version", version=VERSION)


def scheduler():
    _ = scheduler_parser.parse_args()

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if sys.platform.startswith('linux'):
        import resource   # module fails importing on Windows
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    services = {}
    bokeh = False
    with ignoring(ImportError):
        from distributed.bokeh.scheduler import BokehScheduler
        services[('bokeh', 0)] = (BokehScheduler, {})
        bokeh = True

    loop = IOLoop.current()
    addr = uri_from_host_port('tls://', None, 0)
    security = get_security()

    scheduler = Scheduler(loop=loop, services=services, security=security)
    scheduler.start(addr)

    install_signal_handlers(loop)

    if bokeh:
        bokeh_port = scheduler.services['bokeh'].port
        bokeh_host = urlparse(scheduler.address).hostname
        bokeh_address = 'http://%s:%d' % (bokeh_host, bokeh_port)
    else:
        bokeh_address = ''

    loop.run_sync(lambda: send_addresses(scheduler.address, bokeh_address))

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()


worker_parser = argparse.ArgumentParser(
    prog="dask-gateway-worker",
    description="Start a dask-gateway worker"
)

worker_parser.add_argument("--version", action="version", version=VERSION)
worker_parser.add_argument("--nthreads", type=int, default=1,
                           help="The number of threads to use")
worker_parser.add_argument("--memory-limit", default=None,
                           help="The maximum amount of memory to allow")


def worker():
    args = worker_parser.parse_args()

    nthreads = args.nthreads
    memory_limit = args.memory_limit

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    loop = IOLoop.current()

    scheduler = loop.run_sync(get_scheduler_address)

    security = get_security()

    worker = Nanny(scheduler, ncores=nthreads, loop=loop,
                   memory_limit=memory_limit, worker_port=0,
                   security=security)

    @gen.coroutine
    def close(signalnum):
        worker._close(timeout=2)

    install_signal_handlers(loop, cleanup=close)

    @gen.coroutine
    def run():
        yield worker._start(None)
        while worker.status != 'closed':
            yield gen.sleep(0.2)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
