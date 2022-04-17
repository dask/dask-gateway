import asyncio
import collections
import json
import logging
import signal
import sys
import time
import uuid
from base64 import b64encode

from aiohttp import web
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.rest import ApiException
from traitlets import Float, Integer, List, Unicode, validate
from traitlets.config import catch_config_error

from ... import __version__ as VERSION
from ...tls import new_keypair
from ...traitlets import Application
from ...utils import (
    AccessLogger,
    FrozenAttrDict,
    LogFormatter,
    RateLimiter,
    TaskPool,
    normalize_address,
    run_main,
    timestamp,
)
from ...workqueue import Backoff, WorkQueue, WorkQueueClosed
from .backend import KubeBackendAndControllerMixin
from .utils import (
    Informer,
    RateLimitedClient,
    k8s_timestamp,
    merge_json_objects,
    parse_k8s_timestamp,
)


def get_container_status(pod, name):
    """Get the status of container ``name`` from a pod"""
    for cs in pod["status"].get("containerStatuses", ()):
        if cs["name"] == name:
            return cs
    return None


def get_container_state(pod, name):
    """Get the state of container ``name`` from a pod.

    Returns one of ``waiting, running, terminated, unknown``.
    """
    phase = pod["status"].get("phase", "Unknown")
    if phase == "Pending":
        return "waiting"
    cs = get_container_status(pod, name)
    if cs is not None:
        return next(iter(cs["state"]))
    return "unknown"


def get_cluster_key(obj):
    """Get the cluster key for a given k8s object"""
    try:
        namespace = obj["metadata"]["namespace"]
        name = obj["metadata"]["labels"]["gateway.dask.org/cluster"]
        return f"{namespace}.{name}"
    except KeyError:
        return None


class ClusterInfo:
    """Stores in-memory state about a given cluster.

    State can be reconstructed from k8s, this just provides fast access in the
    reconciler loop.
    """

    def __init__(self):
        self.all_pods = set()
        self.pending = set()
        self.running = set()
        self.succeeded = set()
        self.failed = set()
        self.set_expectations()

    def set_expectations(self, creates=0, deletes=0):
        """Indicate that this cluster expects to see ``creates``/``deletes`` pod
        operations before being woken up again.
        """
        self.creates = creates
        self.deletes = deletes
        self.timestamp = time.monotonic()

    def expectations_fulfilled(self):
        """Return true if all expectated pod operations have been seen"""
        return self.creates <= 0 and self.deletes <= 0

    def expectations_expired(self):
        """Return true if expected pod operations have timed out"""
        return time.monotonic() - self.timestamp > 600

    def should_trigger(self):
        """Return true if our expectations indicate the cluster should be
        reconciled again"""
        return self.expectations_fulfilled() or self.expectations_expired()

    def _update(self, kind, pod_name):
        if pod_name not in self.all_pods:
            self.creates -= 1
            self.all_pods.add(pod_name)

        for n in ("pending", "running", "succeeded", "failed"):
            s = getattr(self, n)
            if n == kind:
                change = pod_name not in s
                if change:
                    s.add(pod_name)
            else:
                s.discard(pod_name)
        return change

    def on_worker_pending(self, pod_name):
        return self._update("pending", pod_name)

    def on_worker_running(self, pod_name):
        return self._update("running", pod_name)

    def on_worker_succeeded(self, pod_name):
        return self._update("succeeded", pod_name)

    def on_worker_failed(self, pod_name):
        return self._update("failed", pod_name)

    def on_worker_deleted(self, pod_name):
        if pod_name in self.all_pods:
            self.deletes -= 1
            self.all_pods.discard(pod_name)
            for n in ("pending", "running", "succeeded", "failed"):
                getattr(self, n).discard(pod_name)


class KubeController(KubeBackendAndControllerMixin, Application):
    """Kubernetes controller for dask-gateway"""

    name = "dask-gateway-kube-controller"
    version = VERSION

    description = """Start dask-gateway kubernetes controller"""

    examples = """

    Start the controller with config file ``config.py``

        dask-gateway-kube-controller -f config.py
    """

    aliases = {
        "log-level": "KubeController.log_level",
        "f": "KubeController.config_file",
        "config": "KubeController.config_file",
    }

    config_file = Unicode(
        "dask_gateway_config.py", help="The config file to load", config=True
    )

    address = Unicode(
        ":8000", help="The address the server should listen at", config=True
    )

    @validate("address")
    def _validate_address(self, proposal):
        return normalize_address(proposal.value)

    api_url = Unicode(
        help="""
        The address that internal components (e.g. dask clusters)
        will use when contacting the gateway.
        """,
        config=True,
    )

    parallelism = Integer(
        20,
        help="""
        Number of handlers to use for reconciling k8s objects.
        """,
        config=True,
    )

    completed_cluster_cleanup_period = Float(
        600,
        help="""
        Time (in seconds) between cleanup tasks.

        This sets how frequently old cluster records are deleted from
        kubernetes.  This shouldn't be too small (to keep the overhead low),
        but should be smaller than ``completed_cluster_max_age`` (probably by an
        order of magnitude).
        """,
        config=True,
    )

    completed_cluster_max_age = Float(
        3600 * 24,
        help="""
        Max time (in seconds) to keep around records of completed clusters.

        Every ``completed_cluster_cleanup_period``, completed clusters older than
        ``completed_cluster_max_age`` are deleted from kubernetes.
        """,
        config=True,
    )

    backoff_base_delay = Float(
        0.1,
        help="""
        Base delay (in seconds) for backoff when retrying after failures.

        If an operation fails, it is retried after a backoff computed as:

        ```
        min(backoff_max_delay, backoff_base_delay * 2 ** num_failures)
        ```
        """,
        config=True,
    )

    backoff_max_delay = Float(
        300,
        help="""
        Max delay (in seconds) for backoff policy when retrying after failures.
        """,
        config=True,
    )

    k8s_api_rate_limit = Integer(
        50,
        help="""
        Limit on the average number of k8s api calls per second.
        """,
        config=True,
    )

    k8s_api_rate_limit_burst = Integer(
        100,
        help="""
        Limit on the maximum number of k8s api calls per second.
        """,
        config=True,
    )

    proxy_prefix = Unicode(
        "",
        help="""
        The path prefix the HTTP/HTTPS proxy should serve under.

        This prefix will be prepended to all routes registered with the proxy.
        """,
        config=True,
    )

    @validate("proxy_prefix")
    def _validate_proxy_prefix(self, proposal):
        prefix = proposal.value.strip("/")
        return f"/{prefix}" if prefix else prefix

    proxy_web_entrypoint = Unicode(
        "web",
        help="The traefik entrypoint name to use when creating ingressroutes",
        config=True,
    )

    proxy_tcp_entrypoint = Unicode(
        "tcp",
        help="The traefik entrypoint name to use when creating ingressroutetcps",
        config=True,
    )

    proxy_web_middlewares = List(
        help="A list of middlewares to apply to web routes added to the proxy.",
        config=True,
    )

    _log_formatter_cls = LogFormatter

    # Fail if the config file errors
    raise_config_file_errors = True

    def handle_shutdown_signal(self, sig):
        self.log.warning("Received signal %s, initiating shutdown...", sig.name)
        raise web.GracefulExit

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        if self.subapp is not None:
            return

        # Setup logging
        self.log.propagate = False
        for name in ["aiohttp.access", "aiohttp.server"]:
            l = logging.getLogger(name)
            l.handlers[:] = []
            l.propagate = True
            l.parent = self.log
            l.setLevel(self.log_level)

        self.log.info("Starting %s - version %s", self.name, self.version)

        # Load configuration
        self.load_config_file(self.config_file)

        # Initialize aiohttp application
        self.app = web.Application(logger=self.log)
        self.app["controller"] = self

    def start(self):
        if self.subapp is not None:
            return self.subapp.start()

        try:
            run_main(self.main())
        except (KeyboardInterrupt, web.GracefulExit):
            pass

    async def main(self):
        try:
            try:
                await self.setup()
            except Exception:
                self.log.critical(
                    "Failed to start gateway, shutting down", exc_info=True
                )
                sys.exit(1)

            while True:
                await asyncio.sleep(3600)
        finally:
            try:
                await self.cleanup()
            except Exception:
                self.log.error("Error while shutting down:", exc_info=True)

    async def setup(self):
        # Register signal handlers
        loop = asyncio.get_event_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, self.handle_shutdown_signal, s)

        # Rate limiter for k8s api calls
        self.rate_limiter = RateLimiter(
            rate=self.k8s_api_rate_limit, burst=self.k8s_api_rate_limit_burst
        )

        # Initialize the kubernetes clients
        try:
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()
        self.api_client = client.ApiClient()
        self.core_client = RateLimitedClient(
            client.CoreV1Api(api_client=self.api_client), self.rate_limiter
        )
        self.custom_client = RateLimitedClient(
            client.CustomObjectsApi(api_client=self.api_client), self.rate_limiter
        )

        # Local state
        self.cluster_info = collections.defaultdict(ClusterInfo)
        self.stopped_clusters = {}

        # Initialize queue and informers
        self.queue = WorkQueue(
            backoff=Backoff(
                base_delay=self.backoff_base_delay, max_delay=self.backoff_max_delay
            )
        )
        endpoints_selector = (
            self.label_selector + ",app.kubernetes.io/component=dask-scheduler"
        )
        self.informers = {
            "cluster": Informer(
                parent=self,
                name="cluster",
                client=self.custom_client,
                method="list_cluster_custom_object",
                method_kwargs=dict(
                    group="gateway.dask.org",
                    version=self.crd_version,
                    plural="daskclusters",
                    label_selector=self.label_selector,
                ),
                on_update=self.on_cluster_update,
                on_delete=self.on_cluster_delete,
            ),
            "pod": Informer(
                parent=self,
                name="pod",
                client=self.core_client,
                method="list_pod_for_all_namespaces",
                method_kwargs=dict(label_selector=self.label_selector),
                on_update=self.on_pod_update,
                on_delete=self.on_pod_delete,
            ),
            "endpoints": Informer(
                parent=self,
                name="endpoints",
                client=self.core_client,
                method="list_endpoints_for_all_namespaces",
                method_kwargs=dict(label_selector=endpoints_selector),
                on_update=self.on_endpoints_update,
                on_delete=self.on_endpoints_delete,
            ),
        }
        await asyncio.wait([i.start() for i in self.informers.values()])
        self.log.debug("All informers started")

        # Initialize reconcilers
        self.reconcilers = [
            asyncio.ensure_future(self.reconciler_loop())
            for _ in range(self.parallelism)
        ]

        # Start background tasks
        self.task_pool = TaskPool()
        self.task_pool.spawn(self.cleanup_expired_cluster_records_loop())

        # Start the aiohttp application
        self.runner = web.AppRunner(
            self.app,
            handle_signals=False,
            access_log_class=AccessLogger,
            access_log=self.log,
        )
        await self.runner.setup()

        host, port = self.address.split(":")
        port = int(port)
        site = web.TCPSite(self.runner, host, port, shutdown_timeout=15.0, backlog=128)
        await site.start()
        self.log.info("%s started!", self.name)
        self.log.info("API listening at http://%s", self.address)

    async def cleanup(self):
        # Stop reconcilation workers
        if hasattr(self, "reconcilers"):
            self.queue.close()
            await asyncio.gather(*self.reconcilers, return_exceptions=True)

        if hasattr(self, "informers"):
            await asyncio.wait([i.stop() for i in self.informers.values()])

        # Stop background tasks
        if hasattr(self, "task_pool"):
            await self.task_pool.close()

        # Shutdown the client
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()

        # Shutdown the aiohttp application
        if hasattr(self, "runner"):
            await self.runner.cleanup()

        self.log.info("Stopped successfully")

    async def delete_cluster(self, namespace, name):
        await self.custom_client.delete_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, namespace, "daskclusters", name
        )

    async def cleanup_expired_cluster_records_loop(self):
        while True:
            try:
                cutoff = timestamp() - self.completed_cluster_max_age * 1000
                to_delete = [
                    k.split(".")
                    for k, stop_time in self.stopped_clusters.items()
                    if stop_time <= cutoff
                ]
                if to_delete:
                    self.log.info("Removing %d expired cluster records", len(to_delete))
                    tasks = (self.delete_cluster(ns, name) for ns, name in to_delete)
                    await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error(
                    "Error while deleting expired cluster records", exc_info=exc
                )
            await asyncio.sleep(self.completed_cluster_cleanup_period)

    def on_pod_update(self, pod, old=None):
        cluster_key = get_cluster_key(pod)
        if cluster_key is None:
            return
        cluster_stopping = cluster_key in self.stopped_clusters

        component = pod["metadata"]["labels"]["app.kubernetes.io/component"]
        if component == "dask-scheduler":
            if (
                get_container_state(pod, "dask-scheduler") in ("running", "terminated")
                and not cluster_stopping
            ):
                self.queue.put(cluster_key)
        elif component == "dask-worker":
            pod_name = pod["metadata"]["name"]
            phase = pod["status"].get("phase", "Unknown")
            if phase == "Unknown":
                return
            cs = get_container_status(pod, component)
            if cs is None:
                return

            info = self.cluster_info[cluster_key]
            trigger = False
            if phase == "Pending":
                info.on_worker_pending(pod_name)
            elif phase == "Succeeded":
                info.on_worker_succeeded(pod_name)
            elif phase == "Failed":
                trigger = info.on_worker_failed(pod_name)
            else:
                kind = next(iter(cs["state"]))
                if kind == "terminated" and cs["state"][kind]["exitCode"] == 0:
                    info.on_worker_succeeded(pod_name)
                elif kind == "running":
                    info.on_worker_running(pod_name)

            if (trigger or info.should_trigger()) and not cluster_stopping:
                self.queue.put(cluster_key)

    def on_pod_delete(self, pod):
        cluster_key = get_cluster_key(pod)
        if cluster_key is None:
            return
        cluster_stopping = cluster_key in self.stopped_clusters

        component = pod["metadata"]["labels"]["app.kubernetes.io/component"]
        if component == "dask-scheduler" and not cluster_stopping:
            self.queue.put(cluster_key)
        elif component == "dask-worker":
            info = self.cluster_info[cluster_key]
            info.on_worker_deleted(pod["metadata"]["name"])
            if info.should_trigger() and not cluster_stopping:
                self.queue.put(cluster_key)

    def endpoints_all_ready(self, endpoints):
        subsets = endpoints.get("subsets", ())
        return subsets and not any(s.get("notReadyAddresses") for s in subsets)

    def on_endpoints_update(self, endpoints, old=None):
        cluster_key = get_cluster_key(endpoints)
        if cluster_key is None or cluster_key in self.stopped_clusters:
            return
        if self.endpoints_all_ready(endpoints):
            self.queue.put(cluster_key)

    def on_endpoints_delete(self, endpoints):
        pass

    def on_cluster_update(self, cluster, old=None):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]
        self.queue.put(f"{namespace}.{name}")

    def on_cluster_delete(self, cluster):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]
        self.log.debug("Cluster %s.%s deleted", namespace, name)
        self.stopped_clusters.pop(f"{namespace}.{name}", None)

    async def reconciler_loop(self):
        while True:
            try:
                name = await self.queue.get()
            except WorkQueueClosed:
                return

            self.log.info("Reconciling cluster %s", name)
            try:
                requeue = await self.reconcile_cluster(name)
            except Exception:
                self.log.warning(
                    "Error while reconciling cluster %s", name, exc_info=True
                )
                self.queue.put_backoff(name)
            else:
                self.log.info("Finished reconciling cluster %s", name)
                if requeue:
                    self.queue.put_backoff(name)
                else:
                    self.queue.reset_backoff(name)
            finally:
                self.queue.task_done(name)

    async def reconcile_cluster(self, cluster_key):
        cluster = self.informers["cluster"].get(cluster_key)
        if cluster is None:
            # Cluster already deleted, nothing to do
            return

        status_update, requeue = await self.handle_cluster(cluster)

        if status_update is not None and status_update != cluster.get("status"):
            namespace = cluster["metadata"]["namespace"]
            name = cluster["metadata"]["name"]
            await self.patch_cluster_status(namespace, name, status_update)
        return requeue

    async def handle_cluster(self, cluster):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]

        status = cluster.get("status", {})
        spec = cluster.get("spec")

        phase = status.get("phase", "Pending")

        if phase in {"Stopped", "Failed"}:
            if "completionTime" in status:
                stop_time = parse_k8s_timestamp(status["completionTime"])
            else:
                stop_time = timestamp()
            self.stopped_clusters[f"{namespace}.{name}"] = stop_time
            return None, False

        active = spec.get("active", True)

        if not active:
            self.log.info("Shutting down %s.%s", namespace, name)
            await self.cleanup_cluster_resources(status, namespace)
            status = status.copy()
            status["phase"] = "Stopped"
            status["completionTime"] = k8s_timestamp()
            return status, False

        if phase == "Pending":
            return await self.handle_pending_cluster(cluster)

        if phase == "Running":
            return await self.handle_running_cluster(cluster)

    async def handle_pending_cluster(self, cluster):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]

        status = cluster.get("status", {}).copy()
        status.setdefault("phase", "Pending")

        if not status.get("credentials"):
            secret_name = await self.create_secret_if_not_exists(cluster)
            status["credentials"] = secret_name

        sched_pod_name = status.get("schedulerPod")
        if not sched_pod_name:
            sched_pod_name, sched_pod = await self.create_scheduler_pod_if_not_exists(
                cluster
            )
            status["schedulerPod"] = sched_pod_name
        else:
            sched_pod = self.informers["pod"].get(f"{namespace}.{sched_pod_name}")

        if sched_pod is None:
            # Scheduler pod was created in an earlier run, but hasn't been seen
            # by the informers. Wait until it shows up to progress further.
            return status, False

        sched_state = get_container_state(sched_pod, "dask-scheduler")

        if sched_state == "running":
            service_name = status.get("service")
            if not status.get("service"):
                service_name = await self.create_service_if_not_exists(
                    cluster, sched_pod
                )
                status["service"] = service_name

            endpoints = self.informers["endpoints"].get(f"{namespace}.{service_name}")
            if endpoints is not None and self.endpoints_all_ready(endpoints):
                # Only add routes if endpoints exist, otherwise traefik gets grumpy
                # This also allows us to delay setting the phase to Running
                # until the route is likely to be added.
                if not status.get("ingressroute"):
                    route = await self.create_ingressroute_if_not_exists(
                        cluster, sched_pod
                    )
                    status["ingressroute"] = route

                if not status.get("ingressroutetcp"):
                    route = await self.create_ingressroutetcp_if_not_exists(
                        cluster, sched_pod
                    )
                    status["ingressroutetcp"] = route

                status["phase"] = "Running"
        elif sched_state == "terminated":
            self.log.info(
                "Scheduler for cluster %s.%s terminated, shutting down", namespace, name
            )
            await self.cleanup_cluster_resources(status, namespace)
            status["phase"] = "Failed"
            status["completionTime"] = k8s_timestamp()

        return status, False

    async def handle_running_cluster(self, cluster):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]
        cluster_key = f"{namespace}.{name}"
        requeue = False

        spec = cluster["spec"]
        status = cluster.get("status", {}).copy()

        sched_pod_name = status.get("schedulerPod")
        sched_pod = None
        sched_state = "terminated"
        if sched_pod_name:
            sched_pod = self.informers["pod"].get(f"{namespace}.{sched_pod_name}")
            if sched_pod is not None:
                sched_state = get_container_state(sched_pod, "dask-scheduler")

        if sched_state == "terminated":
            self.log.info(
                "Scheduler for cluster %s.%s terminated, shutting down", namespace, name
            )
            await self.cleanup_cluster_resources(status, namespace)
            status["phase"] = "Failed"
            status["completionTime"] = k8s_timestamp()

        elif sched_state == "running":
            info = self.cluster_info[cluster_key]
            if info.should_trigger():
                n_workers = len(info.running.union(info.pending))
                replicas = spec.get("replicas", 0)
                delta = replicas - n_workers
                if delta > 0:
                    requeue = await self.handle_scale_up(
                        cluster, sched_pod, info, replicas, delta
                    )
                else:
                    requeue = await self.handle_scale_down(
                        cluster, sched_pod, info, replicas, delta
                    )
        return status, requeue

    async def create_pod(self, namespace, pod, info=None):
        try:
            res = await self.core_client.create_namespaced_pod(namespace, pod)
            self.log.debug("Created pod %s/%s", namespace, res.metadata.name)
            return res
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if info is not None:
                info.creates -= 1
            if isinstance(exc, ApiException):
                # Try to log a nicer message if an api exception
                try:
                    message = json.loads(exc.body)["message"]
                    self.log.warning(
                        "Failed to create pod in namespace %s - %s", namespace, message
                    )
                except Exception:
                    pass
                else:
                    raise
            self.log.warning(
                "Failed to create pod in namespace %s", namespace, exc_info=True
            )
            raise

    async def delete_pod(self, namespace, pod_name, info=None):
        try:
            await self.core_client.delete_namespaced_pod(pod_name, namespace)
            self.log.debug("Deleted pod %s/%s", namespace, pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if info is not None:
                info.deletes -= 1
            if isinstance(exc, ApiException) and exc.code == 404:
                return
            self.log.warning(
                "Failed to delete pod %s/%s", namespace, pod_name, exc_info=True
            )
            raise

    async def batch_create_pods(self, info, namespace, pod, count):
        """Create pods in exponentially increasing batches.

        This helps prevent excessive failed api requests (which are sometimes
        expected, due to e.g. resource-quotas). It also reduces load on the k8s
        api server.
        """
        remaining = count
        batch_size = 1
        while remaining > 0:
            batch = [
                self.create_pod(namespace, pod, info)
                for _ in range(min(batch_size, remaining))
            ]
            res = await asyncio.gather(*batch, return_exceptions=True)
            remaining -= len(batch)
            if any(isinstance(r, Exception) for r in res):
                # Remaining creates will never be submitted
                info.creates -= remaining
                return True
            batch_size *= 2
        return False

    async def handle_scale_up(self, cluster, sched_pod, info, replicas, delta):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        config = FrozenAttrDict(cluster["spec"]["config"])

        pod = self.make_pod(namespace, name, config, is_worker=True)
        pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]
        to_delete = info.succeeded.union(info.failed)
        info.set_expectations(creates=delta, deletes=len(to_delete))
        self.log.info(
            "Cluster %s.%s scaled to %d - creating %d workers, deleting %d stopped workers",
            namespace,
            name,
            replicas,
            delta,
            len(to_delete),
        )
        failed = await self.batch_create_pods(info, namespace, pod, delta)
        res = await asyncio.gather(
            *(self.delete_pod(namespace, p, info) for p in to_delete),
            return_exceptions=True,
        )
        return failed or any(isinstance(r, Exception) for r in res)

    async def handle_scale_down(self, cluster, sched_pod, info, replicas, delta):
        namespace = cluster["metadata"]["namespace"]
        name = cluster["metadata"]["name"]

        if info.pending:
            pending = list(info.pending)[:delta]
        else:
            pending = []

        stopped = info.succeeded.union(info.failed)
        if pending or stopped:
            self.log.info(
                "Cluster %s.%s scaled to %d - deleting %d pending and %d stopped workers",
                namespace,
                name,
                replicas,
                len(pending),
                len(stopped),
            )
            pods = pending
            pods.extend(stopped)
            info.set_expectations(deletes=len(pods))
            res = await asyncio.gather(
                *(self.delete_pod(namespace, p, info) for p in pods),
                return_exceptions=True,
            )
            return any(isinstance(r, Exception) for r in res)
        return False

    async def patch_cluster_status(self, namespace, name, status):
        await self.custom_client.patch_namespaced_custom_object_status(
            "gateway.dask.org",
            self.crd_version,
            namespace,
            "daskclusters",
            name,
            [{"op": "add", "path": "/status", "value": status}],
        )

    async def cleanup_cluster_resources(self, status, namespace):
        sched_pod = status.get("schedulerPod")
        if sched_pod:
            try:
                await self.core_client.delete_namespaced_pod(sched_pod, namespace)
            except ApiException as exc:
                if exc.status != 404:
                    raise
        secret = status.get("credentials")
        if secret:
            try:
                await self.core_client.delete_namespaced_secret(secret, namespace)
            except ApiException as exc:
                if exc.status != 404:
                    raise

    async def create_secret_if_not_exists(self, cluster):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        secret = self.make_secret(name)
        secret["metadata"]["ownerReferences"] = [
            {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "name": cluster["metadata"]["name"],
                "uid": cluster["metadata"]["uid"],
            }
        ]

        self.log.info("Creating new credentials for cluster %s.%s", namespace, name)
        try:
            await self.core_client.create_namespaced_secret(namespace, secret)
        except ApiException as exc:
            if exc.status != 409:
                raise

        return secret["metadata"]["name"]

    async def create_scheduler_pod_if_not_exists(self, cluster):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        config = FrozenAttrDict(cluster["spec"]["config"])
        pod = self.make_pod(namespace, name, config)
        pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "name": cluster["metadata"]["name"],
                "uid": cluster["metadata"]["uid"],
            }
        ]
        pod_name = pod["metadata"]["name"]

        self.log.info("Creating scheduler pod for cluster %s.%s", namespace, name)
        try:
            pod = await self.core_client.create_namespaced_pod(namespace, pod)
            pod = self.api_client.sanitize_for_serialization(pod)
        except ApiException as exc:
            if exc.status == 409:
                pod = None
            else:
                raise

        return pod_name, pod

    async def create_service_if_not_exists(self, cluster, sched_pod):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        service = self.make_service(name)
        service["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.info("Creating scheduler service for cluster %s.%s", namespace, name)
        try:
            await self.core_client.create_namespaced_service(namespace, service)
        except ApiException as exc:
            if exc.status != 409:
                raise

        return service["metadata"]["name"]

    async def create_ingressroute_if_not_exists(self, cluster, sched_pod):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        route = self.make_ingressroute(name, namespace)
        route["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.info(
            "Creating scheduler HTTP route for cluster %s.%s", namespace, name
        )
        try:
            await self.custom_client.create_namespaced_custom_object(
                "traefik.containo.us", "v1alpha1", namespace, "ingressroutes", route
            )
        except ApiException as exc:
            if exc.status != 409:
                raise

        return route["metadata"]["name"]

    async def create_ingressroutetcp_if_not_exists(self, cluster, sched_pod):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        route = self.make_ingressroutetcp(name, namespace)
        route["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.info("Creating scheduler TCP route for cluster %s.%s", namespace, name)
        try:
            await self.custom_client.create_namespaced_custom_object(
                "traefik.containo.us", "v1alpha1", namespace, "ingressroutetcps", route
            )
        except ApiException as exc:
            if exc.status != 409:
                raise

        return route["metadata"]["name"]

    def get_scheduler_command(self, namespace, cluster_name, config):
        return config.scheduler_cmd + [
            "--protocol",
            "tls",
            "--host",
            "",
            "--port",
            "8786",
            "--dashboard-address",
            ":8787",
            "--dg-api-address",
            ":8788",
            "--preload",
            "dask_gateway.scheduler_preload",
            "--dg-heartbeat-period",
            "0",
            "--dg-adaptive-period",
            str(config.adaptive_period),
            "--dg-idle-timeout",
            str(config.idle_timeout),
        ]

    def get_worker_command(self, namespace, cluster_name, config):
        service_name = self.make_service_name(cluster_name)
        return config.worker_cmd + [
            f"tls://{service_name}.{namespace}:8786",
            "--dashboard-address",
            ":8787",
            "--name",
            "$(DASK_GATEWAY_WORKER_NAME)",
            "--nthreads",
            str(config.worker_threads),
            "--memory-limit",
            str(config.worker_memory_limit),
        ]

    def get_env(self, namespace, cluster_name, config):
        env = dict(config.environment)
        env.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_CLUSTER_NAME": f"{namespace}.{cluster_name}",
                "DASK_GATEWAY_API_TOKEN": "/etc/dask-credentials/api-token",
                "DASK_DISTRIBUTED__COMM__TLS__CA_FILE": "/etc/dask-credentials/dask.crt",
                "DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__CERT": "/etc/dask-credentials/dask.crt",
                "DASK_DISTRIBUTED__COMM__TLS__SCHEDULER__KEY": "/etc/dask-credentials/dask.pem",
                "DASK_DISTRIBUTED__COMM__TLS__WORKER__CERT": "/etc/dask-credentials/dask.crt",
                "DASK_DISTRIBUTED__COMM__TLS__WORKER__KEY": "/etc/dask-credentials/dask.pem",
            }
        )
        return [{"name": k, "value": v} for k, v in env.items()]

    def get_labels(self, cluster_name, component=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "gateway.dask.org/instance": self.gateway_instance,
                "gateway.dask.org/cluster": cluster_name,
            }
        )
        if component:
            labels["app.kubernetes.io/component"] = component
        return labels

    def make_pod(self, namespace, cluster_name, config, is_worker=False):
        env = self.get_env(namespace, cluster_name, config)

        if is_worker:
            container_name = "dask-worker"
            mem_req = config.worker_memory
            mem_lim = config.worker_memory_limit
            cpu_req = config.worker_cores
            cpu_lim = config.worker_cores_limit
            cmd = self.get_worker_command(namespace, cluster_name, config)
            env.append(
                {
                    "name": "DASK_GATEWAY_WORKER_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
                }
            )
            extra_pod_config = config.worker_extra_pod_config
            extra_container_config = config.worker_extra_container_config
            extra_pod_annotations = config.worker_extra_pod_annotations
            extra_pod_labels = config.worker_extra_pod_labels
            probes = {}
        else:
            container_name = "dask-scheduler"
            mem_req = config.scheduler_memory
            mem_lim = config.scheduler_memory_limit
            cpu_req = config.scheduler_cores
            cpu_lim = config.scheduler_cores_limit
            cmd = self.get_scheduler_command(namespace, cluster_name, config)
            extra_pod_config = config.scheduler_extra_pod_config
            extra_container_config = config.scheduler_extra_container_config
            extra_pod_annotations = config.scheduler_extra_pod_annotations
            extra_pod_labels = config.scheduler_extra_pod_labels
            # TODO: make this configurable. If supported, we should use a
            # startupProbe here instead.
            probes = {
                "readinessProbe": {
                    "httpGet": {"port": 8788, "path": "/api/health"},
                    "periodSeconds": 5,
                    "failureThreshold": 3,
                }
            }

        volume = {
            "name": "dask-credentials",
            "secret": {"secretName": self.make_secret_name(cluster_name)},
        }

        container = {
            "name": container_name,
            "image": config.image,
            "args": cmd,
            "env": env,
            "imagePullPolicy": config.image_pull_policy,
            "resources": {
                "requests": {"cpu": f"{cpu_req:.1f}", "memory": str(mem_req)},
                "limits": {"cpu": f"{cpu_lim:.1f}", "memory": str(mem_lim)},
            },
            "volumeMounts": [
                {
                    "name": "dask-credentials",
                    "mountPath": "/etc/dask-credentials/",
                    "readOnly": True,
                }
            ],
            "ports": [
                {"name": "scheduler", "containerPort": 8786},
                {"name": "dashboard", "containerPort": 8787},
                {"name": "api", "containerPort": 8788},
            ],
        }

        container.update(probes)

        if extra_container_config:
            container = merge_json_objects(container, extra_container_config)

        annotations = self.common_annotations.copy()
        annotations.update(extra_pod_annotations)

        labels = self.get_labels(cluster_name, container_name)
        labels.update(extra_pod_labels)

        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"labels": labels, "annotations": annotations},
            "spec": {
                "containers": [container],
                "volumes": [volume],
                "restartPolicy": "OnFailure" if is_worker else "Never",
                "automountServiceAccountToken": False,
            },
        }

        if extra_pod_config:
            pod["spec"] = merge_json_objects(pod["spec"], extra_pod_config)

        if is_worker:
            pod["metadata"]["generateName"] = f"dask-worker-{cluster_name}-"
        else:
            pod["metadata"]["name"] = f"dask-scheduler-{cluster_name}"

        return pod

    def make_secret_name(self, cluster_name):
        return f"dask-credentials-{cluster_name}"

    def make_secret(self, cluster_name):
        api_token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)

        labels = self.get_labels(cluster_name, "credentials")

        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "labels": labels,
                "annotations": self.common_annotations,
                "name": self.make_secret_name(cluster_name),
            },
            "data": {
                "dask.crt": b64encode(tls_cert).decode(),
                "dask.pem": b64encode(tls_key).decode(),
                "api-token": b64encode(api_token.encode()).decode(),
            },
        }

    def make_service_name(self, cluster_name):
        return f"dask-{cluster_name}"

    def make_service(self, cluster_name):
        return {
            "metadata": {
                "labels": self.get_labels(cluster_name, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": self.make_service_name(cluster_name),
            },
            "spec": {
                "clusterIP": "None",
                "selector": {
                    "gateway.dask.org/cluster": cluster_name,
                    "gateway.dask.org/instance": self.gateway_instance,
                    "app.kubernetes.io/component": "dask-scheduler",
                },
                "ports": [
                    {"name": "scheduler", "port": 8786, "target_port": "scheduler"},
                    {"name": "dashboard", "port": 8787, "target_port": "dashboard"},
                    {"name": "api", "port": 8788, "target_port": "api"},
                ],
            },
        }

    def make_ingressroute(self, cluster_name, namespace):
        route = f"{self.proxy_prefix}/clusters/{namespace}.{cluster_name}/"
        return {
            "apiVersion": "traefik.containo.us/v1alpha1",
            "kind": "IngressRoute",
            "metadata": {
                "labels": self.get_labels(cluster_name, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": f"dask-{cluster_name}",
            },
            "spec": {
                "entryPoints": [self.proxy_web_entrypoint],
                "routes": [
                    {
                        "kind": "Rule",
                        "match": f"PathPrefix(`{route}`)",
                        "services": [
                            {
                                "name": self.make_service_name(cluster_name),
                                "namespace": namespace,
                                "port": 8787,
                            }
                        ],
                        "middlewares": self.proxy_web_middlewares,
                    }
                ],
            },
        }

    def make_ingressroutetcp(self, cluster_name, namespace):
        return {
            "apiVersion": "traefik.containo.us/v1alpha1",
            "kind": "IngressRouteTCP",
            "metadata": {
                "labels": self.get_labels(cluster_name, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": f"dask-{cluster_name}",
            },
            "spec": {
                "entryPoints": [self.proxy_tcp_entrypoint],
                "routes": [
                    {
                        "match": f"HostSNI(`daskgateway-{namespace}.{cluster_name}`)",
                        "services": [
                            {
                                "name": self.make_service_name(cluster_name),
                                "namespace": namespace,
                                "port": 8786,
                            }
                        ],
                    }
                ],
                "tls": {"passthrough": True},
            },
        }


main = KubeController.launch_instance

if __name__ == "__main__":
    main()
