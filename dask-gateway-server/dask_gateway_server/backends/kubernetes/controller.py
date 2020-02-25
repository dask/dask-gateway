import asyncio
import logging
import signal
import sys
from aiohttp import web

from traitlets import Unicode, Integer, validate, default
from traitlets.config import catch_config_error

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.rest import ApiException

from ... import __version__ as VERSION
from ...utils import (
    AccessLogger,
    LogFormatter,
    normalize_address,
    run_main,
    UniqueQueue,
    CancelGroup,
)
from ...traitlets import Application
from .utils import Reflector, DELETED


class HashingQueue(object):
    def __init__(self, queues):
        self.queues = queues
        self.n = len(queues)

    async def put(self, item):
        await self.queues[hash(item) % self.n].put(item)


class KubeController(Application):
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

    crd_version = Unicode(
        "v1alpha1", help="The version for the DaskCluster CRD", config=True
    )

    gateway_instance = Unicode(
        help="""
        A unique ID for this instance of dask-gateway.

        The gateway server must also be configured with the same ID.
        """,
        config=True,
    )

    label_selector = Unicode(
        help="""
        The label selector to use when watching objects managed by the gateway.
        """,
        config=True,
    )

    @default("label_selector")
    def _default_label_selector(self):
        return f"gateway.dask.org/instance={self.gateway_instance}"

    parallelism = Integer(
        20,
        help="""
        Number of handlers to use for reconciling k8s objects.
        """,
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

        # Initialize the kubernetes clients
        try:
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()
        self.api_client = client.ApiClient()
        self.core_client = client.CoreV1Api(api_client=self.api_client)
        self.custom_client = client.CustomObjectsApi(api_client=self.api_client)

        # Initialize queue and reflectors
        self.queues = [UniqueQueue() for _ in range(self.parallelism)]
        self.queue = HashingQueue(self.queues)
        self.reflectors = {
            "cluster": Reflector(
                parent=self,
                kind="cluster",
                api_client=self.api_client,
                queue=self.queue,
                method=self.custom_client.list_cluster_custom_object,
                method_kwargs=dict(
                    group="gateway.dask.org",
                    version=self.crd_version,
                    plural="daskclusters",
                    label_selector=self.label_selector,
                ),
            ),
            "pod": Reflector(
                parent=self,
                kind="pod",
                api_client=self.api_client,
                queue=self.queue,
                method=self.core_client.list_pod_for_all_namespaces,
                method_kwargs=dict(label_selector=self.label_selector),
            ),
        }
        await asyncio.wait([r.start() for r in self.reflectors.values()])
        self.log.debug("All reflectors started")

        # Initialize reconcilers
        self.cg = CancelGroup()
        self.reconcilers = [
            asyncio.ensure_future(self.reconciler_loop(q)) for q in self.queues
        ]

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
        # Stop reconcilation queues
        if hasattr(self, "cg"):
            await self.cg.cancel()
            await asyncio.gather(*self.reconcilers, return_exceptions=True)

        # Shutdown the client
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()

        # Shutdown the aiohttp application
        if hasattr(self, "runner"):
            await self.runner.cleanup()

        self.log.info("Stopped successfully")

    async def reconciler_loop(self, queue):
        while True:
            async with self.cg.cancellable():
                kind, name = await queue.get()

            if kind == "cluster":
                method = self.reconcile_cluster
            else:
                method = self.reconcile_pod

            self.log.debug("Reconciling %s %s", kind, name)
            try:
                await method(name)
            except Exception:
                self.log.warning(
                    "Error while reconciling %s %s", kind, name, exc_info=True
                )

    async def reconcile_cluster(self, name):
        cluster = self.reflectors["cluster"].get(name)

        if cluster is DELETED:
            self.log.debug("Cluster %s deleted", name)
            return

        status = cluster.get("status") or {}
        phase = status.get("phase", "Pending")

        if phase == "Pending":
            await self.cluster_to_running(cluster)

    async def reconcile_pod(self, name):
        pass

    def _init_template(self, template):
        template = template.copy()
        template["metadata"] = template["metadata"].copy()
        return template

    async def cluster_to_running(self, cluster):
        meta = cluster["metadata"]
        namespace = meta["namespace"]
        cluster_name = meta["labels"]["gateway.dask.org/cluster"]

        pod = self.reflectors["pod"].get(f"dask-gateway-{cluster_name}")
        if pod is None:
            cluster_ref = {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "name": cluster["metadata"]["name"],
                "uid": cluster["metadata"]["uid"],
            }
            # Create the secret if it doesn't exist
            secret = self._init_template(cluster["spec"]["secret"])
            secret["metadata"]["namespace"] = namespace
            secret["metadata"]["ownerReferences"] = [cluster_ref]
            secret["apiVersion"] = "v1"
            secret["kind"] = "Secret"
            try:
                await self.core_client.create_namespaced_secret(namespace, secret)
                self.log.debug("Created secret for %s", cluster_name)
            except ApiException as exc:
                if exc.status != 409:
                    raise

            # Create the scheduler pod if it doesn't exist
            scheduler = self._init_template(cluster["spec"]["schedulerTemplate"])
            scheduler["metadata"]["namespace"] = namespace
            scheduler["metadata"]["name"] = f"dask-gateway-{cluster_name}"
            scheduler["metadata"]["ownerReferences"] = [cluster_ref]
            scheduler["apiVersion"] = "v1"
            scheduler["kind"] = "Pod"
            try:
                pod = await self.core_client.create_namespaced_pod(namespace, scheduler)
                pod = self.api_client.sanitize_for_serialization(pod)
                self.log.debug("Created scheduler pod for %s", cluster_name)
            except ApiException as exc:
                if exc.status != 409:
                    raise
            self.reflectors["pod"].put(scheduler["metadata"]["name"], pod)

        sched_ref = {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": pod["metadata"]["name"],
            "uid": pod["metadata"]["uid"],
        }
        service = self._init_template(cluster["spec"]["service"])
        service["apiVersion"] = "v1"
        service["kind"] = "Service"
        service["metadata"]["namespace"] = namespace
        service["metadata"]["ownerReferences"] = [sched_ref]
        try:
            await self.core_client.create_namespaced_service(namespace, service)
            self.log.debug("Created service for %s", cluster_name)
        except ApiException as exc:
            if exc.status != 409:
                raise

        route = self._init_template(cluster["spec"]["ingressroute"])
        route["apiVersion"] = "traefik.containo.us/v1alpha1"
        route["kind"] = "IngressRoute"
        route["metadata"]["namespace"] = namespace
        route["metadata"]["ownerReferences"] = [sched_ref]
        try:
            await self.custom_client.create_namespaced_custom_object(
                "traefik.containo.us", "v1alpha1", namespace, "ingressroutes", route
            )
            self.log.debug("Created ingress route for %s", cluster_name)
        except ApiException as exc:
            if exc.status != 409:
                raise

        # IngressRouteTCP
        route = self._init_template(cluster["spec"]["ingressroutetcp"])
        route["apiVersion"] = "traefik.containo.us/v1alpha1"
        route["kind"] = "IngressRouteTCP"
        route["metadata"]["namespace"] = namespace
        route["metadata"]["ownerReferences"] = [sched_ref]
        try:
            await self.custom_client.create_namespaced_custom_object(
                "traefik.containo.us", "v1alpha1", namespace, "ingressroutetcps", route
            )
            self.log.debug("Created ingress route tcp for %s", cluster_name)
        except ApiException as exc:
            if exc.status != 409:
                raise


main = KubeController.launch_instance

if __name__ == "__main__":
    main()
