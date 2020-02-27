import asyncio
import logging
import signal
import sys
import uuid
from base64 import b64encode

from aiohttp import web
from traitlets import Unicode, Integer, Dict, validate, default
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
from ...tls import new_keypair
from ...utils import FrozenAttrDict
from .utils import Reflector, DELETED, merge_json_objects


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

    api_url = Unicode(
        help="""
        The address that internal components (e.g. dask clusters)
        will use when contacting the gateway.
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

    common_labels = Dict(
        {
            "app.kubernetes.io/name": "dask-gateway",
            "app.kubernetes.io/version": VERSION.replace("+", "_"),
        },
        help="Kubernetes labels to apply to all objects created by the gateway",
        config=True,
    )

    common_annotations = Dict(
        help="Kubernetes annotations to apply to all objects created by the gateway",
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

    proxy_web_entrypoint = Unicode("web")

    proxy_tcp_entrypoint = Unicode("tcp")

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

    async def read_namespaced_pod(self, pod_name, namespace):
        reflector = self.reflectors["pod"]
        pod = reflector.get(pod_name)
        if pod is not None:
            return pod

        pod = await self.core_client.read_namespaced_pod(pod_name, namespace)
        pod = self.api_client.sanitize_for_serialization(pod)
        if pod_name not in reflector.cache:
            reflector.put(pod_name, pod)
        return pod

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

    def get_pod_state(self, pod):
        component = pod["metadata"]["labels"]["app.kubernetes.io/component"]
        for cs in pod["status"].get("containerStatuses", ()):
            if cs["name"] == component:
                return next(iter(cs["state"]))
        return "unknown"

    async def reconcile_pod(self, name):
        pod = self.reflectors["pod"].get(name)

        if pod is DELETED:
            self.log.debug("Pod %s is deleted", name)
            # TODO: fix reconciler to not drop data on delete, keep object
            # Enqueue related cluster
            await self.queue.put(("cluster", name))
            return

        cluster_name = pod["metadata"]["labels"]["gateway.dask.org/cluster"]
        component = pod["metadata"]["labels"]["app.kubernetes.io/component"]
        state = self.get_pod_state(pod)
        if component == "dask-scheduler":
            if state in ("running", "terminated"):
                await self.queue.put(("cluster", cluster_name))

    async def reconcile_cluster(self, name):
        cluster = self.reflectors["cluster"].get(name)

        if cluster is DELETED:
            self.log.debug("Cluster %s deleted", name)
            return

        namespace = cluster["metadata"]["namespace"]

        status = cluster.get("status") or {}
        status_update = status.copy()

        if not status.get("credentials"):
            secret_name = await self.create_secret_if_not_exists(cluster)
            status_update["credentials"] = secret_name

        sched_pod_name = status.get("schedulerPod")
        if not sched_pod_name:
            sched_pod_name = await self.create_scheduler_pod_if_not_exists(cluster)
            status_update["schedulerPod"] = sched_pod_name

        sched_pod = await self.read_namespaced_pod(sched_pod_name, namespace)

        if not status.get("service"):
            service_name = await self.create_service_if_not_exists(cluster, sched_pod)
            status_update["service"] = service_name

        sched_state = self.get_pod_state(sched_pod)
        if sched_state == "running":
            if not status.get("ingressroute"):
                route = await self.create_ingressroute_if_not_exists(cluster, sched_pod)
                status_update["ingressroute"] = route

            if not status.get("ingressroutetcp"):
                route = await self.create_ingressroutetcp_if_not_exists(
                    cluster, sched_pod
                )
                status_update["ingressroutetcp"] = route

        if status_update:
            update = {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "metadata": cluster["metadata"],
                "status": status_update,
            }
            await self.custom_client.replace_namespaced_custom_object_status(
                "gateway.dask.org",
                self.crd_version,
                cluster["metadata"]["namespace"],
                "daskclusters",
                cluster["metadata"]["name"],
                update,
            )

    async def create_secret_if_not_exists(self, cluster):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        username = cluster["metadata"]["labels"].get("gateway.dask.org/user")
        secret = self.make_secret(name, username)
        secret["metadata"]["ownerReferences"] = [
            {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "name": cluster["metadata"]["name"],
                "uid": cluster["metadata"]["uid"],
            }
        ]

        self.log.debug("Creating new credentials for %s/%s", namespace, name)
        try:
            await self.core_client.create_namespaced_secret(namespace, secret)
        except ApiException as exc:
            if exc.status != 409:
                raise

        return secret["metadata"]["name"]

    async def create_scheduler_pod_if_not_exists(self, cluster):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        username = cluster["metadata"]["labels"].get("gateway.dask.org/user")
        config = FrozenAttrDict(cluster["spec"]["config"])
        pod = self.make_pod(name, username, config)
        pod["metadata"]["ownerReferences"] = [
            {
                "apiVersion": cluster["apiVersion"],
                "kind": cluster["kind"],
                "name": cluster["metadata"]["name"],
                "uid": cluster["metadata"]["uid"],
            }
        ]

        self.log.debug("Creating scheduler pod for %s/%s", namespace, name)
        try:
            pod = await self.core_client.create_namespaced_pod(namespace, pod)
            pod = self.api_client.sanitize_for_serialization(pod)
            self.reflectors["pod"].put(pod["metadata"]["name"], pod)
        except ApiException as exc:
            if exc.status != 409:
                raise

        return pod["metadata"]["name"]

    async def create_service_if_not_exists(self, cluster, sched_pod):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        username = cluster["metadata"]["labels"].get("gateway.dask.org/user")
        service = self.make_service(name, username)
        service["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.debug("Creating scheduler service for %s/%s", namespace, name)
        try:
            await self.core_client.create_namespaced_service(namespace, service)
        except ApiException as exc:
            if exc.status != 409:
                raise

        return service["metadata"]["name"]

    async def create_ingressroute_if_not_exists(self, cluster, sched_pod):
        name = cluster["metadata"]["name"]
        namespace = cluster["metadata"]["namespace"]
        username = cluster["metadata"]["labels"].get("gateway.dask.org/user")
        route = self.make_ingressroute(name, username, namespace)
        route["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.debug("Creating scheduler HTTP route for %s/%s", namespace, name)
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
        username = cluster["metadata"]["labels"].get("gateway.dask.org/user")
        route = self.make_ingressroutetcp(name, username, namespace)
        route["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": sched_pod["metadata"]["name"],
                "uid": sched_pod["metadata"]["uid"],
            }
        ]

        self.log.debug("Creating scheduler TCP route for %s/%s", namespace, name)
        try:
            await self.custom_client.create_namespaced_custom_object(
                "traefik.containo.us", "v1alpha1", namespace, "ingressroutetcps", route
            )
        except ApiException as exc:
            if exc.status != 409:
                raise

        return route["metadata"]["name"]

    def get_scheduler_command(self, config):
        return [
            config.scheduler_cmd,
            "--scheduler-address",
            ":8786",
            "--dashboard-address",
            ":8787",
            "--api-address",
            ":8788",
            "--heartbeat-period",
            "0",
            "--adaptive-period",
            str(config.adaptive_period),
            "--idle-timeout",
            str(config.idle_timeout),
        ]

    def get_worker_command(self, config):
        return [
            config.worker_cmd,
            "--nthreads",
            str(int(config.worker_cores_limit)),
            "--memory-limit",
            str(config.worker_memory_limit),
        ]

    def get_env(self, cluster_name, config):
        out = dict(config.environment)
        out.update(
            {
                "DASK_GATEWAY_API_URL": self.api_url,
                "DASK_GATEWAY_CLUSTER_NAME": cluster_name,
                "DASK_GATEWAY_API_TOKEN": "/etc/dask-credentials/api-token",
                "DASK_GATEWAY_TLS_CERT": "/etc/dask-credentials/dask.crt",
                "DASK_GATEWAY_TLS_KEY": "/etc/dask-credentials/dask.pem",
            }
        )
        return out

    def get_labels(self, cluster_name, username, component=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "gateway.dask.org/instance": self.gateway_instance,
                "gateway.dask.org/cluster": cluster_name,
                "gateway.dask.org/user": username,
            }
        )
        if component:
            labels["app.kubernetes.io/component"] = component
        return labels

    def make_pod(self, cluster_name, username, config, is_worker=False):
        env = self.get_env(cluster_name, config)

        if is_worker:
            container_name = "dask-worker"
            mem_req = config.worker_memory
            mem_lim = config.worker_memory_limit
            cpu_req = config.worker_cores
            cpu_lim = config.worker_cores_limit
            cmd = self.get_worker_command(config)
            extra_pod_config = config.worker_extra_pod_config
            extra_container_config = config.worker_extra_container_config
        else:
            container_name = "dask-scheduler"
            mem_req = config.scheduler_memory
            mem_lim = config.scheduler_memory_limit
            cpu_req = config.scheduler_cores
            cpu_lim = config.scheduler_cores_limit
            cmd = self.get_scheduler_command(config)
            extra_pod_config = config.scheduler_extra_pod_config
            extra_container_config = config.scheduler_extra_container_config

        labels = self.get_labels(cluster_name, username, container_name)

        volume = {
            "name": "dask-credentials",
            "secret": {"secretName": f"dask-gateway-{cluster_name}"},
        }

        container = {
            "name": container_name,
            "image": config.image,
            "args": cmd,
            "env": [{"name": k, "value": v} for k, v in env.items()],
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

        if extra_container_config:
            container = merge_json_objects(container, extra_container_config)

        name = f"dask-gateway-{cluster_name}"

        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "labels": labels,
                "annotations": self.common_annotations,
                "name": name,
            },
            "spec": {
                "containers": [container],
                "volumes": [volume],
                "restartPolicy": "OnFailure" if is_worker else "Never",
                "automountServiceAccountToken": False,
            },
        }

        if extra_pod_config:
            pod["spec"] = merge_json_objects(pod["spec"], extra_pod_config)

        return pod

    def make_secret(self, cluster_name, username):
        api_token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)

        labels = self.get_labels(cluster_name, username, "credentials")

        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "labels": labels,
                "annotations": self.common_annotations,
                "name": f"dask-gateway-{cluster_name}",
            },
            "data": {
                "dask.crt": b64encode(tls_cert).decode(),
                "dask.pem": b64encode(tls_key).decode(),
                "api-token": b64encode(api_token.encode()).decode(),
            },
        }

    def make_service(self, cluster_name, username):
        return {
            "metadata": {
                "labels": self.get_labels(cluster_name, username, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": f"dask-gateway-{cluster_name}",
            },
            "spec": {
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

    def make_ingressroute(self, cluster_name, username, namespace):
        route = f"{self.proxy_prefix}/clusters/{cluster_name}/"
        return {
            "apiVersion": "traefik.containo.us/v1alpha1",
            "kind": "IngressRoute",
            "metadata": {
                "labels": self.get_labels(cluster_name, username, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": f"dask-gateway-{cluster_name}",
            },
            "spec": {
                "entryPoints": [self.proxy_web_entrypoint],
                "routes": [
                    {
                        "kind": "Rule",
                        "match": f"PathPrefix(`{route}`)",
                        "services": [
                            {
                                "name": f"dask-gateway-{cluster_name}",
                                "namespace": namespace,
                                "port": 8787,
                            }
                        ],
                        "middlewares": [
                            {
                                "name": "clusters-prefix-dask-gateway"
                            }  # TODO: configurable
                        ],
                    }
                ],
            },
        }

    def make_ingressroutetcp(self, cluster_name, username, namespace):
        return {
            "apiVersion": "traefik.containo.us/v1alpha1",
            "kind": "IngressRouteTCP",
            "metadata": {
                "labels": self.get_labels(cluster_name, username, "dask-scheduler"),
                "annotations": self.common_annotations,
                "name": f"dask-gateway-{cluster_name}",
            },
            "spec": {
                "entryPoints": [self.proxy_tcp_entrypoint],
                "routes": [
                    {
                        "match": f"HostSNI(`daskgateway-{cluster_name}`)",
                        "services": [
                            {
                                "name": f"dask-gateway-{cluster_name}",
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
