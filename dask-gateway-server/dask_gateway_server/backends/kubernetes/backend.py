import asyncio
import os
import sys
import uuid
from base64 import b64encode, b64decode
from collections import defaultdict
from datetime import datetime, timezone

from traitlets import Float, Dict, Unicode, default, validate
from kubernetes_asyncio import client, config

from ... import __version__ as VERSION
from ... import models
from ...tls import new_keypair
from ...traitlets import MemoryLimit, Type
from ...utils import cancel_task, UniqueQueue
from ..base import Backend, ClusterConfig
from .utils import Reflector, DELETED


__all__ = ("KubeClusterConfig", "KubeBackend")


if sys.version_info[:2] >= (3, 7):

    def parse_k8s_timestamp(ts):
        t = datetime.fromisoformat(ts[:-1])
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


else:

    def parse_k8s_timestamp(ts):
        t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return int(t.replace(tzinfo=timezone.utc).timestamp() * 1000)


def merge_json_objects(a, b):
    """Merge two JSON objects recursively.

    - If a dict, keys are merged, preferring ``b``'s values
    - If a list, values from ``b`` are appended to ``a``

    Copying is minimized. No input collection will be mutated, but a deep copy
    is not performed.

    Parameters
    ----------
    a, b : dict
        JSON objects to be merged.

    Returns
    -------
    merged : dict
    """
    if b:
        # Use a shallow copy here to avoid needlessly copying
        a = a.copy()
        for key, b_val in b.items():
            if key in a:
                a_val = a[key]
                if isinstance(a_val, dict) and isinstance(b_val, dict):
                    a[key] = merge_json_objects(a_val, b_val)
                elif isinstance(a_val, list) and isinstance(b_val, list):
                    a[key] = a_val + b_val
                else:
                    a[key] = b_val
            else:
                a[key] = b_val
    return a


class KubeClusterConfig(ClusterConfig):
    """Configuration for a single Dask cluster running on kubernetes"""

    namespace = Unicode(
        "default",
        help="""
        Kubernetes namespace to launch pods in.

        If running inside a kubernetes cluster with service accounts enabled,
        defaults to the current namespace. If not, defaults to `default`
        """,
        config=True,
    )

    @default("namespace")
    def _default_namespace(self):
        ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return "default"

    image = Unicode(
        "daskgateway/dask-gateway:latest",
        help="Docker image to use for running user's containers.",
        config=True,
    )

    image_pull_policy = Unicode(
        "IfNotPresent",
        help="The image pull policy of the docker image specified in ``image``",
        config=True,
    )

    # Kubernetes is a bit different in types/granularity of resource requests.
    # We redefine these common fields here to support that.
    worker_cores = Float(
        1,
        min=0,
        help="""
        Number of cpu-cores available for a dask worker.
        """,
        config=True,
    )

    worker_cores_limit = Float(
        min=0,
        help="""
        Maximum number of cpu-cores available for a dask worker.

        Defaults to ``worker_cores``.
        """,
        config=True,
    )

    @default("worker_cores_limit")
    def _default_worker_cores_limit(self):
        return self.worker_cores

    worker_memory_limit = MemoryLimit(
        help="""
        Maximum number of bytes available for a dask worker. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Defaults to ``worker_memory``.
        """,
        config=True,
    )

    @default("worker_memory_limit")
    def _default_worker_memory_limit(self):
        return self.worker_memory

    scheduler_cores = Float(
        1,
        min=0,
        help="""
        Number of cpu-cores available for a dask scheduler.
        """,
        config=True,
    )

    scheduler_cores_limit = Float(
        min=0,
        help="""
        Maximum number of cpu-cores available for a dask scheduler.

        Defaults to ``scheduler_cores``.
        """,
        config=True,
    )

    @default("scheduler_cores_limit")
    def _default_scheduler_cores_limit(self):
        return self.scheduler_cores

    scheduler_memory_limit = MemoryLimit(
        help="""
        Maximum number of bytes available for a dask scheduler. Allows the
        following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Defaults to ``scheduler_memory``.
        """,
        config=True,
    )

    @default("scheduler_memory_limit")
    def _default_scheduler_memory_limit(self):
        return self.scheduler_memory

    worker_extra_container_config = Dict(
        help="""
        Any extra configuration for the worker container.

        This dict will be deep merged with the worker container (a
        ``V1Container`` object) before submission. Keys should match those in
        the `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        For example, here we add environment variables from a secret to the
        worker container:

        .. code::

            c.KubeClusterConfig.worker_extra_container_config = {
                "envFrom": [
                    {"secretRef": {"name": "my-env-secret"}}
                ]
            }
        """,
        config=True,
    )

    scheduler_extra_container_config = Dict(
        help="""
        Any extra configuration for the scheduler container.

        This dict will be deep merged with the scheduler container (a
        ``V1Container`` object) before submission. Keys should match those in
        the `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        See ``worker_extra_container_config`` for more information.
        """,
        config=True,
    )

    worker_extra_pod_config = Dict(
        help="""
        Any extra configuration for the worker pods.

        This dict will be deep merged with the worker pod spec (a ``V1PodSpec``
        object) before submission. Keys should match those in the `kubernetes
        spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        For example, here we add a toleration to worker pods.

        .. code::

            c.KubeClusterConfig.worker_extra_pod_config = {
                "tolerations": [
                    {
                        "key": "key",
                        "operator": "Equal",
                        "value": "value",
                        "effect": "NoSchedule",
                    }
                ]
            }
        """,
        config=True,
    )

    scheduler_extra_pod_config = Dict(
        help="""
        Any extra configuration for the scheduler pods.

        This dict will be deep merged with the scheduler pod spec (a
        ``V1PodSpec`` object) before submission. Keys should match those in the
        `kubernetes spec
        <https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#container-v1-core>`__,
        and should be camelCase.

        See ``worker_extra_pod_config`` for more information.
        """,
        config=True,
    )


class KubeBackend(Backend):
    """A dask-gateway backend for running on Kubernetes"""

    cluster_config_class = Type(
        "dask_gateway_server.backends.kubernetes.backend.KubeClusterConfig",
        klass="dask_gateway_server.backends.base.ClusterConfig",
        help="The cluster config class to use",
        config=True,
    )

    gateway_instance = Unicode(
        help="""
        A unique ID for this instance of dask-gateway.

        The controller must also be configured with the same ID.
        """,
        config=True,
    )

    @default("instance")
    def _default_instance(self):
        instance = os.environ.get("DASK_GATEWAY_INSTANCE")
        if not instance:
            raise ValueError("Must specify `c.KubeBackend.instance`")
        return instance

    crd_version = Unicode(
        "v1alpha1", help="The version for the DaskCluster CRD", config=True
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

    async def setup(self, app):
        try:
            # Not a coroutine for some reason
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()

        self.api_client = client.ApiClient()
        self.custom_client = client.CustomObjectsApi(api_client=self.api_client)

        self.clusters = {}
        self.username_to_clusters = defaultdict(dict)
        self.queue = UniqueQueue()
        self.reflector = Reflector(
            parent=self,
            kind="cluster",
            api_client=self.api_client,
            queue=self.queue,
            method=self.custom_client.list_cluster_custom_object,
            method_kwargs=dict(
                group="gateway.dask.org",
                version=self.crd_version,
                plural="daskclusters",
                label_selector=self.get_label_selector(),
            ),
        )
        await self.reflector.start()
        self._watcher_task = asyncio.ensure_future(self.watcher_loop())

    async def cleanup(self):
        if hasattr(self, "reflector"):
            await self.reflector.stop()
        if hasattr(self, "_watcher_task"):
            await cancel_task(self._watcher_task)
        if hasattr(self, "api_client"):
            await self.api_client.rest_client.pool_manager.close()

    async def start_cluster(self, user, cluster_options):
        options, config = await self.process_cluster_options(user, cluster_options)

        obj = self.make_cluster_object(user.name, options, config)

        res = await self.custom_client.create_namespaced_custom_object(
            "gateway.dask.org", self.crd_version, config.namespace, "daskclusters", obj
        )

        cluster_name = res["metadata"]["name"]

        self.reflector.put(cluster_name, res)
        self.update_cluster_cache(cluster_name)

        return cluster_name

    async def stop_cluster(self, cluster_name, failed=False):
        raise NotImplementedError

    async def get_cluster(self, cluster_name, wait=False):
        cluster = self.clusters.get(cluster_name)
        return cluster

    async def list_clusters(self, username=None, statuses=None):
        if statuses is None:
            select = lambda x: x.status <= models.ClusterStatus.RUNNING
        else:
            statuses = set(statuses)
            select = lambda x: x.status in statuses
        if username is None:
            return [cluster for cluster in self.clusters.values() if select(cluster)]
        else:
            clusters = self.username_to_clusters.get(username)
            if clusters is None:
                return []
            return [cluster for cluster in clusters.values() if select(cluster)]

    async def on_cluster_heartbeat(self, cluster_name, msg):
        raise NotImplementedError

    def get_label_selector(self):
        return f"gateway.dask.org/instance={self.gateway_instance}"

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

    def make_pod_template(self, cluster_name, username, config, is_worker=False):
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
            extra_container_config = self.api_client.sanitize_for_serialization(
                extra_container_config
            )
            container = merge_json_objects(container, extra_container_config)

        pod = {
            "metadata": {"labels": labels, "annotations": self.common_annotations},
            "spec": {
                "containers": [container],
                "volumes": [volume],
                "restartPolicy": "OnFailure",
                "automountServiceAccountToken": False,
            },
        }

        if extra_pod_config:
            extra_pod_config = self.api_client.sanitize_for_serialization(
                extra_pod_config
            )
            pod["spec"] = merge_json_objects(pod["spec"], extra_pod_config)

        return pod

    def make_secret_template(self, cluster_name, username):
        api_token = uuid.uuid4().hex
        tls_cert, tls_key = new_keypair(cluster_name)

        labels = self.get_labels(cluster_name, username, "credentials")

        return {
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

    def make_service_template(self, cluster_name, username, config):
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

    def make_ingressroute_template(self, cluster_name, username, config):
        route = f"{self.proxy_prefix}/clusters/{cluster_name}/"
        return {
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
                                "namespace": config.namespace,
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

    def make_ingressroutetcp_template(self, cluster_name, username, config):
        return {
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
                                "namespace": config.namespace,
                                "port": 8786,
                            }
                        ],
                    }
                ],
                "tls": {"passthrough": True},
            },
        }

    def make_cluster_object(self, username, options, config):
        cluster_name = uuid.uuid4().hex

        annotations = self.common_annotations
        labels = self.get_labels(cluster_name, username)

        secret = self.make_secret_template(cluster_name, username)
        scheduler = self.make_pod_template(cluster_name, username, config)
        worker = self.make_pod_template(cluster_name, username, config, is_worker=True)
        service = self.make_service_template(cluster_name, username, config)
        ingressroute = self.make_ingressroute_template(cluster_name, username, config)
        ingressroutetcp = self.make_ingressroutetcp_template(
            cluster_name, username, config
        )

        obj = {
            "apiVersion": f"gateway.dask.org/{self.crd_version}",
            "kind": "DaskCluster",
            "metadata": {
                "name": cluster_name,
                "labels": labels,
                "annotations": annotations,
            },
            "spec": {
                "schedulerTemplate": scheduler,
                "workerTemplate": worker,
                "replicas": 0,
                "secret": secret,
                "service": service,
                "ingressroute": ingressroute,
                "ingressroutetcp": ingressroutetcp,
                "info": {"options": options},
            },
            "status": {},
        }
        return obj

    async def watcher_loop(self):
        while True:
            kind, name = await self.queue.get()
            try:
                self.update_cluster_cache(name)
            except Exception as exc:
                self.log.info(
                    "Error updating cluster cache for %s:", name, exc_info=exc
                )

    def update_cluster_cache(self, name):
        obj = self.reflector.get(name)

        if obj is DELETED:
            self.log.debug("Deleting %s from cache", name)
            self.reflector.pop(name, None)
            # Delete the cluster from the cache
            cluster = self.clusters.pop(name, None)
            if cluster is not None:
                user = self.username_to_clusters.get(cluster.username)
                if user is not None:
                    user.pop(cluster.name, None)
                    if not user:
                        self.username_to_clusters.pop(cluster.username, None)
        else:
            self.log.debug("Updating %s in cache", name)

            # TODO: infer more of these fields from the backing kubernetes objects
            # Not all of these are correct
            secret = obj["spec"]["secret"]

            tls_cert = b64decode(secret["data"]["dask.crt"])
            tls_key = b64decode(secret["data"]["dask.pem"])
            token = b64decode(secret["data"]["api-token"]).decode()

            cluster = models.Cluster(
                name=name,
                username=obj["metadata"]["labels"]["gateway.dask.org/user"],
                options=obj["spec"]["info"]["options"],
                token=token,
                tls_cert=tls_cert,
                tls_key=tls_key,
                status=models.ClusterStatus.RUNNING,
                start_time=parse_k8s_timestamp(obj["metadata"]["creationTimestamp"]),
            )
            self.clusters[cluster.name] = cluster
            self.username_to_clusters[cluster.username][cluster.name] = cluster
