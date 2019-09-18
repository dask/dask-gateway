import os
import threading
import time

from traitlets import Int, Float, Dict, Unicode, Instance, default
from traitlets.config import SingletonConfigurable

try:
    import kubernetes
except ImportError:
    raise ImportError(
        "'%s.KubeClusterManager' requires 'kubernetes' as a dependency. "
        "To install required dependencies, use:\n"
        "  $ pip install dask-gateway-server[kubernetes]\n"
        "or\n"
        "  $ conda install dask-gateway-server-kubernetes -c conda-forge\n" % __name__
    )

import kubernetes.client
import kubernetes.config
import kubernetes.watch
from kubernetes.client.models import (
    V1Container,
    V1EnvVar,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Secret,
    V1Volume,
    V1VolumeMount,
    V1SecretVolumeSource,
)
from urllib3.exceptions import ReadTimeoutError

from .base import ClusterManager
from .. import __version__ as VERSION
from ..compat import get_running_loop
from ..utils import MemoryLimit


def patch_kube_threadpool(_has_run=[]):
    if not _has_run:
        # Patch kubernetes to avoid creating a threadpool
        from unittest.mock import Mock
        from kubernetes.client import api_client

        api_client.ThreadPool = lambda *args, **kwargs: Mock()
        _has_run.append(1)


def load_kube_config():
    # Load the appropriate kubernetes configuration
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()


_shared_api_client = None


def _get_api_client():
    global _shared_api_client
    if _shared_api_client is None:
        patch_kube_threadpool()
        _shared_api_client = kubernetes.client.ApiClient()
    return _shared_api_client


def merge_kube_objects(orig, changes):
    """Merges a kubernetes object `orig` with another object `changes`.

    A deep merge strategy is used, merging both keys and lists.
    """
    api_client = _get_api_client()
    orig = api_client.sanitize_for_serialization(orig)
    changes = api_client.sanitize_for_serialization(changes)
    return merge_json_objects(orig, changes)


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


class PodReflector(SingletonConfigurable):
    request_timeout = Float(
        60,
        help="""
        Network timeout for kubernetes watch.

        Trigger watch reconnect when a given request is taking too long,
        which can indicate network issues.
        """,
        config=True,
    )

    timeout_seconds = Int(
        10,
        help="""
        Timeout (in seconds) for kubernetes watch.

        Trigger watch reconnect when no watch event has been received.
        This will cause a full reload of the currently existing resources
        from the API server.
        """,
        config=True,
    )

    restart_seconds = Float(
        30, help="Maximum time (in seconds) before restarting a watch.", config=True
    )

    kube_client = Instance(kubernetes.client.CoreV1Api)
    label_selector = Unicode()
    namespace = Unicode()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_load_future = get_running_loop().create_future()
        self.stopped = False
        self.start()

    def __del__(self):
        self.stop()

    def _list_and_update(self):
        initial_resources = self.kube_client.list_namespaced_pod(
            self.namespace,
            label_selector=self.label_selector,
            _request_timeout=self.request_timeout,
        )
        self.pods = {p.metadata.name: p for p in initial_resources.items}
        # return the resource version so we can hook up a watch
        return initial_resources.metadata.resource_version

    def _watch_and_update(self):
        self.log.info("Starting pod watcher...")
        cur_delay = 0.1
        while not self.stopped:
            start = time.monotonic()
            watch = kubernetes.watch.Watch()
            try:
                resource_version = self._list_and_update()
                if not self.first_load_future.done():
                    # signal that we've loaded our initial data
                    self.first_load_future.set_result(None)
                kwargs = {
                    "namespace": self.namespace,
                    "label_selector": self.label_selector,
                    "resource_version": resource_version,
                    "_request_timeout": self.request_timeout,
                    "timeout_seconds": self.timeout_seconds,
                }
                for ev in watch.stream(self.kube_client.list_namespaced_pod, **kwargs):
                    cur_delay = 0.1
                    pod = ev["object"]
                    if ev["type"] == "DELETED":
                        self.pods.pop(pod.metadata.name, None)
                    else:
                        self.pods[pod.metadata.name] = pod
                    if self.stopped:
                        # Check in inner loop to provide faster shutdown
                        break
                    watch_duration = time.monotonic() - start
                    if watch_duration >= self.restart_seconds:
                        self.log.debug(
                            "Restarting pod watcher after %.1f seconds", watch_duration
                        )
                        break
            except ReadTimeoutError:
                # network read time out, just continue and restart the watch
                # this could be due to a network problem or just low activity
                self.log.warning("Read timeout watching pods, reconnecting")
                continue
            except Exception as exc:
                if cur_delay < 30:
                    cur_delay = cur_delay * 2
                self.log.error(
                    "Error when watching pods, retrying in %.1f seconds...",
                    cur_delay,
                    exc_info=exc,
                )
                time.sleep(cur_delay)
                continue
            else:
                # no events on watch, reconnect
                self.log.debug("Pod watcher timeout, restarting")
            finally:
                watch.stop()
        self.log.debug("Pod watcher stopped")

    def start(self):
        self.watch_thread = threading.Thread(target=self._watch_and_update, daemon=True)
        self.watch_thread.start()

    def stop(self):
        self.stopped = True


class KubeClusterManager(ClusterManager):
    """A cluster manager for deploying Dask on a Kubernetes cluster."""

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
        """
        Set namespace default to current namespace if running in a k8s cluster

        If not in a k8s cluster with service accounts enabled, default to
        `default`
        """
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

            c.KubeClusterManager.worker_extra_container_config = {
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

            c.KubeClusterManager.worker_extra_pod_config = {
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

    # Internal fields
    kube_client = Instance(kubernetes.client.CoreV1Api)

    @default("kube_client")
    def _default_kube_client(self):
        patch_kube_threadpool()
        load_kube_config()
        return kubernetes.client.CoreV1Api()

    def __init__(self, *args, **kwargs):
        # If in tests
        testing = kwargs.pop("_testing", False)
        super().__init__(*args, **kwargs)
        if testing:
            return
        # Starts the pod reflector for the first instance only
        self.pod_reflector = PodReflector.instance(
            parent=self.parent or self,
            kube_client=self.kube_client,
            namespace=self.namespace,
            label_selector=self.pod_label_selector,
        )

    def get_tls_paths(self):
        """Get the absolute paths to the tls cert and key files."""
        return "/etc/dask-credentials/dask.crt", "/etc/dask-credentials/dask.pem"

    @property
    def worker_command(self):
        """The full command (with args) to launch a dask worker"""
        return [
            self.worker_cmd,
            "--nthreads",
            str(int(self.worker_cores_limit)),
            "--memory-limit",
            str(self.worker_memory_limit),
        ]

    @property
    def scheduler_command(self):
        """The full command (with args) to launch a dask scheduler"""
        return [self.scheduler_cmd]

    @property
    def pod_label_selector(self):
        """A label selector for all pods started by dask-gateway"""
        return ",".join("%s=%s" % (k, v) for k, v in self.common_labels.items())

    def get_labels_for(self, component, worker_name=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "app.kubernetes.io/component": component,
                "cluster-name": self.cluster_name,
            }
        )
        if worker_name:
            labels["worker-name"] = worker_name
        return labels

    def make_secret_spec(self):
        name = "dask-gateway-tls-%s" % self.cluster_name
        labels = self.get_labels_for("dask-gateway-tls")
        annotations = self.common_annotations

        secret = V1Secret(
            kind="Secret",
            api_version="v1",
            string_data={
                "dask.crt": self.tls_cert.decode(),
                "dask.pem": self.tls_key.decode(),
            },
            metadata=V1ObjectMeta(name=name, labels=labels, annotations=annotations),
        )
        return secret

    def make_pod_spec(self, tls_secret, worker_name=None):
        annotations = self.common_annotations
        env = self.get_env()

        if worker_name is not None:
            # Worker
            name = "dask-gateway-worker-%s" % worker_name
            container_name = "dask-gateway-worker"
            labels = self.get_labels_for("dask-gateway-worker", worker_name=worker_name)
            mem_req = self.worker_memory
            mem_lim = self.worker_memory_limit
            cpu_req = self.worker_cores
            cpu_lim = self.worker_cores_limit
            env["DASK_GATEWAY_WORKER_NAME"] = worker_name
            cmd = self.worker_command
            extra_pod_config = self.worker_extra_pod_config
            extra_container_config = self.worker_extra_container_config
        else:
            # Scheduler
            name = "dask-gateway-scheduler-%s" % self.cluster_name
            container_name = "dask-gateway-scheduler"
            labels = self.get_labels_for("dask-gateway-scheduler")
            mem_req = self.scheduler_memory
            mem_lim = self.scheduler_memory_limit
            cpu_req = self.scheduler_cores
            cpu_lim = self.scheduler_cores_limit
            cmd = self.scheduler_command
            extra_pod_config = self.scheduler_extra_pod_config
            extra_container_config = self.scheduler_extra_container_config

        volume = V1Volume(
            name="dask-credentials", secret=V1SecretVolumeSource(secret_name=tls_secret)
        )

        container = V1Container(
            name=container_name,
            image=self.image,
            args=cmd,
            env=[V1EnvVar(k, v) for k, v in env.items()],
            image_pull_policy=self.image_pull_policy,
            resources=V1ResourceRequirements(
                requests={"cpu": cpu_req, "memory": mem_req},
                limits={"cpu": cpu_lim, "memory": mem_lim},
            ),
            volume_mounts=[
                V1VolumeMount(
                    name=volume.name,
                    mount_path="/etc/dask-credentials/",
                    read_only=True,
                )
            ],
        )

        if extra_container_config:
            container = merge_kube_objects(container, extra_container_config)

        pod = V1Pod(
            kind="Pod",
            api_version="v1",
            metadata=V1ObjectMeta(name=name, labels=labels, annotations=annotations),
            spec=V1PodSpec(
                containers=[container], volumes=[volume], restart_policy="Never"
            ),
        )

        # Ensure we don't accidentally give access to the kubernetes API
        pod.spec.automount_service_account_token = False

        if extra_pod_config:
            pod.spec = merge_kube_objects(pod.spec, extra_pod_config)

        return pod

    async def start_cluster(self):
        tls_secret = self.make_secret_spec()

        secret_name = tls_secret.metadata.name

        self.log.debug("Creating secret %s", secret_name)

        loop = get_running_loop()
        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_secret, self.namespace, tls_secret
        )
        yield {"secret_name": secret_name}

        pod = self.make_pod_spec(secret_name)

        self.log.debug("Starting pod %s", pod.metadata.name)

        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_pod, self.namespace, pod
        )

        yield {"secret_name": secret_name, "pod_name": pod.metadata.name}

    async def pod_status(self, pod_name, container_name):
        if pod_name is None:
            return

        # Ensure initial data already loaded
        if not self.pod_reflector.first_load_future.done():
            await self.pod_reflector.first_load_future

        pod = self.pod_reflector.pods.get(pod_name)
        if pod is not None:
            if pod.status.phase == "Pending":
                return True
            if pod.status.container_statuses is None:
                return False
            for c in pod.status.container_statuses:
                if c.name == container_name:
                    if c.state.terminated:
                        msg = (
                            "Container stopped with exit code %d"
                            % c.state.terminated.exit_code
                        )
                        return False, msg
                    return True
        # pod doesn't exist or has been deleted
        return False, ("Pod %s already deleted" % pod_name)

    async def cluster_status(self, cluster_state):
        return await self.pod_status(
            cluster_state.get("pod_name"), "dask-gateway-scheduler"
        )

    async def worker_status(self, worker_name, worker_state, cluster_state):
        return await self.pod_status(
            worker_state.get("pod_name"), "dask-gateway-worker"
        )

    async def stop_cluster(self, cluster_state):
        loop = get_running_loop()

        pod_name = cluster_state.get("pod_name")
        if pod_name is not None:
            await loop.run_in_executor(
                None, self.kube_client.delete_namespaced_pod, pod_name, self.namespace
            )

        secret_name = cluster_state.get("secret_name")
        if secret_name is not None:
            await loop.run_in_executor(
                None,
                self.kube_client.delete_namespaced_secret,
                secret_name,
                self.namespace,
            )

    async def start_worker(self, worker_name, cluster_state):
        secret_name = cluster_state["secret_name"]

        pod = self.make_pod_spec(secret_name, worker_name)

        self.log.debug("Starting pod %s", pod.metadata.name)

        loop = get_running_loop()
        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_pod, self.namespace, pod
        )

        yield {"pod_name": pod.metadata.name}

    async def stop_worker(self, worker_name, worker_state, cluster_state):
        pod_name = worker_state.get("pod_name")
        if pod_name is not None:
            loop = get_running_loop()
            await loop.run_in_executor(
                None, self.kube_client.delete_namespaced_pod, pod_name, self.namespace
            )
