import asyncio
import os

from traitlets import Float, List, Dict, Unicode, Instance, default

import kubernetes.client
import kubernetes.config
from kubernetes.client.models import (
    V1Container,
    V1EnvVar,
    V1LocalObjectReference,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1ResourceRequirements,
    V1Secret,
    V1Volume,
    V1VolumeMount,
    V1SecretVolumeSource,
)

from .base import ClusterManager
from .. import __version__ as VERSION
from ..utils import MemoryLimit


def configure_kubernetes_clients():
    # Patch kubernetes to avoid creating a threadpool
    from unittest.mock import Mock
    from kubernetes.client import api_client

    api_client.ThreadPool = lambda *args, **kwargs: Mock()

    # Load the appropriate kubernetes configuration
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()


configure_kubernetes_clients()


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
        "jcrist/dask-gateway:latest",
        help="Docker image to use for running user's containers.",
        config=True,
    )

    image_pull_policy = Unicode(
        "IfNotPresent",
        help="The image pull policy of the docker image specified in ``image``",
        config=True,
    )

    image_pull_secrets = List(
        trait=Unicode(),
        help="""
        A list of secrets to use for pulling images from a private repository.

        See `the Kubernetes documentation
        <https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod>`__
        for more information.
        """,
        config=True,
    )

    working_dir = Unicode(
        help="""
        The working directory where the command will be run inside the
        container. Default is the working directory defined in the Dockerfile.
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

    # Internal fields
    kube_client = Instance(kubernetes.client.CoreV1Api, args=())

    def get_tls_paths(self, cluster_info):
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

    def get_labels_for(self, cluster_info, component, worker_name=None):
        labels = self.common_labels.copy()
        labels.update(
            {
                "app.kubernetes.io/component": component,
                "cluster-name": cluster_info.cluster_name,
            }
        )
        if worker_name:
            labels["worker-name"] = worker_name
        return labels

    def make_secret_spec(self, cluster_info):
        name = "dask-gateway-tls-%s" % cluster_info.cluster_name
        labels = self.get_labels_for(cluster_info, "dask-gateway-tls")
        annotations = self.common_annotations

        secret = V1Secret(
            kind="Secret",
            api_version="v1",
            string_data={
                "dask.crt": cluster_info.tls_cert.decode(),
                "dask.pem": cluster_info.tls_key.decode(),
            },
            metadata=V1ObjectMeta(name=name, labels=labels, annotations=annotations),
        )
        return secret

    def make_pod_spec(self, cluster_info, tls_secret, worker_name=None):
        annotations = self.common_annotations
        env = self.get_env(cluster_info)

        if worker_name is not None:
            # Worker
            name = "dask-gateway-worker-%s" % worker_name
            container_name = "dask-gateway-worker"
            labels = self.get_labels_for(
                cluster_info, "dask-gateway-worker", worker_name=worker_name
            )
            mem_req = self.worker_memory
            mem_lim = self.worker_memory_limit
            cpu_req = self.worker_cores
            cpu_lim = self.worker_cores_limit
            env["DASK_GATEWAY_WORKER_NAME"] = worker_name
            cmd = self.worker_command
        else:
            # Scheduler
            name = "dask-gateway-scheduler-%s" % cluster_info.cluster_name
            container_name = "dask-gateway-scheduler"
            labels = self.get_labels_for(cluster_info, "dask-gateway-scheduler")
            mem_req = self.scheduler_memory
            mem_lim = self.scheduler_memory_limit
            cpu_req = self.scheduler_cores
            cpu_lim = self.scheduler_cores_limit
            cmd = self.scheduler_command

        volume = V1Volume(
            name="dask-credentials", secret=V1SecretVolumeSource(secret_name=tls_secret)
        )

        container = V1Container(
            name=container_name,
            image=self.image,
            args=cmd,
            env=[V1EnvVar(k, v) for k, v in env.items()],
            working_dir=self.working_dir or None,
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

        pod = V1Pod(
            kind="Pod",
            api_version="v1",
            metadata=V1ObjectMeta(name=name, labels=labels, annotations=annotations),
            spec=V1PodSpec(
                containers=[container], volumes=[volume], restart_policy="Never"
            ),
        )

        if self.image_pull_secrets:
            pod.spec.image_pull_secrets = [
                V1LocalObjectReference(name=s) for s in self.image_pull_secrets
            ]

        # Ensure we don't accidentally give access to the kubernetes API
        pod.spec.automount_service_account_token = False

        return pod

    async def start_cluster(self, cluster_info):
        tls_secret = self.make_secret_spec(cluster_info)

        secret_name = tls_secret.metadata.name

        self.log.debug("Creating secret %s", secret_name)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_secret, self.namespace, tls_secret
        )
        yield {"secret_name": secret_name}

        pod = self.make_pod_spec(cluster_info, secret_name)

        self.log.debug("Starting pod %s", pod.metadata.name)

        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_pod, self.namespace, pod
        )

        yield {"secret_name": secret_name, "pod_name": pod.metadata.name}

    async def stop_cluster(self, cluster_info, cluster_state):
        loop = asyncio.get_running_loop()

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

    async def start_worker(self, worker_name, cluster_info, cluster_state):
        secret_name = cluster_state["secret_name"]

        pod = self.make_pod_spec(cluster_info, secret_name, worker_name)

        self.log.debug("Starting pod %s", pod.metadata.name)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self.kube_client.create_namespaced_pod, self.namespace, pod
        )

        yield {"pod_name": pod.metadata.name}

    async def stop_worker(self, worker_name, worker_state, cluster_info, cluster_state):
        pod_name = worker_state.get("pod_name")
        if pod_name is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self.kube_client.delete_namespaced_pod, pod_name, self.namespace
            )
