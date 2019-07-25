import os

from dask_gateway_helm import get_property


# Expose the gateway api outside the gateway container
c.DaskGateway.private_url = "http://0.0.0.0:8001"


# Configure the cluster manager
c.DaskGateway.cluster_manager_class = (
    "dask_gateway_server.managers.kubernetes.KubeClusterManager"
)
if "POD_NAMESPACE" in os.environ:
    c.KubeClusterManager.namespace = os.environ["POD_NAMESPACE"]

# Configure the cluster manager image
image_name = get_property("gateway.clusterManager.image.name")
if image_name:
    image_tag = get_property("gateway.clusterManager.image.tag")
    c.KubeClusterManager.image = (
        "%s:%s" % (image_name, image_tag) if image_tag else image_name
    )

# Forward cluster manager configuration
for field, prop_name in [
    # Scheduler resources
    ("scheduler_cores", "scheduler.cores.request"),
    ("scheduler_core_limit", "scheduler.cores.limit"),
    ("scheduler_memory", "scheduler.memory.request"),
    ("scheduler_memory_limit", "scheduler.memory.limit"),
    # Worker resources
    ("worker_cores", "worker.cores.request"),
    ("worker_core_limit", "worker.cores.limit"),
    ("worker_memory", "worker.memory.request"),
    ("worker_memory_limit", "worker.memory.limit"),
    # Additional fields
    ("image_pull_policy", "image.pullPolicy"),
    ("environment", "environment"),
    ("cluster_start_timeout", "clusterStartTimeout"),
    ("cluster_connect_timeout", "clusterConnectTimeout"),
    ("worker_start_timeout", "workerStartTimeout"),
    ("worker_connect_timeout", "workerConnectTimeout"),
]:
    value = get_property("gateway.clusterManager." + prop_name)
    if value is not None:
        setattr(c.KubeClusterManager, field, value)


# Authentication
c.DaskGateway.authenticator_class = "dask_gateway_server.auth.DummyAuthenticator"


# Proxies
if os.environ.get("DASK_GATEWAY_PROXY_CONTAINER", False):
    scheduler_proxy_host = web_proxy_host = ""
else:
    scheduler_proxy_host = os.environ["SCHEDULER_PROXY_API_SERVICE_HOST"]
    web_proxy_host = os.environ["WEB_PROXY_API_SERVICE_HOST"]
scheduler_proxy_port = int(os.environ["SCHEDULER_PROXY_API_SERVICE_PORT"])
web_proxy_port = int(os.environ["WEB_PROXY_API_SERVICE_PORT"])

c.SchedulerProxy.api_url = "http://%s:%d" % (scheduler_proxy_host, scheduler_proxy_port)
c.WebProxy.api_url = "http://%s:%d" % (web_proxy_host, web_proxy_port)
c.SchedulerProxy.externally_managed = True
c.WebProxy.externally_managed = True
