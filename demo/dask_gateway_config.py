# Configure the 3 main addresses
c.DaskGateway.gateway_url = "tls://0.0.0.0:8786"
c.DaskGateway.public_url = "http://0.0.0.0:8787"
c.DaskGateway.private_url = "http://127.0.0.1:8081"

# Set the database location
c.DaskGateway.db_url = "sqlite:////var/dask-gateway/dask_gateway.sqlite"

# Set the encryption keys for the database. These should be randomly generated,
# and probably set via the DASK_GATEWAY_ENCRYPT_KEYS environment variable
# instead of stored in plaintext in the config.
c.DaskGateway.db_encrypt_keys = [
    b"z+hBxgauqbndqCZSqJMsKYI7FDMth6YYGqwnx7Ssqvg=",
    b"2Ai+D94CX/47LiRv3TLWV+K18TQ3fB1nTz18zmALbc8=",
]

# Run the proxies separately from the gateway. The auth_tokens here should be
# randomly generated, and probably set via the DASK_GATEWAY_PROXY_TOKEN
# environment variable instead of stored in plaintext in the config.
c.WebProxy.auth_token = "supersecret"
c.WebProxy.externally_managed = True
c.WebProxy.api_url = "http://127.0.0.1:8082"
c.SchedulerProxy.auth_token = "supersecret"
c.SchedulerProxy.externally_managed = True
c.SchedulerProxy.api_url = "http://127.0.0.1:8083"

# Don't shutdown active clusters when the gateway process exits
c.DaskGateway.stop_clusters_on_shutdown = False

# Use the dummy authenticator for easy login. This should never be used in production
c.DaskGateway.authenticator_class = "dask_gateway_server.auth.DummyAuthenticator"

# Configure the gateway to use YARN as the cluster manager
c.DaskGateway.cluster_manager_class = (
    "dask_gateway_server.managers.yarn.YarnClusterManager"
)

c.YarnClusterManager.scheduler_cmd = "/opt/miniconda/bin/dask-gateway-scheduler"
c.YarnClusterManager.worker_cmd = "/opt/miniconda/bin/dask-gateway-worker"
c.YarnClusterManager.keytab = "/home/dask/dask.keytab"
c.YarnClusterManager.principal = "dask"
c.YarnClusterManager.scheduler_memory = "512M"
c.YarnClusterManager.worker_memory = "512M"
c.YarnClusterManager.scheduler_cores = 1
c.YarnClusterManager.worker_cores = 2
c.YarnClusterManager.cluster_start_timeout = 30
