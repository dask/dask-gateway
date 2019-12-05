FROM daskgateway/dask-gateway-base:0.6.0
LABEL MAINTAINER="Jim Crist"

# Install dask-gateway
RUN /opt/conda/bin/pip install --no-cache-dir \
    dask-gateway-server[kubernetes]==0.6.1

# Setup working directory
USER root
WORKDIR /srv/dask-gateway
RUN chown dask:dask /srv/dask-gateway

USER 1000:1000
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
