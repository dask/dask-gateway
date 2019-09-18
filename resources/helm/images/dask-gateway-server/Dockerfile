FROM daskgateway/dask-gateway-base
LABEL MAINTAINER="Jim Crist"

# Install dask-gateway
RUN /opt/conda/bin/pip install --no-cache-dir \
    dask-gateway-server[kubernetes]

# Setup working directory
USER root
WORKDIR /srv/dask-gateway
RUN chown dask:dask /srv/dask-gateway

USER dask:dask
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
