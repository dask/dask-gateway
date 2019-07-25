FROM jcrist/dask-gateway-base
LABEL MAINTAINER="Jim Crist"

# Install dask-gateway
RUN /opt/conda/bin/pip install --no-cache-dir \
    dask-gateway-server[kubernetes]

# Install dask-gateway-helm helper library
ADD dask_gateway_helm.py /opt/conda/lib/python3.7/site-packages/dask_gateway_helm.py

# Setup config directory
USER root
RUN mkdir /etc/dask-gateway/
ADD dask_gateway_config.py /etc/dask-gateway/dask_gateway_config.py

# Setup working directory
WORKDIR /srv/dask-gateway
RUN chown dask:dask /srv/dask-gateway

USER dask:dask
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
