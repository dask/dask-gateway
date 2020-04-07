FROM daskgateway/dask-gateway-base:0.7.0 AS dependencies
LABEL MAINTAINER="Jim Crist-Harif"

# Install dependencies
RUN /opt/conda/bin/conda install -c conda-forge --freeze-installed -y \
        aiohttp=3.6.2 \
        colorlog \
        cryptography \
        traitlets=4.3.3 \
        pyyaml \
    && /opt/conda/bin/pip install kubernetes-asyncio==10.0.1 --no-cache-dir --no-deps \
    && /opt/conda/bin/conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete

# Build dask-gateway-server from source in a builder stage
FROM dependencies AS builder

RUN mkdir -p /tmp/workdir
RUN mkdir -p /tmp/install-prefix
COPY . /tmp/workdir/
WORKDIR /tmp/workdir/
RUN /opt/conda/bin/python setup.py install \
    --no-build-proxy \
    --single-version-externally-managed \
    --record=record.txt \
    --prefix /tmp/install-prefix

# Final image - merge dependencies and built dask-gateway
FROM dependencies

COPY --from=builder /tmp/install-prefix /opt/conda/

# Setup working directory
USER root
WORKDIR /srv/dask-gateway
RUN chown dask:dask /srv/dask-gateway

USER 1000:1000
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
