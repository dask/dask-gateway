FROM python:3.8.6-slim-buster as dependencies
LABEL MAINTAINER="Jim Crist-Harif"

RUN apt-get update \
    && apt-get install -y tini \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    aiohttp==3.7.2 \
    colorlog \
    cryptography \
    traitlets==4.3.3 \
    pyyaml \
    kubernetes-asyncio==11.0.0

# Build dask-gateway-server from source in a builder stage
FROM dependencies AS builder

RUN mkdir -p /tmp/workdir
RUN mkdir -p /tmp/install-prefix
COPY . /tmp/workdir/
WORKDIR /tmp/workdir/
RUN python setup.py install \
    --no-build-proxy \
    --single-version-externally-managed \
    --record=record.txt \
    --prefix /tmp/install-prefix

# Final image - merge dependencies and built dask-gateway
FROM dependencies

COPY --from=builder /tmp/install-prefix/bin/dask-gateway-server /usr/local/bin/
COPY --from=builder /tmp/install-prefix/lib /usr/local/lib/

# Create non-root user and working directory
WORKDIR /srv/dask-gateway
RUN useradd -m -U -u 1000 dask && chown dask:dask /srv/dask-gateway
USER 1000:1000

ENTRYPOINT ["tini", "-g", "--"]
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
