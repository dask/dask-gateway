# This Dockerfile and image, ghcr.io/dask/dask-gateway-server, is used by the
# dask-gateway Helm chart, by the api pod and the controller pod.
#
# The pods are started with different commands:
#
# - api pod command:        dask-gateway-server ...
# - controller pod command: dask-gateway-server kube-controller ...
#
FROM python:3.10-slim-bullseye

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.source="https://github.com/dask/dask-gateway"
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/dask-gateway-server/Dockerfile"

# Install tini and upgrade linux packages are updated to patch known
# vulnerabilities.
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get install -y \
        tini \
 && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run as
RUN useradd --create-home --user-group --uid 1000 dask
USER dask:dask
ENV PATH=/home/dask/.local/bin:$PATH
WORKDIR /home/dask/

# Install dask-gateway-server
#
# The Golang proxy binary isn't built as the dask-gateway Helm chart relies on
# Traefik as a proxy instead to run in its dedicated pod.
#
COPY --chown=dask:dask . /srv/dask-gateway-server

RUN DASK_GATEWAY_SERVER__NO_PROXY=true pip install --no-cache-dir \
        "/srv/dask-gateway-server[kubernetes]"

ENTRYPOINT ["tini", "-g", "--"]
CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]
