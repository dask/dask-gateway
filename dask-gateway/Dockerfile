# PURPOSE:
#
# This Dockerfile and image, ghcr.io/dask/dask-gateway, is used by the
# dask-gateway Helm chart. It acts as the sample image for scheduler and workers
# in Dask Clusters created by end users.
#
# The admin installing the dask-gateway Helm chart or its end users are meant to
# specify an image for the scheduler and worker pods to use that meets their
# needs for the Dask clusters they startup. Please build your own according to
# the documentation if this very limited image doesn't meet your needs.
#
# See https://gateway.dask.org/install-kube.html#using-a-custom-image.
#


# The build stage
# ---------------
# This stage is building Python wheels for use in later stages by using a base
# image that has more pre-requisites to do so, such as a C++ compiler.
#
# psutils, a dependency of distributed, is currently the sole reason we have to
# have this build stage.
#
FROM python:3.13-bullseye as build-stage

# Build wheels
#
# We set pip's cache directory and expose it across build stages via an
# ephemeral docker cache (--mount=type=cache,target=${PIP_CACHE_DIR}).
#
COPY . /opt/dask-gateway
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    pip install build \
 && pip wheel \
        --wheel-dir=/tmp/wheels \
        -r /opt/dask-gateway/Dockerfile.requirements.txt


# The final stage
# ---------------
#
FROM python:3.13-slim-bullseye as slim-stage

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.source="https://github.com/dask/dask-gateway"
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/dask-gateway/Dockerfile"

# Install tini and update linux packages to patch known vulnerabilities.
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

# Install dask-gateway
COPY --chown=dask:dask . /opt/dask-gateway
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    --mount=type=cache,from=build-stage,source=/tmp/wheels,target=/tmp/wheels \
    pip install \
        --find-links=/tmp/wheels/ \
        -r /opt/dask-gateway/Dockerfile.requirements.txt

# Only set ENTRYPOINT, CMD is configured at runtime by dask-gateway-server
ENTRYPOINT ["tini", "-g", "--"]
