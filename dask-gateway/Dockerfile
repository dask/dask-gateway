# USAGE DISCLAIMER:
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
FROM python:3.10-slim-bullseye

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
RUN pip install --no-cache-dir /opt/dask-gateway

ENTRYPOINT ["tini", "-g", "--"]
# CMD is not set by default in the image as it will depend based on if this
# image is started as a scheduler or worker.
