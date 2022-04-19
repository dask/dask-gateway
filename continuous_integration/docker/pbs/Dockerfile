# See continuous_integration/docker/README.md for details about this and other
# Dockerfiles under the continuous_integration/docker folder on their purpose
# and how to work with them.
#
FROM ghcr.io/dask/dask-gateway-ci-base:latest

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/continuous_integration/docker/pbs/Dockerfile"

# Notify dask-gateway tests that PBS is available
ENV TEST_DASK_GATEWAY_PBS true

# Install openpbs
#
# 1. Download and install .rpm
#
#    OpenPBS versions: https://github.com/openpbs/openpbs/releases
#
#    We use an old version because there isn't a modern one pre-built for
#    centos:7 as used in the base image. The old version was called propbs, so
#    there is a change needed in the download url related to that if switching
#    to a newwer version.
#
RUN INSTALL_OPENPBS_VERSION=19.1.3 \
 && yum install -y unzip \
  \
 && curl -sL -o /tmp/openpbs.zip https://github.com/openpbs/openpbs/releases/download/v${INSTALL_OPENPBS_VERSION}/pbspro_${INSTALL_OPENPBS_VERSION}.centos_7.zip \
 && unzip /tmp/openpbs.zip -d /opt/openpbs \
 && rm /tmp/openpbs.zip \
 && yum install -y \
        /opt/openpbs/*pbs*/*-server-*.rpm \
  \
 && yum remove -y unzip \
 && yum clean all \
 && rm -rf /var/cache/yum
#
# 2. Update PATH environment variable
#
#    Note that this PATH environment will be preserved when sudo is used to
#    switch to other users thanks to changes to /etc/sudoers.d/preserve_path,
#    which is configured in the base Dockerfile.
#
ENV PATH=/opt/pbs/bin:$PATH

# Copy over files
COPY ./files /

ENTRYPOINT ["/opt/python/bin/tini", "-g", "--"]
CMD ["/scripts/start.sh"]
