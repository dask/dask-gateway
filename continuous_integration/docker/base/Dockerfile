# See continuous_integration/docker/README.md for details about this and other
# Dockerfiles under the continuous_integration/docker folder on their purpose
# and how to work with them.
#
# centos:8 reached end-of-life 31 Dec 2021
# centos:7 reach end-of-life 30 Jun 2024
#
FROM centos:7

ARG python_version="3.10"
ARG go_version="1.18"

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.source="https://github.com/dask/dask-gateway"
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/continuous_integration/docker/base/Dockerfile"

# Configure yum to error on missing packages
RUN echo "skip_missing_names_on_install=False" >> /etc/yum.conf

# Install common yum packages
RUN yum install -y \
        sudo \
            # sudo is used to run commands as various other users
        git \
            # git is a requirement for golang to fetch dependencies during
            # compilation of the golang code we have in
            # dask-gateway-server/dask-gateway-proxy.
 && yum clean all \
 && rm -rf /var/cache/yum

# Install python and the following utilities:
#
# - tini: can wrap an container entrypoint to avoid misc issues, see
#   https://github.com/krallin/tini#readme
# - psutil: provides misc tools of relevance for debugging, see
#   https://psutil.readthedocs.io/en/latest/#about
#
# NOTE: micromamba is a slimmed mamba/conda executable functioning without a
#       pre-installed Python environment we use to install a Python version of
#       choice to not first need to install a full Python environment to then
#       install another Python environment.
#
#       See https://github.com/mamba-org/mamba#micromamba.
#
RUN yum install -y bzip2 \
  \
 && curl -sL https://micromamba.snakepit.net/api/micromamba/linux-64/latest \
  | tar --extract --verbose --bzip2 bin/micromamba --strip-components=1 \
 && ./micromamba install \
    --channel conda-forge \
    --root-prefix="/opt/python" \
    --prefix="/opt/python" \
          python="${python_version}" \
          mamba \
          psutil \
          tini \
 && rm ./micromamba \
 && /opt/python/bin/mamba clean -af \
 && find /opt/python/ -type f -name '*.a' -delete \
 && find /opt/python/ -type f -name '*.pyc' -delete \
  \
 && yum remove -y bzip2 \
 && yum clean all \
 && rm -rf /var/cache/yum

# Install go
RUN curl -sL https://dl.google.com/go/go${go_version}.linux-amd64.tar.gz \
  | tar --extract --verbose --gzip --directory=/opt/

# Put Python and Go environments on PATH
#
# NOTE: This PATH environment will be preserved if sudo is used to switch to
#       other users thanks to changes to /etc/sudoers.d/preserve_path.
#
ENV PATH=/opt/python/bin:/opt/go/bin:$PATH
COPY ./files/etc /etc/

# Make a few user accounts and a user group for later use
RUN useradd --create-home dask \
 && useradd --create-home alice \
 && useradd --create-home bob \
 && groupadd dask_users \
 && usermod --append --groups dask_users alice \
 && usermod --append --groups dask_users bob
