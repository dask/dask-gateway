# See continuous_integration/docker/README.md for details about this and other
# Dockerfiles under the continuous_integration/docker folder on their purpose
# and how to work with them.
#
FROM ghcr.io/dask/dask-gateway-ci-base:latest

# Set labels based on the Open Containers Initiative (OCI):
# https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
#
LABEL org.opencontainers.image.url="https://github.com/dask/dask-gateway/blob/HEAD/continuous_integration/docker/slurm/Dockerfile"

# Notify dask-gateway tests that Slurm is available
ENV TEST_DASK_GATEWAY_SLURM true

# Install Slurm
#
# 1. Download and compile slurm
#
#    Slurm versions:      https://download.schedmd.com/slurm/
#    Slurm release notes: https://github.com/SchedMD/slurm/blame/HEAD/RELEASE_NOTES
#
RUN INSTALL_SLURM_VERSION=21.08.6 \
 && yum install -y \
        # required to install supervisor (and more?)
        epel-release \
 && yum install -y \
        # temporary installation dependencies later uninstalled
        bzip2 \
        gcc \
        mariadb-devel \
        munge-devel \
        ncurses-devel \
        openssl-devel \
        readline-devel \
        # persistent installation dependencies
        man2html \
        mariadb-server \
        munge \
        openssl \
        perl \
        supervisor \
  \
 && curl -sL https://download.schedmd.com/slurm/slurm-${INSTALL_SLURM_VERSION}.tar.bz2 \
  | tar --extract --verbose --bzip2 --directory=/tmp \
 && cd /tmp/slurm-* \
 && ./configure \
        --sysconfdir=/etc/slurm \
        --with-mysql_config=/usr/bin \
        --libdir=/usr/lib64 \
 && make install \
 && rm -rf /tmp/slurm-* \
  \
 && yum remove -y \
        bzip2 \
        gcc \
        mariadb-devel \
        munge-devel \
        ncurses-devel \
        openssl-devel \
        readline-devel \
 && yum clean all \
 && rm -rf /var/cache/yum
#
# 2. Setup Slurm
#
RUN groupadd --system slurm  \
 && useradd --system --gid slurm slurm \
 && mkdir \
        /etc/sysconfig/slurm \
        /var/lib/slurmd \
        /var/log/slurm \
        /var/run/slurmd \
        /var/spool/slurmd \
 && chown slurm:slurm \
        /var/lib/slurmd \
        /var/log/slurm \
        /var/run/slurmd \
        /var/spool/slurmd \
 && /sbin/create-munge-key
#
# 3. Copy misc configuration files
#
COPY --chown=slurm:slurm ./files/etc/slurm /etc/slurm/
COPY ./files/etc/sudoers.d /etc/sudoers.d/
COPY ./files/etc/supervisord.conf /etc/
COPY ./files/etc/supervisord.d /etc/supervisord.d/
RUN chmod 644 /etc/slurm/slurm.conf \
 && chmod 600 /etc/slurm/slurmdbd.conf \
 && chmod 440 /etc/sudoers.d/dask \
 && chmod 644 /etc/supervisord.conf \
 && chmod 644 /etc/supervisord.d/*
#
# 4. Initialize a Slurm database
#
COPY ./files/scripts /scripts/
RUN /scripts/init-mysql.sh

ENTRYPOINT ["/usr/bin/supervisord", "--configuration", "/etc/supervisord.conf"]
