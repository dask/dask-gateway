FROM daskgateway/testing-base:latest

# Build and install slurm
RUN yum install -y epel-release \
    && yum install -y \
        bzip2 \
        epel-release \
        gcc \
        man2html \
        mariadb-server \
        mariadb-devel \
        munge \
        munge-devel \
        ncurses-devel \
        openssl \
        openssl-devel \
        perl \
        readline-devel \
    && curl -L https://download.schedmd.com/slurm/slurm-19.05.0.tar.bz2 -o /tmp/slurm.tar.bz2 \
    && cd /tmp \
    && tar -jxf /tmp/slurm.tar.bz2 \
    && cd slurm-19.05.0 \
    && ./configure --sysconfdir=/etc/slurm --with-mysql_config=/usr/bin --libdir=/usr/lib64 \
    && make install \
    && cd .. \
    && rm -rf slurm-19.05.0 \
    && rm -r slurm.tar.bz2 \
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

# Install supervisor and sudo
RUN yum install -y supervisor sudo \
    && yum clean all \
    && rm -rf /var/cache/yum

# Setup Slurm
RUN groupadd -r slurm  \
    && useradd -r -g slurm slurm \
    && mkdir /etc/sysconfig/slurm \
        /var/spool/slurmd \
        /var/run/slurmd \
        /var/lib/slurmd \
        /var/log/slurm \
    && chown slurm:slurm /var/spool/slurmd \
        /var/run/slurmd \
        /var/lib/slurmd \
        /var/log/slurm \
    && /sbin/create-munge-key

# Copy over files
COPY ./files /

RUN /root/init-mysql.sh

# Notify dask-gateway tests that Slurm is available
ENV TEST_DASK_GATEWAY_SLURM true

CMD ["/usr/bin/supervisord", "--configuration", "/etc/supervisord.conf"]
