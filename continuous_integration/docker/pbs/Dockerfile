FROM centos:7.6.1810

# Install miniconda and tini
RUN yum install -y bzip2 \
    && curl https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -p /opt/miniconda \
    && rm /tmp/miniconda.sh \
    && /opt/miniconda/bin/conda update conda -y \
    && /opt/miniconda/bin/conda config --set always_yes yes --set changeps1 no \
    && /opt/miniconda/bin/conda install tini \
    && /opt/miniconda/bin/conda clean -af \
    && find /opt/miniconda/ -type f -name '*.a' -delete \
    && find /opt/miniconda/ -type f -name '*.pyc' -delete \
    && yum remove -y bzip2 \
    && yum clean all \
    && rm -rf /var/cache/yum

# Install pbspro
RUN yum install -y unzip \
    && curl -L -o /tmp/pbspro.zip https://github.com/PBSPro/pbspro/releases/download/v18.1.4/pbspro_1.8.4.centos7.zip \
    && unzip /tmp/pbspro.zip -d /tmp/pbspro \
    && yum install -y /tmp/pbspro/pbspro*/pbspro-server-*.rpm \
    && yum remove -y unzip \
    && yum clean all \
    && rm -rf /var/cache/yum

# Notify dask-gateway tests that PBS is available
ENV TEST_DASK_GATEWAY_PBS true
ENV PBS_MASTER pbs_master

# Make a few user accounts
RUN useradd -m alice
RUN useradd -m bob

# Copy over files
COPY ./files /

ENTRYPOINT ["/opt/miniconda/bin/tini", "-g", "--"]
CMD ["/root/start.sh"]
