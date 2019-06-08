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

# Install go
RUN curl https://dl.google.com/go/go1.12.2.linux-amd64.tar.gz -o /tmp/go.tar.gz \
    && tar -xzf /tmp/go.tar.gz -C /opt/ \
    && rm /tmp/go.tar.gz

# Make a few user accounts, and configure their bashrc files
RUN useradd -m dask \
    && useradd -m alice \
    && useradd -m bob \
    && groupadd dask_users \
    && usermod -a -G dask_users alice \
    && usermod -a -G dask_users bob \
    && echo 'export PATH="/opt/go/bin:/opt/miniconda/bin:$PATH"' >> ~/.bashrc \
    && echo 'export PATH="/opt/miniconda/bin:$PATH"' >> /home/dask/.bashrc \
    && echo 'export PATH="/opt/miniconda/bin:$PATH"' >> /home/alice/.bashrc \
    && echo 'export PATH="/opt/miniconda/bin:$PATH"' >> /home/bob/.bashrc
