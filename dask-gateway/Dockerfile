# ** A base miniconda image **
FROM debian:buster-slim as miniconda
LABEL MAINTAINER="Jim Crist-Harif"

ARG CONDA_VERSION=py38_4.8.3
ARG CONDA_SHA256=879457af6a0bf5b34b48c12de31d4df0ee2f06a8e68768e5758c3293b2daf688

# - Create user dask
# - Install tini
# - Install miniconda build dependencies
# - Download miniconda and check the sha256 checksum
# - Install miniconda
# - Configure conda to minimize automatic package updates
# - Remove all conda managed static libraries
# - Cleanup conda files
# - Uninstall miniconda build dependencies
RUN useradd -m -U -u 1000 dask \
    && apt-get update \
    && apt-get install -y tini wget bzip2 \
    && wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-$CONDA_VERSION-Linux-x86_64.sh \
    && echo "${CONDA_SHA256}  Miniconda3-$CONDA_VERSION-Linux-x86_64.sh" > miniconda.checksum \
    && if [ $(sha256sum -c miniconda.checksum | awk '{print $2}') != "OK" ] ; then exit 1; fi \
    && mv Miniconda3-$CONDA_VERSION-Linux-x86_64.sh miniconda.sh \
    && sh ./miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh miniconda.checksum \
    && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> /home/dask/.profile \
    && echo "conda activate base" >> /home/dask/.profile \
    && echo "always_yes: true" >> /home/dask/.condarc \
    && echo "changeps1: false" >> /home/dask/.condarc \
    && echo "auto_update_conda: false" >> /home/dask/.condarc \
    && echo "aggressive_update_packages: []" >> /home/dask/.condarc \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && /opt/conda/bin/conda clean -afy \
    && chown -R dask:dask /opt/conda \
    && apt-get autoremove --purge -y wget bzip2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER 1000:1000
ENV PATH="/opt/conda/bin:$PATH"
WORKDIR /home/dask/

ENTRYPOINT ["tini", "-g", "--"]

# ** An image with all of dask-gateway's dependencies **
FROM miniconda as dependencies

ARG DASK_VERSION=2021.7.1
ARG DISTRIBUTED_VERSION=2021.7.1

# - Installs dask and dependencies
# - Cleans up conda files
# - Removes unnecessary static libraries
# - Removes unnecessary *.js.map files
# - Removes unminified bokeh js
RUN /opt/conda/bin/conda install -c conda-forge --freeze-installed -y \
        aiohttp=3.6.2 \
        dask==$DASK_VERSION \
        distributed==$DISTRIBUTED_VERSION \
        numpy==1.19.2 \
        pandas==1.1.4 \
    && /opt/conda/bin/conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -follow -type f -name '*.js' ! -name '*.min.js' -delete

# ** Build dask-gateway from source in a temporary image **
FROM dependencies AS builder

RUN mkdir -p /tmp/workdir
RUN mkdir -p /tmp/install-prefix
COPY . /tmp/workdir/
WORKDIR /tmp/workdir/
RUN /opt/conda/bin/pip install . --no-cache-dir --no-deps --prefix /tmp/install-prefix

# ** The final image **
FROM dependencies

# Copy over the built dask-gateway
COPY --from=builder /tmp/install-prefix /opt/conda/
