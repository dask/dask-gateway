# ** A base miniconda image **
FROM debian:bullseye-slim as miniconda
LABEL MAINTAINER="Jim Crist-Harif"

# List of conda versions: https://repo.anaconda.com/miniconda/
#
# Miniconda 4.9.2 has been chosen as it has a aarch64 release and hasn't
# introduced a bug described in:
# https://stackoverflow.com/questions/68213186/illegal-instruction-error-when-verifying-anaconda-miniconda-install
#
# FIXME: Use micromamba or mambaforge instead, see
#        https://github.com/mamba-org/mamba for more info
#
ARG CONDA_VERSION=py39_4.10.3

# - Create user dask
RUN useradd -m -U -u 1000 dask

# - Install tini
# - Install miniconda build dependencies
RUN apt-get update \
    && apt-get install -y tini wget bzip2 \
    && rm -rf /var/lib/apt/lists/*

# - Download and install miniconda
# - Configure conda to minimize automatic package updates
# - Cleanup conda files
# - Uninstall miniconda build dependencies
RUN ARCH=$(uname -m) \
    && wget --quiet "https://repo.anaconda.com/miniconda/Miniconda3-$CONDA_VERSION-Linux-$ARCH.sh" \
    && mv "Miniconda3-$CONDA_VERSION-Linux-$ARCH.sh" miniconda.sh \
    && sh ./miniconda.sh -b -p /opt/conda \
    && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> /home/dask/.profile \
    && echo "conda activate base" >> /home/dask/.profile \
    && echo "always_yes: true" >> /home/dask/.condarc \
    && echo "changeps1: false" >> /home/dask/.condarc \
    && echo "auto_update_conda: false" >> /home/dask/.condarc \
    && echo "aggressive_update_packages: []" >> /home/dask/.condarc \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && /opt/conda/bin/conda clean -afy \
    && chown -R dask:dask /opt/conda

USER 1000:1000
ENV PATH="/opt/conda/bin:$PATH"
WORKDIR /home/dask/

ENTRYPOINT ["tini", "-g", "--"]



# ** An image with all of dask-gateway's dependencies **
FROM miniconda as dependencies

ARG DASK_VERSION=2021.11.2
ARG DISTRIBUTED_VERSION=2021.11.2

# - Installs dask and dependencies
# - Cleans up conda files
# - Removes unnecessary static libraries
# - Removes unnecessary *.js.map files
# - Removes unminified bokeh js
RUN /opt/conda/bin/conda install -c conda-forge --freeze-installed -y \
        aiohttp=3.8.1 \
        dask==$DASK_VERSION \
        distributed==$DISTRIBUTED_VERSION \
        numpy==1.21.4 \
        pandas==1.3.4 \
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
