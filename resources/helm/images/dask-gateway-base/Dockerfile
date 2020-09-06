# python 3.8: -> python:3.8-slim-buster
ARG base_image=debian:10.4-slim
FROM ${base_image}
# FROM python:3.8-slim-buster
LABEL MAINTAINER="Jim Crist-Harif"

# python 3.8: -> py38_4.8.3
ARG conda_version=py37_4.8.3
# pythom 3.8" -> 879457af6a0bf5b34b48c12de31d4df0ee2f06a8e68768e5758c3293b2daf688
ARG conda_sha256=bb2e3cedd2e78a8bb6872ab3ab5b1266a90f8c7004a22d8dc2ea5effeb6a439a
ENV CONDA_VERSION ${conda_version}
ENV CONDA_SHA256 ${conda_sha256}
ENV DEBIAN_FRONTEND noninteractive

# We do the following all in one block:
# - Create user dask
# - Install miniconda install dependencies
# - Download miniconda and check the sha256 checksum
# - Install miniconda
# - Configure conda to minimize automatic package updates
# - Install tini
# - Remove all conda managed static libraries
# - Cleanup conda files
# - Uninstall miniconda install dependencies
RUN useradd -m -U -u 1000 dask \
    && apt-get update \
    && apt-get install -y wget bzip2 \
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
    && /opt/conda/bin/conda install --freeze-installed tini -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && /opt/conda/bin/conda clean -afy \
    && chown -R dask:dask /opt/conda \
    && apt-get autoremove --purge -y wget bzip2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER 1000:1000
ENV PATH="/opt/conda/bin:$PATH"
WORKDIR /home/dask/

ENTRYPOINT ["/opt/conda/bin/tini", "-g", "--"]
CMD ["/bin/bash", "--login"]
