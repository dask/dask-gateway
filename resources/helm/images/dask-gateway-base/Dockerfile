FROM debian:10.1-slim
LABEL MAINTAINER="Jim Crist"

ENV CONDA_VERSION 4.7.12.1
ENV CONDA_MD5 81c773ff87af5cfac79ab862942ab6b3

# We do the following all in one block:
# - Create user dask
# - Install miniconda install dependencies
# - Download miniconda and check the md5sum
# - Install miniconda
# - Configure conda to minimize automatic package updates
# - Install tini
# - Remove all conda managed static libraries
# - Cleanup conda files
# - Uninstall miniconda install dependencies
RUN useradd -m -U -u 1000 dask \
    && apt-get update \
    && apt-get install -y wget bzip2 \
    && wget --quiet https://repo.continuum.io/miniconda/Miniconda3-$CONDA_VERSION-Linux-x86_64.sh \
    && echo "${CONDA_MD5}  Miniconda3-$CONDA_VERSION-Linux-x86_64.sh" > miniconda.md5 \
    && if [ $(md5sum -c miniconda.md5 | awk '{print $2}') != "OK" ] ; then exit 1; fi \
    && mv Miniconda3-$CONDA_VERSION-Linux-x86_64.sh miniconda.sh \
    && sh ./miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh miniconda.md5 \
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
