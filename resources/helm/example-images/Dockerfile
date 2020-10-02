# An example Dockerfile for creating an image to use as a dask scheduler/worker
# with dask-gateway.
#
# The only requirement for such an image is that it has a version of
# `dask-gateway` installed that is compatible with the corresponding
# `dask-gateway-server`.
#
# Additionally, it is recommended that your `dask-gateway` image uses an init
# process (we recommend `tini` https://github.com/krallin/tini). This isn't
# strictly required, but you may run into weird bugs without an init process.
#
# Note that the version of Python or Python distribution doesn't matter. Here
# we'll install python via miniconda, then use `conda` to install all
# dependencies
FROM debian:buster-slim

# The miniconda version and corresponding SHA256 to use.
# You can see all available options here:
# https://docs.conda.io/en/latest/miniconda_hashes.html
ARG CONDA_VERSION=py38_4.8.3
ARG CONDA_SHA256=879457af6a0bf5b34b48c12de31d4df0ee2f06a8e68768e5758c3293b2daf688

# Does the following in one layer:
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

# Setup the dask user's environment
USER 1000:1000
ENV PATH="/opt/conda/bin:$PATH"
WORKDIR /home/dask/

# Configure the entrypoint to use tini
ENTRYPOINT ["tini", "-g", "--"]

# Install dask-gateway.
# If you need additional libraries, you can add them here.
# We run `conda clean -af` afterwards to drop unnecessary files from the
# docker image. This helps reduce the image size.
RUN conda install -c conda-forge --freeze-installed \
        dask-gateway \
        && \
    conda clean -af
