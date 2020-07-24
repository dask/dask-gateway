FROM daskgateway/dask-gateway-base:0.7.1 as dependencies
LABEL MAINTAINER="Jim Crist-Harif"

# Does the following in one layer
# - Installs dask and dependencies
# - Cleans up conda files
# - Removes unnecessary static libraries
# - Removes unnecessary *.js.map files
# - Removes unminified bokeh js
RUN /opt/conda/bin/conda install -c conda-forge --freeze-installed -y \
        aiohttp=3.6.2 \
        dask==2.21.0 \
        distributed==2.21.0 \
        numpy==1.19.1 \
        pandas==1.0.5 \
    && /opt/conda/bin/conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -follow -type f -name '*.js' ! -name '*.min.js' -delete

# Build dask-gateway from source in a builder stage
FROM dependencies AS builder

RUN mkdir -p /tmp/workdir
RUN mkdir -p /tmp/install-prefix
COPY . /tmp/workdir/
WORKDIR /tmp/workdir/
RUN /opt/conda/bin/pip install . --no-cache-dir --no-deps --prefix /tmp/install-prefix

# Copy over the built dask-gateway
FROM dependencies

COPY --from=builder /tmp/install-prefix /opt/conda/
