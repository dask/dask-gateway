FROM daskgateway/dask-gateway-base
LABEL MAINTAINER="Jim Crist"

# Does the following in one layer
# - Installs dask and dependencies
# - Cleans up conda files
# - Removes unnecessary static libraries
# - Removes unnecessary *.js.map files
# - Removes unminified bokeh js
RUN /opt/conda/bin/conda install -c conda-forge --freeze-installed -y \
        dask==2.2.0 \
        numpy==1.16.3 \
        pandas==0.24.2 \
    && /opt/conda/bin/conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -follow -type f -name '*.js' ! -name '*.min.js' -delete

# Install dask-gateway
RUN /opt/conda/bin/pip install dask-gateway --no-deps
