FROM daskgateway/dask-gateway:latest

# Install dask-gateway
COPY ./dask-gateway /gateway-source
RUN /opt/conda/bin/pip install -U /gateway-source --no-deps
