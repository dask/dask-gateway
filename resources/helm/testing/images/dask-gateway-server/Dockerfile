FROM daskgateway/dask-gateway-server:latest

RUN /opt/conda/bin/conda install -c conda-forge go
ENV GOOS=linux GOARCH=amd64

# Install dask-gateway-server
COPY ./dask-gateway-server /source
USER root
RUN /usr/bin/env bash -c "cd /source && pip uninstall dask-gateway-server -y && python setup.py build_go install"
USER dask
