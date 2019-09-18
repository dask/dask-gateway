FROM daskgateway/testing-hadoop:latest
MAINTAINER jcrist

# Copy over files
COPY ./files /

RUN mkdir -p /var/log/dask-gateway/ \
    && chown dask /var/log/dask-gateway/ \
    && mkdir -p /var/dask-gateway \
    && chown dask /var/dask-gateway

CMD ["/root/install-and-start.sh"]
