FROM daskgateway/dask-gateway-server:latest

COPY --chown=dask:dask skaffold/condarc /opt/conda/condarc

RUN conda install -c conda-forge \
go=1.12.5 \ 
cryptography \
# Leaving tornado for backwards compatibility. 
tornado \
traitlets \
sqlalchemy \
python-kubernetes \
aiohttp \
colorlog \
-y

COPY --chown=dask:dask . /dask
WORKDIR /dask/dask-gateway-server/
# Perform an editable install to avoid packaging a wheel.
RUN pip install -e .
# TODO: Add kubernetes extras once the kubernetes backend is ready.
# RUN pip install -e .[kubernetes]
WORKDIR /srv/dask-gateway
USER root
RUN chown dask:dask /srv/dask-gateway
USER 1000:1000

CMD ["dask-gateway-server", "--config", "/etc/dask-gateway/dask_gateway_config.py"]``
