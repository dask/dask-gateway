#!/usr/bin/env bash
source ~/.bashrc

set -xe

cd /working

conda install psutil pykerberos

pip install \
    aiohttp \
    colorlog \
    dask \
    distributed \
    cryptography \
    traitlets \
    sqlalchemy \
    skein \
    "pytest<5.4.0" \
    pytest-asyncio

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
