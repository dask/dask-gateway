#!/usr/bin/env bash
source ~/.bashrc

set -xe

cd /working

conda install psutil pykerberos

pip install \
    dask \
    distributed \
    cryptography \
    tornado \
    traitlets \
    sqlalchemy \
    skein \
    pytest \
    pytest-asyncio

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
