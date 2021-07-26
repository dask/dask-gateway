#!/usr/bin/env bash
source ~/.bashrc

set -xe

cd /working

conda install psutil
conda install -c conda-forge python=3.8

pip install \
    aiohttp \
    colorlog \
    dask \
    distributed \
    cryptography \
    traitlets \
    sqlalchemy \
    pytest \
    pytest-asyncio==0.12.0

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
