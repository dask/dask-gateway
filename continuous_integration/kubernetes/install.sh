#!/usr/bin/env bash
set -xe

pip install -U \
    aiohttp \
    colorlog \
    cryptography \
    dask \
    distributed \
    pytest \
    pytest-asyncio==0.12.0 \
    kubernetes-asyncio \
    sqlalchemy \
    traitlets

pushd dask-gateway
sudo python setup.py develop
popd

pushd dask-gateway-server
sudo python setup.py develop --no-build-proxy
popd

pip list
