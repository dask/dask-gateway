#!/usr/bin/env bash
set -xe

pip install -U \
    aiohttp \
    black \
    colorlog \
    cryptography \
    dask \
    distributed \
    flake8 \
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
