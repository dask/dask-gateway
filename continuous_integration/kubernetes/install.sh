#!/usr/bin/env bash
set -xe

pip install \
    aiohttp \
    black \
    colorlog \
    cryptography \
    dask \
    distributed \
    flake8 \
    pytest \
    pytest-asyncio \
    kubernetes-asyncio \
    traitlets

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
