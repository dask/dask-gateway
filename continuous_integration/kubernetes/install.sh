#!/usr/bin/env bash
set -xe

pip install \
    aiohttp \
    dask \
    distributed \
    flake8 \
    pytest \
    pytest-asyncio \
    kubernetes_asyncio

pushd dask-gateway
python setup.py develop
popd

pip list
