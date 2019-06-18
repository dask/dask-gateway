#!/usr/bin/env bash
set -xe

pip install \
    cryptography \
    tornado \
    traitlets \
    sqlalchemy \
    pytest \
    pytest-asyncio \
    kubernetes

if [[ "$1" == "-e" ]]; then
    pip install -e /working/dask-gateway-server/
else
    pip install /working/dask-gateway-server/
fi

pip list
