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

pip install -e /working/dask-gateway-server/

pip list
