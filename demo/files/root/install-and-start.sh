#!/usr/bin/env bash
source ~/.bashrc

set -xe

pushd /working

# Install dependencies
conda install psutil

pip install \
    dask \
    distributed \
    bokeh \
    cryptography \
    tornado \
    traitlets \
    sqlalchemy \
    skein \
    pytest \
    pytest-asyncio

# Install dask-gateway
pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py build_go develop
popd

# List all packages
pip list

popd

# Start supervisord
exec supervisord --configuration /etc/supervisord.conf
