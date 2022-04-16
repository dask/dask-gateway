#!/usr/bin/env bash
set -xe

cd /working

# This installs everything besides compiling
# dask-gateway-server/dask-gateway-proxy
pushd tests
pip install -r requirements.txt
popd

# This ensures we also have a compiled dask-gateway-server/dask-gateway-proxy
# bundled with dask-gateway-proxy, something that we may not always want to do
# as part of installing the tests/requirements.txt.
pip install --editable dask-gateway-server

pip list
