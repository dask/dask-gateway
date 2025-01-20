#!/usr/bin/env bash
set -xe

cd /working

# FIXME: pip should be installed to a modern version in the base image instead
#        of being upgraded here. It isn't because of
#        https://github.com/dask/dask-gateway/issues/837.
pip install "pip==24.*"

# pykerberos needs to compile c++ code that depends on system libraries, by
# installing it from conda-forge, we avoid such hassle.
#
mamba install -c conda-forge pykerberos

# This installs everything else we need for tests
pip install -r tests/requirements.txt

pip list
