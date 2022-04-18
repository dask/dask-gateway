#!/usr/bin/env bash
set -xe

cd /working

# pykerberos needs to compile c++ code that depends on system libraries, by
# installing it from conda-forge, we avoid such hassle.
mamba install pykerberos

# This installs everything else we need for tests
pushd tests
pip install -r requirements.txt
popd

pip list
