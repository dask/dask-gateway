#!/usr/bin/env bash
set -xe

cd /working

# pykerberos needs to compile c++ code that depends on system libraries, by
# installing it from conda-forge, we avoid such hassle.
mamba install pykerberos

# This installs everything else we need for tests
pip install -r tests/requirements.txt

pip list
