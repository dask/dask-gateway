#!/usr/bin/env bash
set -xe

cd /working

# FIXME: Don't install pyzmq from conda-forge when pyzmq 25.0.0 is released, see
#        https://github.com/zeromq/pyzmq/issues/1821 and
#        https://pypi.org/project/pyzmq/.
#
mamba install -c conda-forge pyzmq

# This installs everything we need for tests
pip install -r tests/requirements.txt

pip list
