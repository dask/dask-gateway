#!/usr/bin/env bash
set -xe

cd /working

# This installs everything we need for tests
pushd tests
pip install -r requirements.txt
popd

pip list
