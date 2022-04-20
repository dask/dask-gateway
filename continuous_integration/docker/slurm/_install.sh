#!/usr/bin/env bash
set -xe

cd /working

# This installs everything we need for tests
pip install -r tests/requirements.txt

pip list
