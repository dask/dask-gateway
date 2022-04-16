#!/usr/bin/env bash
set -xe

cd /working
pytest -v \
    tests/test_yarn_backend.py \
    tests/test_auth.py \
    -k "yarn or kerberos"
