#!/usr/bin/env bash
set -xe

cd /working
pytest -v tests/test_pbs_backend.py
