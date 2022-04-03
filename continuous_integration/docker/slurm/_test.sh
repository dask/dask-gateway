#!/usr/bin/env bash
set -xe

cd /working
pytest -v tests/test_slurm_backend.py
