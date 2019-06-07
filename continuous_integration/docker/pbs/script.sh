#!/usr/bin/env bash
source ~/.bashrc

set -xe

cd /working

py.test tests/test_pbs_cluster.py -v
