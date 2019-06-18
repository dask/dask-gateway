#!/usr/bin/env bash
set -xe

pushd /working

TEST_DASK_GATEWAY_KUBE=true py.test tests/test_kubernetes_cluster.py -v
