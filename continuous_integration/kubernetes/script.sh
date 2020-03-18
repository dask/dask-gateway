#!/usr/bin/env bash
TEST_DASK_GATEWAY_KUBE=true TEST_DASK_GATEWAY_KUBE_ADDRESS=http://localhost:30200 py.test tests/kubernetes/ -vv
