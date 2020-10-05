#!/usr/bin/env bash
TEST_DASK_GATEWAY_KUBE=true TEST_DASK_GATEWAY_KUBE_ADDRESS=http://localhost:30200/services/dask-gateway/ py.test tests/kubernetes/ -vv

helm install --dry-run --debug --generate-name resources/helm/dask-gateway/ -f continuous_integration/kubernetes/test_config.yaml
