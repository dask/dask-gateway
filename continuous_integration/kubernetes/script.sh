#!/usr/bin/env bash

echo "Running tests..."
kubectl exec dask-gateway-tests -n dask-gateway /working/continuous_integration/kubernetes/script-internal.sh
