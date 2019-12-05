#!/usr/bin/env bash
export KUBECONFIG="$(k3d get-kubeconfig --name='k3s-default')"

echo "Running tests..."
kubectl exec dask-gateway-tests -n dask-gateway /working/continuous_integration/kubernetes/script-internal.sh
