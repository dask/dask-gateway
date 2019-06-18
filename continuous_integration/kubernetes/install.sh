#!/usr/bin/env bash

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../.." && pwd)"

echo "Building Go source"
pushd $git_root/dask-gateway-server
GOOS=linux GOARCH=amd64 python setup.py build_go
popd

echo "Installing..."
kubectl exec dask-gateway-tests -n dask-gateway /working/continuous_integration/kubernetes/install-internal.sh

echo "Building dask-gateway source"
eval $(minikube docker-env)
pushd $git_root
docker build -t jcrist/dask-gateway -f continuous_integration/kubernetes/docker/dask-gateway/Dockerfile .
popd
