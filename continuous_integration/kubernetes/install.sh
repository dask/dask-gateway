#!/usr/bin/env bash

set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../.." && pwd)"

echo "Building Go source"
pushd $git_root/dask-gateway-server
GOOS=linux GOARCH=amd64 python setup.py build_go
popd

echo "Installing..."
if [[ "$TRAVIS" != "true" ]]; then
    pip_args="-e"
else
    pip_args=""
fi
kubectl exec dask-gateway-tests -n dask-gateway -- \
    /working/continuous_integration/kubernetes/install-internal.sh "$pip_args"

echo "Building dask-gateway source"
if [[ "$TRAVIS" != "true" ]]; then
    eval $(minikube docker-env)
fi
pushd $git_root
docker build -t daskgateway/dask-gateway -f continuous_integration/kubernetes/docker/dask-gateway/Dockerfile .
popd
