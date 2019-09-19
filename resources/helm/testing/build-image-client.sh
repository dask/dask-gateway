#!/usr/bin/env bash

set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../../.." && pwd)"

echo "Building scheduler/worker test image"
if [[ "$TRAVIS" != "true" ]]; then
    eval $(minikube docker-env)
fi

pushd $git_root
docker build -t dask-gateway-test -f resources/helm/testing/images/dask-gateway/Dockerfile .
popd
