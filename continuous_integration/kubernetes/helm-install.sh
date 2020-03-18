#!/usr/bin/env bash
set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
git_root="$(cd "${this_dir}/../.." && pwd)"

echo "Building daskgateway/dask-gateway"
fullname="$(docker build -q $git_root/dask-gateway)"
DASK_GATEWAY_TAG="${fullname#'sha256:'}"
export DASK_GATEWAY_IMAGE="daskgateway/dask-gateway:$DASK_GATEWAY_TAG"
docker tag $DASK_GATEWAY_TAG $DASK_GATEWAY_IMAGE

echo "Building daskgateway/dask-gateway-server"
fullname="$(docker build -q $git_root/dask-gateway-server)"
DASK_GATEWAY_SERVER_TAG="${fullname#'sha256:'}"
export DASK_GATEWAY_SERVER_IMAGE="daskgateway/dask-gateway-server:$DASK_GATEWAY_SERVER_TAG"
docker tag $DASK_GATEWAY_SERVER_TAG $DASK_GATEWAY_SERVER_IMAGE

echo "Importing images into k3d"
k3d import-images $DASK_GATEWAY_IMAGE $DASK_GATEWAY_SERVER_IMAGE

echo "Installing Helm Chart"
helm install \
    test-dask-gateway \
    "${git_root}/resources/helm/dask-gateway" \
    -f "${git_root}/resources/helm/testing/travis.yaml" \
    --set "gateway.image.tag=$DASK_GATEWAY_SERVER_TAG" \
    --set "gateway.backend.image.tag=$DASK_GATEWAY_TAG" \
    --set "controller.image.tag=$DASK_GATEWAY_SERVER_TAG" \
    --wait \
    --timeout 3m0s
