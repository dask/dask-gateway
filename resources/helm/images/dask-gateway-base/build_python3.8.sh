# Build docker image with arguments appropriate for python3.8
#
# $1 should be tag for image
tag=${1:=daskgateway/dask-gateway:0.7.1-py38}
docker build --build-arg base_image=python:3.8-slim-buster --build-arg conda_version=py38_4.8.3 --build-arg conda_sha256=879457af6a0bf5b34b48c12de31d4df0ee2f06a8e68768e5758c3293b2daf688  -t $tag .
