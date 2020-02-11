#!/usr/bin/env bash
set -xe

ci_docker_hadoop="$(dirname "${BASH_SOURCE[0]}")"
full_path_ci_docker_hadoop="$(cd "${ci_docker_hadoop}" && pwd)"
git_root="$(cd "${full_path_ci_docker_hadoop}/../../.." && pwd)"

docker run --rm -d \
    --name master \
    -h master.example.com \
    -v "$git_root":/working \
    -p 8000:8000 \
    -p 8786:8786 \
    -p 8088:8088 \
    daskgateway/testing-hadoop
