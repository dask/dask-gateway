#!/usr/bin/env bash
set -xe

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../../.." && pwd)"

docker run --rm -d \
    --name pbs \
    -h pbs \
    -v "$git_root":/working \
    -p 8000:8000 \
    -p 8786:8786 \
    -p 8088:8088 \
    --cap-add=SYS_RESOURCE \
    daskgateway/testing-pbs
