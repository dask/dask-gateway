#!/usr/bin/env bash
set -xe

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../../.." && pwd)"

docker run --rm -d \
    --name slurm \
    -h slurm \
    -v "$git_root":/working \
    daskgateway/testing-slurm
