#!/usr/bin/env bash
set -xe

demo_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_demo_dir="$(cd "${demo_dir}" && pwd)"
git_root="$(cd "${full_path_demo_dir}/.." && pwd)"

docker run -d --rm \
    --name master \
    -h master.example.com \
    -v "$git_root":/working \
    -p 8787:8787 \
    -p 8786:8786 \
    -p 8088:8088 \
    daskgateway/demo-hadoop
