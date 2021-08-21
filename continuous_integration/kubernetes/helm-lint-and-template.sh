#!/usr/bin/env bash
set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
git_root="$(cd "${this_dir}/../.." && pwd)"

echo "Linting Helm Chart"
helm lint \
    "${git_root}/resources/helm/dask-gateway" \
    -f "${git_root}/resources/helm/testing/travis.yaml"

echo "Rendering Helm Chart Templates"
helm template dask-gateway \
    --include-crds \
    "${git_root}/resources/helm/dask-gateway" \
    -f "${git_root}/resources/helm/testing/travis.yaml"
