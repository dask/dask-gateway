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
    ghcr.io/dask/dask-gateway-ci-pbs

# The pbs container's entrypoint, files/scripts/start.sh, emits a log message
# that we will await observing. We do it to avoid getting OOMKilled by peaking
# memory needs during startup, which is prone to happen if we run pip install at
# the same time.
#
set +x
await_startup() {
    i=0; while [ $i -ne 30 ]; do
        docker logs pbs 2>/dev/null | grep --silent "Entering sleep" \
            && start_script_finishing=true && break \
            || start_script_finishing=false && sleep 1 && i=$((i + 1)) && echo "Waiting for pbs container startup ($i seconds)"
    done
    if [ "$start_script_finishing" != "true" ]; then
        echo "WARNING: /script/start.sh was slow to finish!"
        exit 1
    fi
    echo "pbs container started!"
}
await_startup
