#!/usr/bin/env bash
set -xe

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../../.." && pwd)"

docker run --rm -d \
    --name slurm \
    -h slurm \
    -v "$git_root":/working \
    -p 8000:8000 \
    -p 8786:8786 \
    -p 8088:8088 \
    ghcr.io/meta-introspector/dask-gateway-ci-slurm

# The slurm container's systemd process emits logs about the progress of
# starting up declared services that we will await.
#
# We do it to avoid getting OOMKilled by peaking memory needs during startup,
# which is prone to happen if we run pip install at the same time.
#
# Practically, we await "entered RUNNING state" to be observed exactly 5 times,
# which represents our 5 systemd services.
#
#     INFO success: mysqld entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
#     INFO success: slurmdbd entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
#     INFO success: slurmd entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
#     INFO success: slurmctld entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
#     INFO success: munged entered RUNNING state, process has stayed up for > than 5 seconds (startsecs)
#
set +x
await_startup() {
    i=0; while [ $i -ne 30 ]; do
        docker logs slurm | grep "entered RUNNING state" | wc -l 2>/dev/null | grep --silent "5" \
            && start_script_finishing=true && break \
            || start_script_finishing=false && sleep 1 && i=$((i + 1)) && echo "Waiting for slurm container startup ($i seconds)"
    done
    if [ "$start_script_finishing" != "true" ]; then
        echo "WARNING: /script/start.sh was slow to finish!"
        exit 1
    fi

    echo "slurm container started!"
}
await_startup
