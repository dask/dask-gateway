#!/usr/bin/env bash
set -xe

ci_docker_hadoop="$(dirname "${BASH_SOURCE[0]}")"
full_path_ci_docker_hadoop="$(cd "${ci_docker_hadoop}" && pwd)"
git_root="$(cd "${full_path_ci_docker_hadoop}/../../.." && pwd)"

docker run --rm -d \
    --name hadoop \
    -h master.example.com \
    -v "$git_root":/working \
    -p 8000:8000 \
    -p 8786:8786 \
    -p 8088:8088 \
    ghcr.io/meta-introspector/dask-gateway-ci-hadoop

# The hadoop container's systemd process emits logs about the progress of
# starting up declared services that we will await.
#
# We do it to avoid getting OOMKilled by peaking memory needs during startup,
# which is prone to happen if we run pip install at the same time.
#
# Practically, we await "entered RUNNING state" to be observed exactly 6 times,
# which represents our 6 systemd services.
#
#     INFO success: kadmind entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
#     INFO success: krb5kdc entered RUNNING state, process has stayed up for > than 1 seconds (startsecs)
#     INFO success: hdfs-namenode entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
#     INFO success: hdfs-datanode entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
#     INFO success: yarn-resourcemanager entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
#     INFO success: yarn-nodemanager entered RUNNING state, process has stayed up for > than 3 seconds (startsecs)
#
set +x
await_startup() {
    i=0; while [ $i -ne 30 ]; do
        docker logs hadoop | grep "entered RUNNING state" | wc -l 2>/dev/null | grep --silent "6" \
            && start_script_finishing=true && break \
            || start_script_finishing=false && sleep 1 && i=$((i + 1)) && echo "Waiting for hadoop container startup ($i seconds)"
    done
    if [ "$start_script_finishing" != "true" ]; then
        echo "WARNING: /script/start.sh was slow to finish!"
        exit 1
    fi

    echo "hadoop container started!"
}
await_startup
