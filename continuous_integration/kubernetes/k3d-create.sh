#!/usr/bin/env bash
# This script can be used during local development to setup a k8s cluster.
#
# Note that if you are using k3d you must also "load" images so that your pods
# in the k3d cluster can access them. What docker images are available on your
# machine are different from those in the k3d sandbox.
# --------------------------------------------------------------------------
set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../.." && pwd)"

echo "Starting k3d"
k3d create \
    --publish 30200:30200 \
    --api-port 6444 \
    --name k3s-default

echo "Waiting for k3d access..."
for i in {1..10}; do
    export KUBECONFIG="$(k3d get-kubeconfig --name='k3s-default')"
    if [[ $KUBECONFIG != "" ]]; then
        break;
    fi
    sleep 1
done

echo "Waiting for k3d nodes..."
JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do
  sleep 0.5
done

echo "k3d is running!"

kubectl get nodes
