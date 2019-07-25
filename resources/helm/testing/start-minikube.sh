#!/usr/bin/env bash
set -e

KUBE_VERSION=1.13.0

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../.." && pwd)"

echo "Starting minikube"

if [[ "$TRAVIS" == "true" ]]; then
    # Install minikube
    $this_dir/install-minikube.sh

    # Start with no VM driver on Travis CI
    sudo CHANGE_MINIKUBE_NONE_USER=true minikube start \
        --kubernetes-version=v${KUBE_VERSION} \
        --extra-config=apiserver.authorization-mode=RBAC \
        --vm-driver=none

    # Ensure the travis user owns the minikube configuration directory
    sudo chown -R travis: /home/travis/.minikube/
else
    # Mount the repo in minikube
    minikube start \
        --kubernetes-version=v${KUBE_VERSION} \
        --extra-config=apiserver.authorization-mode=RBAC
fi

minikube update-context

echo "Waiting for minikube..."

JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do
  sleep 0.5
done
echo "Minikube is running!"

kubectl get nodes

echo "Starting tiller"
kubectl --namespace kube-system create sa tiller
kubectl create clusterrolebinding tiller --clusterrole cluster-admin --serviceaccount=kube-system:tiller
helm init --service-account tiller

echo "Waiting for tiller..."
kubectl --namespace=kube-system rollout status --watch deployment/tiller-deploy
echo "Tiller is running!"
