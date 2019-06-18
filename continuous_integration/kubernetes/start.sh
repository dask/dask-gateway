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

    DASK_GATEWAY_SOURCE="$git_root"
else
    # Mount the repo in minikube
    minikube start \
        --kubernetes-version=v${KUBE_VERSION} \
        --extra-config=apiserver.authorization-mode=RBAC \
        --mount-string "$git_root:/dask-gateway-host" \
        --mount
    DASK_GATEWAY_SOURCE="/dask-gateway-host"
fi

minikube update-context

echo "Waiting for minikube..."

JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do
  sleep 0.5
done

kubectl get nodes

echo "Starting dask-gateway-tests pod"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: dask-gateway
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gateway
  namespace: dask-gateway
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gateway
  namespace: dask-gateway
rules:
  - apiGroups: [""]
    resources: ["pods", "secrets"]
    verbs: ["get", "watch", "list", "create", "delete"]
---
kind: RoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: gateway
  namespace: dask-gateway
subjects:
  - kind: ServiceAccount
    name: gateway
    namespace: dask-gateway
roleRef:
  kind: Role
  name: gateway
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: Pod
metadata:
  name: dask-gateway-tests
  namespace: dask-gateway
spec:
  serviceAccountName: gateway
  containers:
    - name: dask-gateway-tests
      image: jcrist/dask-gateway-base:latest
      args: 
        - sleep
        - infinity
      volumeMounts:
        - name: dask-gateway-source
          mountPath: /working
  volumes:
    - name: dask-gateway-source
      hostPath: 
        path: $DASK_GATEWAY_SOURCE
        type: Directory
EOF

echo "Waiting for testing pod to start"
kubectl wait -n dask-gateway --for=condition=Ready pod/dask-gateway-tests
