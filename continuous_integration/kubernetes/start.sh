#!/usr/bin/env bash
set -e

this_dir="$(dirname "${BASH_SOURCE[0]}")"
full_path_this_dir="$(cd "${this_dir}" && pwd)"
git_root="$(cd "${full_path_this_dir}/../.." && pwd)"

if [[ "$TRAVIS" == "true" ]]; then
    echo "Installing kubectl and k3d"
    $this_dir/install-k3d.sh
fi

echo "Starting k3d"

k3d create \
    -v $git_root:/dask-gateway-host \
    --publish 30200:30200 \
    --publish 30201:30201 \
    --name k3s-default

for i in {1..10}; do
    export KUBECONFIG="$(k3d get-kubeconfig --name='k3s-default')"
    if [[ $KUBECONFIG != "" ]]; then
        break;
    fi
    sleep 1
done

# Fixup the kubeconfig file, since k3d doesn't do it properly
if [[ "$DOCKER_MACHINE_NAME" != "" ]]; then
    api_host=$(docker-machine ip $DOCKER_MACHINE_NAME)
    echo "Patching kubeconfig file"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i "" "s/127.0.0.1/$api_host/g" $KUBECONFIG
    else
        sed -i "s/127.0.0.1/$api_host/g" $KUBECONFIG
    fi
fi

echo "Waiting for k3d..."

JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do
  sleep 0.5
done
echo "k3d is running!"

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
      image: daskgateway/dask-gateway-base:latest
      args:
        - sleep
        - infinity
      volumeMounts:
        - name: dask-gateway-source
          mountPath: /working
  volumes:
    - name: dask-gateway-source
      hostPath:
        path: /dask-gateway-host
        type: Directory
EOF

echo "Waiting for testing pod to start"
kubectl wait -n dask-gateway --for=condition=Ready pod/dask-gateway-tests --timeout=90s
