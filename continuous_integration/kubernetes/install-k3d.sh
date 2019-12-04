#!/usr/bin/env bash
set -e

KUBE_VERSION=1.16.0
K3D_VERSION=1.3.4

# Install kubectl
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v${KUBE_VERSION}/bin/linux/amd64/kubectl \
	&& chmod +x kubectl \
    && sudo mv kubectl /usr/local/bin/

# Install k3d
curl -Lo k3d https://github.com/rancher/k3d/releases/download/v${K3D_VERSION}/k3d-linux-amd64 \
    && chmod +x k3d \
    && sudo mv k3d /usr/local/bin/
