#!/usr/bin/env bash
set -e

KUBE_VERSION=1.16.0
HELM_VERSION=3.1.2
K3D_VERSION=1.6.0
STERN_VERSION=1.11.0

# Install kubectl
echo "Installing kubectl"
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v${KUBE_VERSION}/bin/linux/amd64/kubectl \
	&& chmod +x kubectl \
    && sudo mv kubectl /usr/local/bin/

# Install helm
echo "Installing helm"
curl -sSLo "helm.tar.gz" "https://get.helm.sh/helm-v${HELM_VERSION}-linux-amd64.tar.gz" \
    && tar -xf "helm.tar.gz" --strip-components 1 linux-amd64/helm \
    && rm "helm.tar.gz" \
    && sudo mv helm /usr/local/bin/

# Install k3d
echo "Installing k3d"
curl -Lo k3d https://github.com/rancher/k3d/releases/download/v${K3D_VERSION}/k3d-linux-amd64 \
    && chmod +x k3d \
    && sudo mv k3d /usr/local/bin/

# Install stern
echo "Installing stern"
curl -Lo stern https://github.com/wercker/stern/releases/download/${STERN_VERSION}/stern_linux_amd64 \
    && chmod +x stern \
    && sudo mv stern /usr/local/bin
