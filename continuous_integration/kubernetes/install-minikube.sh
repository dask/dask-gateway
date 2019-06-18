#!/usr/bin/env bash
set -e

KUBE_VERSION=1.13.0
MINIKUBE_VERSION=0.35.0

# Install kubectl
curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v${KUBE_VERSION}/bin/linux/amd64/kubectl \
	&& chmod +x kubectl \
    && sudo mv kubectl /usr/local/bin/

# Install minikube
curl -Lo minikube https://storage.googleapis.com/minikube/releases/v${MINIKUBE_VERSION}/minikube-linux-amd64 \
    && chmod +x minikube \
    && sudo mv minikube /usr/local/bin/
