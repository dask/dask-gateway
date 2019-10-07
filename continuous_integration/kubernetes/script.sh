#!/usr/bin/env bash

echo "Running tests..."
kubectl exec dask-gateway-tests -n dask-gateway /working/continuous_integration/kubernetes/script-internal.sh

echo "Running chartpress"
chartpress --commit-range ${TRAVIS_COMMIT_RANGE} --tag `date +%y.%m.%d`
git diff

if [[ "$TRAVIS_BRANCH" == "master" && "$TRAVIS_EVENT_TYPE" == "push" ]]; then
    ./continuous_integration/kubernetes/deploy.sh
fi