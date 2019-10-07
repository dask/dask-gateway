#!/usr/bin/env bash

echo "Deploying chartpress..."

set -eu
# TODO: add deploy-key.rsa.enc and set keys in travis UI
openssl aes-256-cbc -K $encrypted_058f1ad78f7f_key -iv $encrypted_058f1ad78f7f_iv -in deploy-key.rsa.enc -out deploy-key.rsa -d
set -x
chmod 0400 deploy-key.rsa
helm init --client-only
helm repo update
export GIT_SSH_COMMAND="ssh -i ${PWD}/deploy-key.rsa"
chartpress --commit-range ${TRAVIS_COMMIT_RANGE} --publish-chart $@
git diff
