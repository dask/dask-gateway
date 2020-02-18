# Skaffold

[Skaffold](https://skaffold.dev) is a CLI developed by Google that streamlines
the local Kubernetes development cycle. We're going to use it to streamline
development of Dask Gateway Server.

Note, this workflow is compatible with release version 0.6.1. The current
gateway server build succeeds but the container fails because the Kubernetes
backend has not been implemented yet. Nevertheless, the Skaffold development
cycle can be leveraged as work on the backend proceeds.

## Installation

Install [Skaffold](https://skaffold.dev/docs/install/).
```bash
brew install skaffold
```

[Activate](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#activating-an-environment)
your conda environment.

Enable [Docker
Buildkit](https://docs.docker.com/develop/develop-images/build_enhancements/).
```bash
export DOCKER_BUILDKIT=1
```

## Configuration

Generate a randomized key for the `cookieSecret` and `proxyToken` in
[skaffold.yaml](skaffold.yaml).
```bash
sed -i "s|<key>|$(openssl rand -hex 32)|" skaffold.yaml
```

## Usage

Run Skaffold in [dev mode](https://skaffold.dev/docs/). This ensures that any
modifications to the source result in a rebuild and redeployment (via Helm) of
the source code. The `--no-prune=false` and `--cache-artifacts=false` automate
the image [clean-up](https://skaffold.dev/docs/pipeline-stages/cleanup/)
process.
```bash
skaffold dev --no-prune=false --cache-artifacts=false
```

You may want to create an alias for the above command.
```bash
# bash
echo 'alias skd="skaffold dev --no-prune=false --cache-artifacts=false"' >> ~/.bash_profile
# zsh
echo 'alias skd="skaffold dev --no-prune=false --cache-artifacts=false"' >> ~/.zshrc
```

Update one of the source files in `skaffold/dask-gateway/dask-gateway-server`
and rejoice.

The very first build will take about 90 seconds. After that, the Docker build
process won't need to re-run `conda install`, so subsequent builds should take
around 5.4s!

If you're using an internal artifact store, open [condarc](condarc) and follow
the instructions on the first two lines.
