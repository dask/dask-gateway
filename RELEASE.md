# How to make a release

`dask-gateway` and `dask-gateway-server` are packages available on [PyPI] and
[conda-forge], and `dask-gateway` is a Helm chart available at [helm.dask.org]
which is both a user facing website and a Helm chart repository by having an
[index.yaml] file read by `helm` the CLI linking to packaged Helm charts.

These are instructions on how to make a release.

## Pre-requisites

- Push rights to [dask/dask-gateway]
- Push rights to [conda-forge/dask-gateway-feedstock]

## Steps to make a release

1. Refreeze Dockerfile.requirements.txt files by running the [refreeze workflow]
   and merging created PRs.

   [refreeze workflow]: https://github.com/dask/dask-gateway/actions/workflows/refreeze-dockerfile-requirements-txt.yaml

1. Create a PR updating `docs/source/changelog.md` with [github-activity] and
   continue only when its merged.

   ```shell
   pip install github-activity

   github-activity --heading-level=2 dask/dask-gateway
   ```

  - Visit and label all uncategorized PRs appropriately with: `maintenance`,
    `enhancement`, `new`, `breaking`, `bug`, or `documentation`.
  - Generate a list of PRs again and add it to the changelog
  - Highlight breaking changes
  - Summarize the release changes

2. Checkout main and make sure it is up to date.

   ```shell
   git checkout main
   git fetch origin main
   git reset --hard origin/main
   git clean -xfd
   ```

3. Update the version, make commits, and push a git tag with `tbump`.

   ```shell
   pip install tbump
   tbump --dry-run ${VERSION}

   tbump ${VERSION}
   ```

   Following this, the [CI system] will build and publish the PyPI packages and
   Helm chart.

4. Following the release to PyPI, an automated PR should arrive to
   [conda-forge/jupyterhub-feedstock] with instructions.

[pypi]: https://pypi.org/project/dask-gateway/
[conda-forge]: https://anaconda.org/conda-forge/dask-gateway
[helm.dask.org]: https://helm.dask.org/
[index.yaml]: https://helm.dask.org/index.yaml
[dask/dask-gateway]: https://github.com/dask/dask-gateway
[conda-forge/dask-gateway-feedstock]: https://github.com/conda-forge/jupyterhub-feedstock
[github-activity]: https://github.com/executablebooks/github-activity
[ci system]: https://github.com/dask/dask-gateway/actions
