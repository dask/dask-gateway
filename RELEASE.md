# Release process

Follow these steps to cut a new release.

- Update `docs/changelog.md` in a dedicated PR

  - Generate a list of PRs using [executablebooks/github-activity](https://github.com/executablebooks/github-activity)

    ```bash
    pip install --upgrade github-activity
    github-activity --output github-activity-output.md
    ```

  - Visit and label all uncategorized PRs appropriately with: `maintenance`, `enhancement`, `new`, `breaking`, `bug`, or `documentation`.
  - Generate a list of PRs again and add it to the changelog
  - Highlight breaking changes
  - Summarize the release changes

- Create a version bumping commit and push a git tag

  ```bash
  # VERSION should be for example 2022.4.0, not including a leading zero in the month!
  VERSION=...
  git fetch origin
  git checkout main
  git reset --hard origin/main

  # Update dask-gateway/dask_gateway/version.py
  # Update dask-gateway-server/dask_gateway_server/version.py
  # Update Chart.yaml
  git add .
  git commit -m "Release $VERSION"

  git tag -a $VERSION -m $VERSION
  git push --atomic --follow-tags origin main
  ```

- You can now verify that

  - The Python packages was published to PyPI: https://pypi.org/project/dask-gateway and https://pypi.org/project/dask-gateway-server
  - The Helm chart was pushed to our GitHub based Helm chart repo: https://github.com/dask/helm-chart/tree/gh-pages

- Finally await and review/merge the automated pull requests that will follow to the [conda-forge feedstock](https://github.com/conda-forge/dask-gateway-feedstock/pulls) repository.
