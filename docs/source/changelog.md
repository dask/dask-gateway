# Changelog

## 2022.11.0

([full changelog](https://github.com/dask/dask-gateway/compare/2022.10.0...2c2bed23b83831ad073fdbae80647e96d2111d22))

### Breaking changes

This breaking change only impacts Helm chart installations. The Bundled CRDs for
Traefik has been updated. To upgrade to 2022.11.0, also upgrade the registered
CRDs like below.

```shell
kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/dask/dask-gateway/2022.11.0/resources/helm/dask-gateway/crds/daskclusters.yaml
kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/dask/dask-gateway/2022.11.0/resources/helm/dask-gateway/crds/traefik.yaml
```

### Bugs fixed

- Fix invalid wheel name to PEP 600 [#635](https://github.com/dask/dask-gateway/pull/635) ([@consideRatio](https://github.com/consideRatio))
- Fix failure to build and publish arm64 images [#634](https://github.com/dask/dask-gateway/pull/634) ([@consideRatio](https://github.com/consideRatio))

### Maintenance and upkeep improvements

- helm chart: update traefik to 2.9.4 (associated CRDs unchanged) [#636](https://github.com/dask/dask-gateway/pull/636) ([@consideRatio](https://github.com/consideRatio))
- Extend integration tests to multi-namespace deployment [#627](https://github.com/dask/dask-gateway/pull/627) ([@holzman](https://github.com/holzman))
- helm chart: update traefik to 2.9.1 and the associated CRDs [#621](https://github.com/dask/dask-gateway/pull/621) ([@consideRatio](https://github.com/consideRatio))

### Continuous integration improvements

- ci: use ubuntu-22.04 explicitly and constrain test duration [#644](https://github.com/dask/dask-gateway/pull/644) ([@consideRatio](https://github.com/consideRatio))
- ci: test against golang 1.19, latest slurm, latest hadoop [#637](https://github.com/dask/dask-gateway/pull/637) ([@consideRatio](https://github.com/consideRatio))
- ci: test against latest versions of k8s [#620](https://github.com/dask/dask-gateway/pull/620) ([@consideRatio](https://github.com/consideRatio))

### Other merged PRs

- Refreeze dask-gateway/Dockerfile.requirements.txt [#644](https://github.com/dask/dask-gateway/pull/644) ([@dask-bot](https://github.com/dask-bot))
- Refreeze dask-gateway/Dockerfile.requirements.txt [#640](https://github.com/dask/dask-gateway/pull/640) ([@dask-bot](https://github.com/dask-bot))
- Refreeze dask-gateway-server/Dockerfile.requirements.txt [#639](https://github.com/dask/dask-gateway/pull/639) ([@dask-bot](https://github.com/dask-bot))
- Refreeze dask-gateway-server/Dockerfile.requirements.txt [#631](https://github.com/dask/dask-gateway/pull/631) ([@dask-bot](https://github.com/dask-bot))
- Refreeze dask-gateway/Dockerfile.requirements.txt [#630](https://github.com/dask/dask-gateway/pull/630) ([@dask-bot](https://github.com/dask-bot))
- [pre-commit.ci] pre-commit autoupdate [#628](https://github.com/dask/dask-gateway/pull/628) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- [pre-commit.ci] pre-commit autoupdate [#625](https://github.com/dask/dask-gateway/pull/625) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- build(deps): bump JamesIves/github-pages-deploy-action from 4.4.0 to 4.4.1 [#623](https://github.com/dask/dask-gateway/pull/623) ([@dependabot](https://github.com/dependabot))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/dask/dask-gateway/graphs/contributors?from=2022-10-13&to=2022-11-09&type=c))

[@consideRatio](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AconsideRatio+updated%3A2022-10-13..2022-11-09&type=Issues) | [@holzman](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aholzman+updated%3A2022-10-13..2022-11-09&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amartindurant+updated%3A2022-10-13..2022-11-09&type=Issues)

## 2022.10.0

This release includes no breaking changes.

([full changelog](https://github.com/dask/dask-gateway/compare/2022.6.1...2022.10.0))

### New features added

- Simplified integration with namespace local JupyterHub Helm charts [#612](https://github.com/dask/dask-gateway/pull/612) ([@consideRatio](https://github.com/consideRatio))
- Helm chart: add gateway.backend.imagePullSecrets [#606](https://github.com/dask/dask-gateway/pull/606) ([@maxime1907](https://github.com/maxime1907), [@consideRatio](https://github.com/consideRatio))

### Bugs fixed

- Fix typo in SLURM backend. [#603](https://github.com/dask/dask-gateway/pull/603) ([@amanning9](https://github.com/amanning9), [@jcrist](https://github.com/jcrist))
- Add public address to GatewayCluster when connecting to an existing cluster [#601](https://github.com/dask/dask-gateway/pull/601) ([@giffels](https://github.com/giffels), [@consideRatio](https://github.com/consideRatio))
- fix: add missing fields nameOverride and fullnameOverride [#593](https://github.com/dask/dask-gateway/pull/593) ([@maxime1907](https://github.com/maxime1907), [@consideRatio](https://github.com/consideRatio))
- Await `close_rpc()` in client [#588](https://github.com/dask/dask-gateway/pull/588) ([@patrix58](https://github.com/patrix58), [@consideRatio](https://github.com/consideRatio))

### Maintenance and upkeep improvements

- Refreeze dask-gateway-server/Dockerfile.requirements.txt [#617](https://github.com/dask/dask-gateway/pull/617) ([@dask-bot](https://github.com/dask-bot))
- Refreeze dask-gateway/Dockerfile.requirements.txt [#616](https://github.com/dask/dask-gateway/pull/616) ([@dask-bot](https://github.com/dask-bot))
- refactor: use traefik documented syntax for cli flags [#611](https://github.com/dask/dask-gateway/pull/611) ([@consideRatio](https://github.com/consideRatio), [@martindurant](https://github.com/martindurant))
- Compatibility fix for ipywidgets 8+ [#609](https://github.com/dask/dask-gateway/pull/609) ([@viniciusdc](https://github.com/viniciusdc), [@consideRatio](https://github.com/consideRatio))

### Documentation improvements

- Reorder gateway config and update comment about gateway.nodeSelector [#590](https://github.com/dask/dask-gateway/pull/590) ([@GeorgianaElena](https://github.com/GeorgianaElena), [@consideRatio](https://github.com/consideRatio))

### Continuous integration improvements

- build(deps): bump pypa/gh-action-pypi-publish from 1.5.0 to 1.5.1 [#602](https://github.com/dask/dask-gateway/pull/602) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- build(deps): bump docker/setup-qemu-action from 1 to 2 [#600](https://github.com/dask/dask-gateway/pull/600) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- build(deps): bump actions/setup-python from 3 to 4 [#599](https://github.com/dask/dask-gateway/pull/599) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- build(deps): bump JamesIves/github-pages-deploy-action from 4.2.5 to 4.4.0 [#598](https://github.com/dask/dask-gateway/pull/598) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- build(deps): bump jupyterhub/action-k3s-helm from 2 to 3 [#597](https://github.com/dask/dask-gateway/pull/597) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- build(deps): bump docker/setup-buildx-action from 1 to 2 [#596](https://github.com/dask/dask-gateway/pull/596) ([@dependabot](https://github.com/dependabot), [@consideRatio](https://github.com/consideRatio))
- ci: fix broken dependabot config [#595](https://github.com/dask/dask-gateway/pull/595) ([@consideRatio](https://github.com/consideRatio))


### Contributors to this release

([GitHub contributors page for this release](https://github.com/dask/dask-gateway/graphs/contributors?from=2022-06-13&to=2022-10-13&type=c))

[@amanning9](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aamanning9+updated%3A2022-06-13..2022-10-13&type=Issues) | [@consideRatio](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AconsideRatio+updated%3A2022-06-13..2022-10-13&type=Issues) | [@dependabot](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adependabot+updated%3A2022-06-13..2022-10-13&type=Issues) | [@GeorgianaElena](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AGeorgianaElena+updated%3A2022-06-13..2022-10-13&type=Issues) | [@giffels](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Agiffels+updated%3A2022-06-13..2022-10-13&type=Issues) | [@jcrist](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajcrist+updated%3A2022-06-13..2022-10-13&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amartindurant+updated%3A2022-06-13..2022-10-13&type=Issues) | [@maxime1907](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amaxime1907+updated%3A2022-06-13..2022-10-13&type=Issues) | [@patrix58](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Apatrix58+updated%3A2022-06-13..2022-10-13&type=Issues) | [@pre-commit-ci](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Apre-commit-ci+updated%3A2022-06-13..2022-10-13&type=Issues) | [@viniciusdc](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aviniciusdc+updated%3A2022-06-13..2022-10-13&type=Issues)
## 2022.6.1

### Bugs fixed

- Update dask-gateway package's requirements to what works [#580](https://github.com/dask/dask-gateway/pull/580) ([@consideRatio](https://github.com/consideRatio))

### Continuous integration improvements

- ci: avoid 429 too-many-requests issues from linkcheck [#578](https://github.com/dask/dask-gateway/pull/578) ([@consideRatio](https://github.com/consideRatio))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/dask/dask-gateway/graphs/contributors?from=2022-06-13&to=2022-06-13&type=c))

[@consideRatio](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AconsideRatio+updated%3A2022-06-13..2022-06-13&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amartindurant+updated%3A2022-06-13..2022-06-13&type=Issues)

## 2022.6.0

This release makes `dask-gateway` the client require `dask>=2022`,
`distributed>=2022`, and `click>=8.1.3`, but includes no other breaking changes.

### New features added

- Provide frozen requirements.txt files for images, and automation to update them [#575](https://github.com/dask/dask-gateway/pull/575) ([@consideRatio](https://github.com/consideRatio))

### Bugs fixed

- Fix compatibility with distributed >= 2022.5.1 and traitlets >= 5.2.0, and raise the lower bound of required versions [#573](https://github.com/dask/dask-gateway/pull/573) ([@consideRatio](https://github.com/consideRatio))
- Let Traefik's route traffic across namespaces via IngressRoute resources [#569](https://github.com/dask/dask-gateway/pull/569) ([@olivier-lacroix](https://github.com/olivier-lacroix))

### Maintenance and upkeep improvements

- Install bokeh and numpy in the Helm chart's scheduler and worker sample image [#561](https://github.com/dask/dask-gateway/pull/561) ([@zonca](https://github.com/zonca))
- golang: refresh dask-gateway-proxy using modules and package directories [#559](https://github.com/dask/dask-gateway/pull/559) ([@rigzba21](https://github.com/rigzba21))
- maint: unpin click again as issues seems resolved [#558](https://github.com/dask/dask-gateway/pull/558) ([@consideRatio](https://github.com/consideRatio))

### Documentation improvements

- Update Dask logo [#572](https://github.com/dask/dask-gateway/pull/572) ([@jacobtomlinson](https://github.com/jacobtomlinson))
- Update docs theme for rebranding [#567](https://github.com/dask/dask-gateway/pull/567) ([@scharlottej13](https://github.com/scharlottej13))
- Document setting `display: False` in hub services config [#564](https://github.com/dask/dask-gateway/pull/564) ([@yuvipanda](https://github.com/yuvipanda))
- docs: remove outdated comment about dev-environment.yaml [#557](https://github.com/dask/dask-gateway/pull/557) ([@consideRatio](https://github.com/consideRatio))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/dask/dask-gateway/graphs/contributors?from=2022-04-21&to=2022-06-13&type=c))

[@consideRatio](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AconsideRatio+updated%3A2022-04-21..2022-06-13&type=Issues) | [@jacobtomlinson](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajacobtomlinson+updated%3A2022-04-21..2022-06-13&type=Issues) | [@jcrist](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajcrist+updated%3A2022-04-21..2022-06-13&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amartindurant+updated%3A2022-04-21..2022-06-13&type=Issues) | [@menendes](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amenendes+updated%3A2022-04-21..2022-06-13&type=Issues) | [@olivier-lacroix](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aolivier-lacroix+updated%3A2022-04-21..2022-06-13&type=Issues) | [@rigzba21](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Arigzba21+updated%3A2022-04-21..2022-06-13&type=Issues) | [@scharlottej13](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ascharlottej13+updated%3A2022-04-21..2022-06-13&type=Issues) | [@TomAugspurger](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3ATomAugspurger+updated%3A2022-04-21..2022-06-13&type=Issues) | [@yuvipanda](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ayuvipanda+updated%3A2022-04-21..2022-06-13&type=Issues) | [@zonca](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Azonca+updated%3A2022-04-21..2022-06-13&type=Issues)

## 2022.4.0

This release is the first in a long time, and with it comes significant
improvements in documentation and automation to make it easier to cut releases
going onwards.

The project now adopts [CalVer](https://calver.org/) versioning with a
`YYYY.MM.MICRO` format similar to other Dask organization projects using the
slightly different `YYYY.0M.MICRO` format with a leading zero on the month.

### Breaking changes

- `dask-gateway` and `dask-gateway-server` now requires Python 3.8+
- Breaking changes to the `dask-gateway` Helm chart:
  - _ACTION REQUIRED_: When upgrading to this version you must also update the Helm chart's bundled CRD resources like this:
    ```shell
    kubectl apply -f https://raw.githubusercontent.com/dask/dask-gateway/2022.4.0/resources/helm/dask-gateway/crds/daskclusters.yaml
    kubectl apply -f https://raw.githubusercontent.com/dask/dask-gateway/2022.4.0/resources/helm/dask-gateway/crds/traefik.yaml
    ```
  - Now published to the Helm chart repository https://helm.dask.org.
  - Now require k8s 1.20+ and `helm` 3.5+.
  - Now bundles with a [`values.schema.json`
    file](https://helm.sh/docs/topics/charts/#schema-files) that won't tolerate
    most unrecognized configuration to help users avoid typos in their configs.
  - Now pushes the Helm chart's images to `ghcr.io/dask/dask-gateway-server` and
  `ghcr.io/dask/dask-gateway`.
  - Now declares the purpose of `ghcr.io/dask/dask-gateway` to be a truly
    minimal image for Helm chart testing purposes and encourages users to
    maintain their own image for worker and scheduler pods. See [the related
    documentation](https://gateway.dask.org/install-kube.html#using-a-custom-image)
    on using your own image.

### New features added

- ci, maint: build/publish to PyPI, build linux/mac and amd64/arm64 wheels [#538](https://github.com/dask/dask-gateway/pull/538) ([@consideRatio](https://github.com/consideRatio))
- Helm chart: add values.schema.yaml and associated maint. scripts [#429](https://github.com/dask/dask-gateway/pull/429) ([@consideRatio](https://github.com/consideRatio))
- Add customizable worker_threads [#353](https://github.com/dask/dask-gateway/pull/353) ([@AndreaGiardini](https://github.com/AndreaGiardini))

### Enhancements made

- Helm chart: add imagePullSecrets for traefik [#445](https://github.com/dask/dask-gateway/pull/445) ([@consideRatio](https://github.com/consideRatio))

### Bugs fixed

- Fix failure to start api/controller pods with tini as an entrypoint [#540](https://github.com/dask/dask-gateway/pull/540) ([@consideRatio](https://github.com/consideRatio))
- Fixed worker_threads config [#463](https://github.com/dask/dask-gateway/pull/463) ([@TomAugspurger](https://github.com/TomAugspurger))
- Avoid warning in Gateway.__del__ [#442](https://github.com/dask/dask-gateway/pull/442) ([@TomAugspurger](https://github.com/TomAugspurger))
- Fix for authenticating with JupyterHub service [#410](https://github.com/dask/dask-gateway/pull/410) ([@aktech](https://github.com/aktech))
- helm chart: Add label to be allowed direct network access to the jupyterhub pod [#352](https://github.com/dask/dask-gateway/pull/352) ([@consideRatio](https://github.com/consideRatio))

### Maintenance and upkeep improvements

- Update traefik's CRDs [#554](https://github.com/dask/dask-gateway/pull/554) ([@consideRatio](https://github.com/consideRatio))
- Update dev-environment.yaml and remove test_helm.py [#550](https://github.com/dask/dask-gateway/pull/550) ([@consideRatio](https://github.com/consideRatio))
- maint, docs: add myst-parser for occational markdown files and sphinx_copybutton [#548](https://github.com/dask/dask-gateway/pull/548) ([@consideRatio](https://github.com/consideRatio))
- Disable arm64 for dask/dask-gateway image [#545](https://github.com/dask/dask-gateway/pull/545) ([@consideRatio](https://github.com/consideRatio))
- Reference distributed's actual TimeoutError used [#534](https://github.com/dask/dask-gateway/pull/534) ([@consideRatio](https://github.com/consideRatio))
- Helm chart images: conda removed -> pip only, usage disclaimer added, minimized Dockerfile complexity [#533](https://github.com/dask/dask-gateway/pull/533) ([@consideRatio](https://github.com/consideRatio))
- pre-commit: start using isort [#532](https://github.com/dask/dask-gateway/pull/532) ([@consideRatio](https://github.com/consideRatio))
- Drop support for Python 3.7 [#531](https://github.com/dask/dask-gateway/pull/531) ([@consideRatio](https://github.com/consideRatio))
- maint: avoid regression/breaking change in `click` and declare our dependency to the library explicitly [#525](https://github.com/dask/dask-gateway/pull/525) ([@consideRatio](https://github.com/consideRatio))
- Cleanup no longer needed workarounds for Python 3.6 [#510](https://github.com/dask/dask-gateway/pull/510) ([@consideRatio](https://github.com/consideRatio))
- ci, pre-commit: add whitespace fixing autoformatters [#507](https://github.com/dask/dask-gateway/pull/507) ([@consideRatio](https://github.com/consideRatio))
- maint/ci: remove support for Python 3.6 and test against multiple versions of Python and Golang [#501](https://github.com/dask/dask-gateway/pull/501) ([@consideRatio](https://github.com/consideRatio))
- Add boilerplate .gitignore from GitHub [#499](https://github.com/dask/dask-gateway/pull/499) ([@consideRatio](https://github.com/consideRatio))
- Adding conda environment file for development dependencies [#488](https://github.com/dask/dask-gateway/pull/488) ([@rigzba21](https://github.com/rigzba21))
- Helm chart: update traefik CRDs and Traefik version from 2.5 to 2.6 [#479](https://github.com/dask/dask-gateway/pull/479) ([@consideRatio](https://github.com/consideRatio))
- Bump dask and distributed to 2022.02.0 [#474](https://github.com/dask/dask-gateway/pull/474) ([@consideRatio](https://github.com/consideRatio))
- Bump base images [#468](https://github.com/dask/dask-gateway/pull/468) ([@jcrist](https://github.com/jcrist))
- Update docker images [#464](https://github.com/dask/dask-gateway/pull/464) ([@TomAugspurger](https://github.com/TomAugspurger))
- Use new Dask docs theme [#448](https://github.com/dask/dask-gateway/pull/448) ([@jacobtomlinson](https://github.com/jacobtomlinson))
- Fix/update pre commit config [#443](https://github.com/dask/dask-gateway/pull/443) ([@TomAugspurger](https://github.com/TomAugspurger))
- Register a kubernetes pytest mark [#441](https://github.com/dask/dask-gateway/pull/441) ([@TomAugspurger](https://github.com/TomAugspurger))
- Helm chart: update to traefik v2.5.x [#431](https://github.com/dask/dask-gateway/pull/431) ([@consideRatio](https://github.com/consideRatio))
- helm chart: refactor to use consistent modern syntax [#425](https://github.com/dask/dask-gateway/pull/425) ([@consideRatio](https://github.com/consideRatio))
- helm chart: don't package a README.rst file [#424](https://github.com/dask/dask-gateway/pull/424) ([@consideRatio](https://github.com/consideRatio))
- images: some refactoring and version bumps for arm64 compatible Dockerfiles [#423](https://github.com/dask/dask-gateway/pull/423) ([@consideRatio](https://github.com/consideRatio))
- Helm chart: update deprecated k8s resources no longer supported in k8s 1.22 [#420](https://github.com/dask/dask-gateway/pull/420) ([@consideRatio](https://github.com/consideRatio))
- Use format_bytes from dask instead of distributed [#416](https://github.com/dask/dask-gateway/pull/416) ([@TomAugspurger](https://github.com/TomAugspurger))
- Fix travis main tests [#411](https://github.com/dask/dask-gateway/pull/411) ([@aktech](https://github.com/aktech))
- Change default branch from master to main [#372](https://github.com/dask/dask-gateway/pull/372) ([@jsignell](https://github.com/jsignell))

### Documentation improvements

- docs: revert adding copybutton, doesn't work well with dask theme [#556](https://github.com/dask/dask-gateway/pull/556) ([@consideRatio](https://github.com/consideRatio))
- Add RELEASE.md [#549](https://github.com/dask/dask-gateway/pull/549) ([@consideRatio](https://github.com/consideRatio))
- Document skaffold.yaml and update image references [#513](https://github.com/dask/dask-gateway/pull/513) ([@consideRatio](https://github.com/consideRatio))
- Remove legacy purge flag from k8s uninstall docs [#502](https://github.com/dask/dask-gateway/pull/502) ([@brews](https://github.com/brews))
- ci/docs: updates related to building and testing documentation [#500](https://github.com/dask/dask-gateway/pull/500) ([@consideRatio](https://github.com/consideRatio))
- Added release notes [#467](https://github.com/dask/dask-gateway/pull/467) ([@TomAugspurger](https://github.com/TomAugspurger))
- point readme test & docs badges to destinations [#460](https://github.com/dask/dask-gateway/pull/460) ([@delgadom](https://github.com/delgadom))
- adding kubernetes networking notes from #360 [#454](https://github.com/dask/dask-gateway/pull/454) ([@rigzba21](https://github.com/rigzba21))
- DOC: rm extra https [#447](https://github.com/dask/dask-gateway/pull/447) ([@raybellwaves](https://github.com/raybellwaves))
- Helm chart: add note about dummy schema [#444](https://github.com/dask/dask-gateway/pull/444) ([@consideRatio](https://github.com/consideRatio))
- Update references from old to new Helm chart registry [#438](https://github.com/dask/dask-gateway/pull/438) ([@consideRatio](https://github.com/consideRatio))
- Cleanup outdated travis references [#426](https://github.com/dask/dask-gateway/pull/426) ([@consideRatio](https://github.com/consideRatio))
- Add GitHub Actions badges in README.md [#415](https://github.com/dask/dask-gateway/pull/415) ([@aktech](https://github.com/aktech))
- Add warning about Go version to server installation instructions. [#399](https://github.com/dask/dask-gateway/pull/399) ([@douglasdavis](https://github.com/douglasdavis))
- added a missing comma in the profile options code chunk example [#396](https://github.com/dask/dask-gateway/pull/396) ([@cdibble](https://github.com/cdibble))
- Fixes broken link to z2jh helm setup instructions [#374](https://github.com/dask/dask-gateway/pull/374) ([@arokem](https://github.com/arokem))

### Other merged PRs

- ci: align with PEP600 about wheel platform names [#555](https://github.com/dask/dask-gateway/pull/555) ([@consideRatio](https://github.com/consideRatio))
- ci: refactor three job definitions into one run three times [#552](https://github.com/dask/dask-gateway/pull/552) ([@consideRatio](https://github.com/consideRatio))
- ci: add helm chart upgrade test [#551](https://github.com/dask/dask-gateway/pull/551) ([@consideRatio](https://github.com/consideRatio))
- ci: avoid running tests in fork's PR branches [#541](https://github.com/dask/dask-gateway/pull/541) ([@consideRatio](https://github.com/consideRatio))
- ci: fix pbs image and tests, reduce threads and delay pip install to avoid memory peak causing process termination [#536](https://github.com/dask/dask-gateway/pull/536) ([@consideRatio](https://github.com/consideRatio))
- ci: fix intermittent errors by sleeping a bit before running tests [#530](https://github.com/dask/dask-gateway/pull/530) ([@consideRatio](https://github.com/consideRatio))
- [pre-commit.ci] pre-commit autoupdate [#527](https://github.com/dask/dask-gateway/pull/527) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- [pre-commit.ci] pre-commit autoupdate [#521](https://github.com/dask/dask-gateway/pull/521) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- ci: de-duplicate deps by docs/requirements.txt and tests/requirements.txt and update CI images [#519](https://github.com/dask/dask-gateway/pull/519) ([@consideRatio](https://github.com/consideRatio))
- ci: use k3s instead of k3d to setup k8s, and test against k8s 1.20-1.23 [#518](https://github.com/dask/dask-gateway/pull/518) ([@consideRatio](https://github.com/consideRatio))
- ci: add fixme notes, update python/go versions, make a script executable like others [#517](https://github.com/dask/dask-gateway/pull/517) ([@consideRatio](https://github.com/consideRatio))
- ci: use chartpress to build/test/publish images and Helm chart [#514](https://github.com/dask/dask-gateway/pull/514) ([@consideRatio](https://github.com/consideRatio))
- ci: add timeout to avoid 6h consequence of intermittent hang issue [#512](https://github.com/dask/dask-gateway/pull/512) ([@consideRatio](https://github.com/consideRatio))
- ci, pre-commit: add python style modernizing autoformatter [#508](https://github.com/dask/dask-gateway/pull/508) ([@consideRatio](https://github.com/consideRatio))
- ci: run go native tests against modern versions of go [#505](https://github.com/dask/dask-gateway/pull/505) ([@consideRatio](https://github.com/consideRatio))
- ci: update black config for python 3.7-3.10 [#503](https://github.com/dask/dask-gateway/pull/503) ([@consideRatio](https://github.com/consideRatio))
- ci: let flake8 be configured in a single place instead of three [#497](https://github.com/dask/dask-gateway/pull/497) ([@consideRatio](https://github.com/consideRatio))
- ci: fix docs workflow triggers, update misc action versions, unpin some dependencies [#495](https://github.com/dask/dask-gateway/pull/495) ([@consideRatio](https://github.com/consideRatio))
- ci: build/push python packages workflow, added [#494](https://github.com/dask/dask-gateway/pull/494) ([@consideRatio](https://github.com/consideRatio))
- ci: build/push images workflow, added [#493](https://github.com/dask/dask-gateway/pull/493) ([@consideRatio](https://github.com/consideRatio))
- ci: misc updates to test workflow and pytest-asyncio [#492](https://github.com/dask/dask-gateway/pull/492) ([@consideRatio](https://github.com/consideRatio))
- ci: delete no longer used script before_install.sh (travis legacy) [#491](https://github.com/dask/dask-gateway/pull/491) ([@consideRatio](https://github.com/consideRatio))
- ci: add dependabot config [#490](https://github.com/dask/dask-gateway/pull/490) ([@consideRatio](https://github.com/consideRatio))
- ci: remove no longer used (?) github repo deploy key (to push to gh-pages branch?) [#485](https://github.com/dask/dask-gateway/pull/485) ([@consideRatio](https://github.com/consideRatio))
- [pre-commit.ci] pre-commit autoupdate [#481](https://github.com/dask/dask-gateway/pull/481) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- ci: fix ci failure, optimize workflow triggers, document use of pre-commit.ci [#477](https://github.com/dask/dask-gateway/pull/477) ([@consideRatio](https://github.com/consideRatio))
- ci: add --color=yes to pytest as needed in github actions [#430](https://github.com/dask/dask-gateway/pull/430) ([@consideRatio](https://github.com/consideRatio))
- ci: fix Kubernetes CI Tests [#413](https://github.com/dask/dask-gateway/pull/413) ([@aktech](https://github.com/aktech))
- ci: move CI to GitHub Actions [#408](https://github.com/dask/dask-gateway/pull/408) ([@aktech](https://github.com/aktech))
- ci: test GitHub Actions for auto-release [#339](https://github.com/dask/dask-gateway/pull/339) ([@fanshi118](https://github.com/fanshi118))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/dask/dask-gateway/graphs/contributors?from=2020-11-04&to=2022-04-21&type=c))

[@aktech](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aaktech+updated%3A2020-11-04..2022-04-20&type=Issues) | [@AndreaGiardini](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AAndreaGiardini+updated%3A2020-11-04..2022-04-20&type=Issues) | [@aravindrp](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aaravindrp+updated%3A2020-11-04..2022-04-20&type=Issues) | [@arokem](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aarokem+updated%3A2020-11-04..2022-04-20&type=Issues) | [@bolliger32](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Abolliger32+updated%3A2020-11-04..2022-04-20&type=Issues) | [@brews](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Abrews+updated%3A2020-11-04..2022-04-20&type=Issues) | [@cdibble](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Acdibble+updated%3A2020-11-04..2022-04-20&type=Issues) | [@choldgraf](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Acholdgraf+updated%3A2020-11-04..2022-04-20&type=Issues) | [@consideRatio](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AconsideRatio+updated%3A2020-11-04..2022-04-20&type=Issues) | [@cslovell](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Acslovell+updated%3A2020-11-04..2022-04-20&type=Issues) | [@delgadom](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adelgadom+updated%3A2020-11-04..2022-04-20&type=Issues) | [@dgerlanc](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adgerlanc+updated%3A2020-11-04..2022-04-20&type=Issues) | [@dhirschfeld](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adhirschfeld+updated%3A2020-11-04..2022-04-20&type=Issues) | [@douglasdavis](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adouglasdavis+updated%3A2020-11-04..2022-04-20&type=Issues) | [@droctothorpe](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Adroctothorpe+updated%3A2020-11-04..2022-04-20&type=Issues) | [@erl987](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aerl987+updated%3A2020-11-04..2022-04-20&type=Issues) | [@fanshi118](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Afanshi118+updated%3A2020-11-04..2022-04-20&type=Issues) | [@Id2ndR](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AId2ndR+updated%3A2020-11-04..2022-04-20&type=Issues) | [@jacobtomlinson](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajacobtomlinson+updated%3A2020-11-04..2022-04-20&type=Issues) | [@JColl88](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3AJColl88+updated%3A2020-11-04..2022-04-20&type=Issues) | [@jcrist](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajcrist+updated%3A2020-11-04..2022-04-20&type=Issues) | [@jrbourbeau](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajrbourbeau+updated%3A2020-11-04..2022-04-20&type=Issues) | [@jsignell](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ajsignell+updated%3A2020-11-04..2022-04-20&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amartindurant+updated%3A2020-11-04..2022-04-20&type=Issues) | [@menendes](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amenendes+updated%3A2020-11-04..2022-04-20&type=Issues) | [@mmccarty](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ammccarty+updated%3A2020-11-04..2022-04-20&type=Issues) | [@mukhery](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Amukhery+updated%3A2020-11-04..2022-04-20&type=Issues) | [@pre-commit-ci](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Apre-commit-ci+updated%3A2020-11-04..2022-04-20&type=Issues) | [@quasiben](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Aquasiben+updated%3A2020-11-04..2022-04-20&type=Issues) | [@raybellwaves](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Araybellwaves+updated%3A2020-11-04..2022-04-20&type=Issues) | [@rigzba21](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Arigzba21+updated%3A2020-11-04..2022-04-20&type=Issues) | [@rileyhun](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Arileyhun+updated%3A2020-11-04..2022-04-20&type=Issues) | [@rsignell-usgs](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Arsignell-usgs+updated%3A2020-11-04..2022-04-20&type=Issues) | [@TomAugspurger](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3ATomAugspurger+updated%3A2020-11-04..2022-04-20&type=Issues) | [@wdhowe](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Awdhowe+updated%3A2020-11-04..2022-04-20&type=Issues) | [@yuvipanda](https://github.com/search?q=repo%3Adask%2Fdask-gateway+involves%3Ayuvipanda+updated%3A2020-11-04..2022-04-20&type=Issues)
