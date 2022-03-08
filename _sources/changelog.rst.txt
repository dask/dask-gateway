Changelog
=========

0.10.0
-------

Released on December 3rd, 2021

* Docker images: Update dependencies to the latest stable versions :pr:`464`
* Docker images: some refactoring and arm64 support :pr:`423`
* Fixed unintentional warning when cleaning up ``Gateway`` objects :pr:`442`
* Fixed warning from use of the deprecated ``distributed.format_bytes`` :pr:`416`
* Move CI to GitHub Actions :pr:`408`
* Add support for authenticating with JupyterHub service :pr:`410`
* Add warning about Go version to server installation instructions. :pr:`399`
* Change default branch from master to main :pr:`372`
* Add customizable worker_threads :pr:`353`
* Add label to be allowed direct access to the jupyterhub pod :pr:`352`
* Helm chart: update deprecated k8s resources no longer supported in k8s 1.22 :pr:`420`
* Helm chart: refactor to use consistent modern syntax :pr:`425`
* Helm chart: don't package a README.rst file :pr:`424`
* Helm chart: update to traefik v2.5.x :pr:`431`
* Helm chart: add imagePullSecrets for traefik :pr:`445`
* Update references from old to new Helm chart registry :pr:`438`
* Helm chart: add values.schema.yaml and associated maint. scripts :pr:`429`