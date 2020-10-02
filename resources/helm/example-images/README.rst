Example Docker Image
====================

Here we include an example ``Dockerfile`` that users can use as a reference
when building scheduler/worker images to use with dask-gateway.

For a scheduler/worker image to be usable with dask-gateway, it needs:

- A compatible version of ``dask-gateway`` installed

We also recommend running with an init process. This isn't strictly required,
but you may run into weird bugs if not using an init process. We recommend
`tini <https://github.com/krallin/tini>`__, but other options should also work.
