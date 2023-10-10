docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-base ./base
docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-hadoop ./hadoop
docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-pbs ./pbs
docker build --no-cache -t ghcr.io/meta-introspector/dask-gateway-ci-slurm ./slurm
