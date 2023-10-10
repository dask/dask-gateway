# YYYY-MM-DD format
date=$(date '+%Y-%m-%d')
docker tag ghcr.io/meta-introspector/dask-gateway-ci-base:latest   ghcr.io/meta-introspector/dask-gateway-ci-base:$date
docker tag ghcr.io/meta-introspector/dask-gateway-ci-hadoop:latest ghcr.io/meta-introspector/dask-gateway-ci-hadoop:$date
docker tag ghcr.io/meta-introspector/dask-gateway-ci-pbs:latest    ghcr.io/meta-introspector/dask-gateway-ci-pbs:$date
docker tag ghcr.io/meta-introspector/dask-gateway-ci-slurm:latest  ghcr.io/meta-introspector/dask-gateway-ci-slurm:$date
docker push ghcr.io/meta-introspector/dask-gateway-ci-base:$date
docker push ghcr.io/meta-introspector/dask-gateway-ci-hadoop:$date
docker push ghcr.io/meta-introspector/dask-gateway-ci-pbs:$date
docker push ghcr.io/meta-introspector/dask-gateway-ci-slurm:$date
