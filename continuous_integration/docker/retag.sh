docker tag ghcr.io/meta-introspector/dask-gateway-ci-base:latest   ghcr.io/dask/dask-gateway-ci-base
docker tag ghcr.io/meta-introspector/dask-gateway-ci-hadoop:latest ghcr.io/dask/dask-gateway-ci-hadoop
docker tag ghcr.io/meta-introspector/dask-gateway-ci-pbs:latest    ghcr.io/dask/dask-gateway-ci-pbs
docker tag ghcr.io/meta-introspector/dask-gateway-ci-slurm:latest  ghcr.io/dask/dask-gateway-ci-slurm


docker push ghcr.io/dask/dask-gateway-ci-base
docker push ghcr.io/
docker push ghcr.io/dask/dask-gateway-ci-pbs
docker push ghcr.io/dask/dask-gateway-ci-slurm
