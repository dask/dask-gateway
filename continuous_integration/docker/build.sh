
# --no-cache
docker build  -t ghcr.io/meta-introspector/dask-gateway-ci-base ./base --label "org.opencontainers.image.source=https://github.com/meta-introspector/dask-gateway"      --label "org.opencontainers.image.description=Test image" 

docker build  -t ghcr.io/meta-introspector/dask-gateway-ci-hadoop ./hadoop --label "org.opencontainers.image.source=https://github.com/meta-introspector/dask-gateway"      --label "org.opencontainers.image.description=Test image" 
docker build  -t ghcr.io/meta-introspector/dask-gateway-ci-pbs ./pbs 
docker build  -t ghcr.io/meta-introspector/dask-gateway-ci-slurm ./slurm --label "org.opencontainers.image.source=https://github.com/meta-introspector/dask-gateway"      --label "org.opencontainers.image.description=Test image" 



# 
# --
