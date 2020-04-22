set -xe

npm install -g configurable-http-proxy

pip install \
    aiohttp \
    black \
    colorlog \
    cryptography \
    dask \
    distributed \
    flake8 \
    ipywidgets \
    jupyterhub \
    notebook \
    pytest \
    pytest-asyncio==0.10.0 \
    sqlalchemy \
    tornado \
    traitlets \
    trustme

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
