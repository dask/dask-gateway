set -xe

npm install -g configurable-http-proxy

pip install \
    dask \
    distributed \
    cryptography \
    tornado \
    traitlets \
    sqlalchemy \
    pytest \
    pytest-asyncio \
    trustme \
    jupyterhub \
    ipywidgets \
    notebook \
    black \
    flake8

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
