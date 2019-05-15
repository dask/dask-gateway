set -xe

python -m venv test-environment

source test-environment/bin/activate

pip install \
    dask \
    distributed \
    cryptography \
    tornado \
    traitlets \
    sqlalchemy \
    pytest \
    pytest-asyncio \
    black \
    flake8

pushd dask-gateway
python setup.py develop
popd

pushd dask-gateway-server
python setup.py develop
popd

pip list
