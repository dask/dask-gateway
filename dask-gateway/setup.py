import os
from setuptools import setup

import versioneer

install_requires = ["tornado", "dask", "distributed"]

extras_require = {"kerberos": ["pykerberos"]}

setup(
    name="dask-gateway",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    maintainer="Jim Crist",
    maintainer_email="jiminy.crist@gmail.com",
    license="BSD",
    description="A client library for interacting with a dask-gateway server",
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    url="http://github.com/jcrist/dask-gateway/",
    packages=["dask_gateway"],
    package_data={"dask_gateway": ["*.yaml"]},
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "dask-gateway-scheduler = dask_gateway.dask_cli:scheduler",
            "dask-gateway-worker = dask_gateway.dask_cli:worker",
        ]
    },
    zip_safe=False,
)
