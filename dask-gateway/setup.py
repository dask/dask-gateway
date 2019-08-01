import os
from setuptools import setup, find_packages

import versioneer

install_requires = ["tornado", "dask>=2.2.0", "distributed>=2.2.0"]

extras_require = {
    "kerberos": [
        'pykerberos;platform_system!="Windows"',
        'winkerberos;platform_system=="Windows"',
    ]
}

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
    packages=find_packages(),
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
