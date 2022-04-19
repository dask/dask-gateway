import os

from setuptools import find_packages, setup

# determine version from _version.py
ns = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "dask_gateway", "_version.py")) as f:
    exec(f.read(), {}, ns)
    VERSION = ns["__version__"]

with open("requirements.txt") as f:
    install_requires = [l for l in f.readlines() if not l.startswith("#")]

extras_require = {
    "kerberos": [
        'pykerberos;platform_system!="Windows"',
        'winkerberos;platform_system=="Windows"',
    ],
}

setup(
    name="dask-gateway",
    version=VERSION,
    maintainer="Jim Crist-Harif",
    maintainer_email="jcristharif@gmail.com",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="dask hadoop kubernetes HPC distributed cluster",
    description="A client library for interacting with a dask-gateway server",
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
    url="https://gateway.dask.org/",
    project_urls={
        "Documentation": "https://gateway.dask.org/",
        "Source": "https://github.com/dask/dask-gateway/",
        "Issue Tracker": "https://github.com/dask/dask-gateway/issues",
    },
    packages=find_packages(),
    package_data={"dask_gateway": ["*.yaml"]},
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.8",
    zip_safe=False,
)
