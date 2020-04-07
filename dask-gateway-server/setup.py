import errno
import os
import subprocess
import sys
from distutils.command.build import build as _build
from distutils.command.clean import clean as _clean

from setuptools import setup, find_packages, Command
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
PROXY_SRC_DIR = os.path.join(ROOT_DIR, "dask-gateway-proxy")
PROXY_TGT_DIR = os.path.join(ROOT_DIR, "dask_gateway_server", "proxy")
PROXY_TGT_EXE = os.path.join(PROXY_TGT_DIR, "dask-gateway-proxy")

ns = {}
with open(os.path.join(ROOT_DIR, "dask_gateway_server", "_version.py")) as f:
    exec(f.read(), {}, ns)
    VERSION = ns["__version__"]

NO_PROXY = "--no-build-proxy" in sys.argv
if NO_PROXY:
    package_data = {}
else:
    package_data = {"dask_gateway_server": ["proxy/dask-gateway-proxy"]}


class build_proxy(Command):
    description = "build go artifacts"

    user_options = []

    def initialize_options(self):
        pass

    finalize_options = initialize_options

    def run(self):
        # Compile the go code and copy the executable to dask_gateway_server/proxy/
        # This will be picked up as package_data later
        self.mkpath(PROXY_TGT_DIR)
        try:
            code = subprocess.call(
                ["go", "build", "-o", PROXY_TGT_EXE], cwd=PROXY_SRC_DIR
            )
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                self.warn(
                    "Building dask-gateway-server requires a go compiler, "
                    "which wasn't found in your environment. Please install "
                    "go using a package manager (e.g. apt, brew, conda, etc...), "
                    "or download from https://golang.org/dl/."
                )
                sys.exit(1)
            raise
        if code:
            sys.exit(code)


class mixin(object):
    def initialize_options(self):
        self.no_build_proxy = False
        super().initialize_options()

    def run(self):
        global NO_PROXY
        if self.no_build_proxy:
            NO_PROXY = True
        if not getattr(self, "uninstall", False):
            if not NO_PROXY and not os.path.exists(PROXY_TGT_EXE):
                self.run_command("build_proxy")
        super().run()


class build(mixin, _build):
    user_options = list(_build.user_options)
    user_options.append(("no-build-proxy", None, "Don't build the proxy source"))


class install(mixin, _install):
    user_options = list(_install.user_options)
    user_options.append(("no-build-proxy", None, "Don't build the proxy source"))


class develop(mixin, _develop):
    user_options = list(_develop.user_options)
    user_options.append(("no-build-proxy", None, "Don't build the proxy source"))


class clean(_clean):
    def run(self):
        if self.all:
            for f in [PROXY_TGT_EXE]:
                if os.path.exists(f):
                    os.unlink(f)
        _clean.run(self)


install_requires = ["aiohttp", "colorlog", "cryptography", "traitlets"]

extras_require = {
    "kerberos": ["pykerberos"],
    "jobqueue": ["sqlalchemy"],
    "local": ["sqlalchemy"],
    "yarn": ["sqlalchemy", "skein >= 0.7.3"],
    "kubernetes": ["kubernetes_asyncio"],
}

# Due to quirks in setuptools/distutils dependency ordering, to get the go
# source to build automatically in most cases, we need to check in multiple
# locations. This is unfortunate, but seems necessary.
cmdclass = {
    "build_proxy": build_proxy,  # directly build the proxy source
    "build": build,  # bdist_wheel or pip install .
    "install": install,  # python setup.py install
    "develop": develop,  # python setup.py develop
    "clean": clean,
}


setup(
    name="dask-gateway-server",
    version=VERSION,
    cmdclass=cmdclass,
    maintainer="Jim Crist-Harif",
    maintainer_email="jcristharif@gmail.com",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Systems Administration",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    keywords="dask hadoop kubernetes HPC distributed cluster",
    description=(
        "A multi-tenant server for securely deploying and managing "
        "multiple Dask clusters."
    ),
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    url="https://gateway.dask.org/",
    project_urls={
        "Documentation": "https://gateway.dask.org/",
        "Source": "https://github.com/dask/dask-gateway/",
        "Issue Tracker": "https://github.com/dask/dask-gateway/issues",
    },
    packages=find_packages(),
    package_data=package_data,
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "dask-gateway-server = dask_gateway_server.app:main",
            (
                "dask-gateway-jobqueue-launcher = "
                "dask_gateway_server.backends.jobqueue.launcher:main"
            ),
        ]
    },
    zip_safe=False,
)
