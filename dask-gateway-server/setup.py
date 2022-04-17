import errno
import os
import subprocess
import sys
import sysconfig

# distutils is deprecated but we need to keep using these imports, see
# https://github.com/pypa/setuptools/issues/2928
from distutils.command.build import build as _build
from distutils.command.clean import clean as _clean

from setuptools import setup, find_packages, Command
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
PROXY_SRC_DIR = os.path.join(ROOT_DIR, "dask-gateway-proxy")
PROXY_TGT_DIR = os.path.join(ROOT_DIR, "dask_gateway_server", "proxy")
PROXY_TGT_EXE = os.path.join(PROXY_TGT_DIR, "dask-gateway-proxy")

# determine version from _version.py
ns = {}
with open(os.path.join(ROOT_DIR, "dask_gateway_server", "_version.py")) as f:
    exec(f.read(), {}, ns)
    VERSION = ns["__version__"]

# package_data may be reset to {} by the passing of the --no-build-proxy option
package_data = {"dask_gateway_server": ["proxy/dask-gateway-proxy"]}

platforms = []
if os.environ.get("GOOS") and os.environ.get("GOARCH"):
    platform_tag = f"{os.environ['GOOS']}_{os.environ['GOARCH']}"
else:
    platform_tag = sysconfig.get_platform()
platforms.append(platform_tag)


class build_proxy(Command):
    """
    A command the compile the golang code in ./dask-gateway-proxy and copy the
    executable binary to dask_gateway_server/proxy/dask-gateway-proxy to be
    picked up via package_data.

    setuptools.Command derived classes are required to implement
    initialize_options, finalize_options, and run.
    """

    description = "build go artifact"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        self.mkpath(PROXY_TGT_DIR)
        try:
            code = subprocess.call(
                ["go", "build", "-o", PROXY_TGT_EXE],
                cwd=PROXY_SRC_DIR,
            )
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                self.warn(
                    "Building dask-gateway-server requires a go compiler, "
                    "which wasn't found in your environment. Please install "
                    "go using a package manager (e.g. apt, brew, conda, etc...), "
                    "or download from https://go.dev/dl/."
                )
                sys.exit(1)
            raise
        if code:
            sys.exit(code)


class build_proxy_mixin:
    """
    _no_build_proxy_sticky retains memory if this flag has been passed. This is
    needed as running "python setup.py install" leads to both install and build
    commands are executed but only the install command is passed the flag.
    """

    _no_build_proxy_option = (
        "no-build-proxy",
        None,
        "Don't build the dask-gateway-proxy executable from its Golang source code",
    )
    _no_build_proxy_sticky = False

    def initialize_options(self):
        self.no_build_proxy = False
        super().initialize_options()

    def run(self):
        if self.no_build_proxy:
            __class__._no_build_proxy_sticky = True
        if not __class__._no_build_proxy_sticky:
            if not getattr(self, "uninstall", False) and not os.path.exists(
                PROXY_TGT_EXE
            ):
                self.run_command("build_proxy")
        else:
            package_data.pop("dask_gateway_server", None)
        super().run()


class build(build_proxy_mixin, _build):
    user_options = list(_build.user_options)
    user_options.append(build_proxy_mixin._no_build_proxy_option)


class install(build_proxy_mixin, _install):
    user_options = list(_install.user_options)
    user_options.append(build_proxy_mixin._no_build_proxy_option)


class develop(build_proxy_mixin, _develop):
    user_options = list(_develop.user_options)
    user_options.append(build_proxy_mixin._no_build_proxy_option)


class clean(_clean):
    def run(self):
        if self.all:
            if os.path.exists(PROXY_TGT_EXE):
                os.remove(PROXY_TGT_EXE)
        _clean.run(self)


cmdclass = {
    "build_proxy": build_proxy,  # directly build the proxy source
    "build": build,  # bdist_wheel or pip install .
    "install": install,  # python setup.py install
    "develop": develop,  # python setup.py develop
    "clean": clean,
}


# NOTE: changes to the dependencies here must also be reflected
# in ../dev-environment.yaml
install_requires = [
    "aiohttp",
    "colorlog",
    "cryptography",
    "traitlets",
]

extras_require = {
    # pykerberos is tricky to install and requires a system package to
    # successfully compile some C code, on ubuntu this is libkrb5-dev.
    "kerberos": ["pykerberos"],
    "jobqueue": ["sqlalchemy"],
    "local": ["sqlalchemy"],
    "yarn": ["sqlalchemy", "skein >= 0.7.3"],
    "kubernetes": ["kubernetes_asyncio"],
    "all_backends": [
        "sqlalchemy",
        "skein >= 0.7.3",
        "kubernetes_asyncio",
    ],
}


# setup's keyword reference:
# https://setuptools.pypa.io/en/latest/references/keywords.html
#
setup(
    name="dask-gateway-server",
    version=VERSION,
    cmdclass=cmdclass,
    platforms=platforms,
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="dask hadoop kubernetes HPC distributed cluster",
    description=(
        "A multi-tenant server for securely deploying and managing "
        "multiple Dask clusters."
    ),
    long_description=open("README.rst").read(),
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
    python_requires=">=3.8",
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
