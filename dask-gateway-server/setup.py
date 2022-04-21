"""
dask-gateway-server has Golang source code in ./dask-gateway-proxy that we want
to compile as part of installing this Package and building a source distribution
or wheels. There is also metadata of a wheel that we would like to set. Because
of this, this setup.py file includes some related complexity.

Relevant reference documentation:
- https://setuptools.pypa.io/en/latest/userguide/extension.html#adding-commands
"""
import errno
import os
import subprocess
import sys

# distutils is deprecated but we need to keep using these imports, see
# https://github.com/pypa/setuptools/issues/2928
from distutils.command.build import build as _build
from distutils.command.clean import clean as _clean

from setuptools import Command, find_packages, setup
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
PROXY_SRC_DIR = os.path.join(ROOT_DIR, "dask-gateway-proxy")
PROXY_TGT_DIR = os.path.join(ROOT_DIR, "dask_gateway_server", "proxy")
PROXY_TGT_EXE = os.path.join(PROXY_TGT_DIR, "dask-gateway-proxy")


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
    This mixin class helps us ensure that we build the dask-gateway-proxy
    executable binary no matter if we run the: install, build, develop, or
    bdist_wheel command.
    """

    def run(self):
        if (
            not os.environ.get("DASK_GATEWAY_SERVER__NO_PROXY")
            and not getattr(self, "uninstall", False)
            and not os.path.exists(PROXY_TGT_EXE)
        ):
            self.run_command("build_proxy")
        super().run()


class build(build_proxy_mixin, _build):
    pass


class install(build_proxy_mixin, _install):
    pass


class develop(build_proxy_mixin, _develop):
    pass


class clean(_clean):
    def run(self):
        super().run()
        if self.all and os.path.exists(PROXY_TGT_EXE):
            os.remove(PROXY_TGT_EXE)


try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

    class bdist_wheel(build_proxy_mixin, _bdist_wheel):
        """
        When we build wheels, they are without this override named
        "<something>-py3-none-any.whl" which is incorrect as we have a platform
        dependency.

        By overriding the `root_is_pure` option and the `get_tag` we can declare
        a dependency to the Golang compiled executable binary.

        This is based on https://stackoverflow.com/a/45150383/2220152.
        """

        def finalize_options(self):
            super().finalize_options()
            # Cleanup binaries ahead of time to avoid packaging an existing
            # binary with the wheel that shouldn't be packaged.
            if os.path.exists(PROXY_TGT_EXE):
                os.remove(PROXY_TGT_EXE)

            if os.environ.get("DASK_GATEWAY_SERVER__NO_PROXY"):
                self.root_is_pure = True
            else:
                self.root_is_pure = False

        def get_tag(self):
            python, abi, plat = super().get_tag()
            python, abi = "py3", "none"

            if os.environ.get("DASK_GATEWAY_SERVER__NO_PROXY"):
                return python, abi, "any"

            # If GOOS or GOARCH are set, we will probably be cross compiling.
            # Below we act intelligently based on a few combinations of GOOS and
            # GOARCH to provide a platform tag that is accepted by PyPI.
            #
            # PyPI restricts us to using the following platform tags:
            # https://github.com/pypa/warehouse/blob/82815b06d9f98deed5f205c66e054de59d22a10d/warehouse/forklift/legacy.py#L108-L180
            #
            # For reference, GitHub Actions available operating systems:
            # https://github.com/actions/virtual-environments#available-environments
            #
            go_plat = os.environ.get("GOOS", "") + "_" + os.environ.get("GOARCH", "")
            go_plat_to_pypi_plat_mapping = {
                "linux_amd64": "manylinux_2_17_x86_64.manylinux2014_x86_64",
                "linux_arm64": "manylinux_2_17_aarch64.manylinux2014_aarch64",
                "darwin_amd64": "macosx_10_15_x86_64",
                "darwin_arm64": "macosx_11_0_arm64",
            }
            if go_plat in go_plat_to_pypi_plat_mapping:
                plat = go_plat_to_pypi_plat_mapping[go_plat]

            return python, abi, plat

except ImportError:
    bdist_wheel = None

# cmdclass describes commands we can run with "python setup.py <command>". We
# have overridden several command classes by mixing in a common capability of
# compiling the golang executable binary.
#
# It seems that only by overriding multiple command classes we can ensure we
# manage to compile the Golang executable binary before its needed. Below are
# examples of end user commands that are impacted by our changes to cmdclass.
#
#   python setup.py [bdist_wheel|build|install|develop|clean]
#   python -m pip install [--editable] .
#   python -m build --wheel .
#
cmdclass = {
    "bdist_wheel": bdist_wheel,
    "build": build,
    "build_proxy": build_proxy,
    "install": install,
    "clean": clean,
    "develop": develop,
}

if os.environ.get("DASK_GATEWAY_SERVER__NO_PROXY"):
    package_data = {}
else:
    # FIXME: Does it make sense to put the binary next to the source code
    #        (proxy/dask-gateway-proxy)? If we build a wheel now we get a folder in
    #        the wheel called purelib that includes this and it seems contradictory.
    #
    #        An action point to resolve this would be to investigate how other
    #        projects have bundled compiled binaries.
    #
    package_data = {"dask_gateway_server": ["proxy/dask-gateway-proxy"]}

# determine version from _version.py
ns = {}
with open(os.path.join(ROOT_DIR, "dask_gateway_server", "_version.py")) as f:
    exec(f.read(), {}, ns)
    VERSION = ns["__version__"]

with open("requirements.txt") as f:
    install_requires = [l for l in f.readlines() if not l.startswith("#")]

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
    long_description_content_type="text/x-rst",
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
