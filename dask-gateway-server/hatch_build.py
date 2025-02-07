"""
dask-gateway-server has Golang source code in ./dask-gateway-proxy that we want
to compile when installing or building a wheel.

Based on the excellent code in:
- https://github.com/wandb/wandb/blob/77937418f17d0b93f96aaf53857f10bc27aec137/hatch_build.py
- https://github.com/wandb/wandb/blob/77937418f17d0b93f96aaf53857f10bc27aec137/core/hatch.py
"""

import dataclasses
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sysconfig
from collections.abc import Mapping
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# Build options
_DASK_GATEWAY_SERVER__NO_PROXY = "DASK_GATEWAY_SERVER__NO_PROXY"

SOURCE_DIR_PATH = pathlib.Path("dask-gateway-proxy")
TARGET_BINARY_PATH = pathlib.Path("dask_gateway_server", "proxy", "dask-gateway-proxy")


@dataclasses.dataclass(frozen=True)
class TargetPlatform:
    goos: str
    goarch: str


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if self.target_name == "wheel":
            self._prepare_wheel(build_data)

    def clean(self, versions):
        if os.path.exists(TARGET_BINARY_PATH):
            os.remove(TARGET_BINARY_PATH)

    def _prepare_wheel(self, build_data: dict[str, Any]) -> None:
        artifacts: list[str] = build_data["artifacts"]

        if self._is_platform_wheel():
            build_data["tag"] = f"py3-none-{self._get_platform_tag()}"
        else:
            build_data["tag"] = "py3-none-any"

        if self._include_dask_gateway_proxy():
            artifacts.extend(self._build_dask_gateway_proxy())

    def _get_platform_tag(self) -> str:
        """Returns the platform tag for the current platform."""
        # Replace dots, spaces and dashes with underscores following
        # https://packaging.python.org/en/latest/specifications/platform-compatibility-tags/#platform-tag
        platform_tag = re.sub("[-. ]", "_", sysconfig.get_platform())

        # On macOS versions >=11, pip expects the minor version to be 0:
        #   https://github.com/pypa/packaging/issues/435
        #
        # You can see the list of tags that pip would support on your machine
        # using `pip debug --verbose`. On my macOS, get_platform() returns
        # 14.1, but `pip debug --verbose` reports only these py3 tags with 14:
        #
        # * py3-none-macosx_14_0_arm64
        # * py3-none-macosx_14_0_universal2
        #
        # We do this remapping here because otherwise, it's possible for `pip wheel`
        # to successfully produce a wheel that you then cannot `pip install` on the
        # same machine.
        macos_match = re.fullmatch(r"macosx_(\d+_\d+)_(\w+)", platform_tag)
        if macos_match:
            major, _ = macos_match.group(1).split("_")
            if int(major) >= 11:
                arch = macos_match.group(2)
                platform_tag = f"macosx_{major}_0_{arch}"

        return platform_tag

    def _include_dask_gateway_proxy(self) -> bool:
        """Whether the wheel bundles with the dask-gateway-proxy Go binary."""
        return not _get_env_bool(_DASK_GATEWAY_SERVER__NO_PROXY, False)

    def _is_platform_wheel(self) -> bool:
        """Whether the wheel will be platform-specific."""
        return self._include_dask_gateway_proxy()

    def _build_dask_gateway_proxy(self) -> list[str]:
        plat = self._target_platform()

        self.app.display_waiting(
            f"Building dask-gateway-proxy Go binary ({plat.goos}-{plat.goarch})..."
        )
        _go_build_dask_gateway_proxy(
            go_binary=self._get_and_require_go_binary(),
            output_path=TARGET_BINARY_PATH,
            target_system=plat.goos,
            target_arch=plat.goarch,
        )

        # NOTE: as_posix() is used intentionally. Hatch expects forward slashes
        #       even on Windows.
        return [TARGET_BINARY_PATH.as_posix()]

    def _get_and_require_go_binary(self) -> pathlib.Path:
        go = shutil.which("go")

        if not go:
            self.app.abort(
                "Did not find the 'go' binary. You need Go to build dask-gateway-proxy"
                " from source. See https://go.dev/doc/install.",
            )
            raise AssertionError("unreachable")

        return pathlib.Path(go)

    def _target_platform(self) -> "TargetPlatform":
        """Returns the platform we're building for (for cross-compilation)."""
        if os.environ.get("GOOS") and os.environ.get("GOARCH"):
            return TargetPlatform(
                goos=os.environ.get("GOOS"),
                goarch=os.environ.get("GOARCH"),
            )

        # Checking sysconfig.get_platform() is the "standard" way of getting the
        # target platform in Python cross-compilation. Build tools like
        # cibuildwheel control its output by setting the undocumented
        # _PYTHON_HOST_PLATFORM environment variable which is also a good way
        # of manually testing this function.
        plat = sysconfig.get_platform()
        match = re.match(
            r"(win|linux|macosx-.+)-(aarch64|arm64|x86_64|amd64)",
            plat,
        )
        if match:
            if match.group(1).startswith("macosx"):
                goos = "darwin"
            elif match.group(1) == "win":
                goos = "windows"
            else:
                goos = match.group(1)

            goarch = _to_goarch(match.group(2))

            return TargetPlatform(
                goos=goos,
                goarch=goarch,
            )

        self.app.display_warning(
            f"Failed to parse sysconfig.get_platform() ({plat}); disabling"
            " cross-compilation.",
        )

        host_os = platform.system().lower()
        if host_os in ("windows", "darwin", "linux"):
            goos = host_os
        else:
            goos = ""

        goarch = _to_goarch(platform.machine().lower())

        return TargetPlatform(
            goos=goos,
            goarch=goarch,
        )


def _get_env_bool(name: str, default: bool) -> bool:
    """Returns the value of a boolean environment variable."""
    value = os.getenv(name)

    if value is None:
        return default
    elif value.lower() in ("1", "true"):
        return True
    elif value.lower() in ("0", "false"):
        return False
    else:
        raise ValueError(
            f"Environment variable '{name}' has invalid value '{value}'"
            " expected one of {1,true,0,false}."
        )


def _to_goarch(arch: str) -> str:
    """Returns a valid GOARCH value or the empty string."""
    return {
        # amd64 synonyms
        "amd64": "amd64",
        "x86_64": "amd64",
        # arm64 synonyms
        "arm64": "arm64",
        "aarch64": "arm64",
    }.get(arch, "")


def _go_build_dask_gateway_proxy(
    go_binary: pathlib.Path,
    output_path: pathlib.PurePath,
    target_system,
    target_arch,
) -> None:
    """Builds the dask-gateway-proxy Go module.

    Args:
        go_binary: Path to the Go binary, which must exist.
        output_path: The path where to output the binary, relative to the
            workspace root.
        target_system: The target operating system (GOOS) or an empty string
            to use the current OS.
        target_arch: The target architecture (GOARCH) or an empty string
            to use the current architecture.
    """
    cmd = [
        str(go_binary),
        "build",
        f"-ldflags={_go_linker_flags()}",
        f"-o={str('..' / output_path)}",
        str(pathlib.Path("cmd", "dask-gateway-proxy", "main.go")),
    ]
    cwd = str(SOURCE_DIR_PATH)
    env = _go_env(
        target_system=target_system,
        target_arch=target_arch,
    )

    subprocess.check_call(cmd, cwd=cwd, env=env)


def _go_linker_flags() -> str:
    """Returns linker flags for the Go binary as a string."""
    flags = [
        "-s",  # Omit the symbol table and debug info.
    ]
    return " ".join(flags)


def _go_env(target_system: str, target_arch: str) -> Mapping[str, str]:
    env = os.environ.copy()

    env["GOOS"] = target_system
    env["GOARCH"] = target_arch
    env["CGO_ENABLED"] = "0"

    return env
