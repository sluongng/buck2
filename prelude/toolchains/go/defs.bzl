# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""Helpers for hermetic Go SDK toolchains backed by Go release archives."""

load("@prelude//:native.bzl", _native = "native")
load(
    "@prelude//toolchains/go:go_bootstrap_toolchain.bzl",
    "go_bootstrap_distr",
    "go_bootstrap_toolchain",
)
load("@prelude//toolchains/go:go_toolchain.bzl", "go_distr", "go_toolchain")
load(":releases.bzl", "go_latest_by_release_line", "go_releases", "go_supported_platforms")

_OS_CONSTRAINTS = {
    "darwin": "config//os:macos",
    "linux": "config//os:linux",
    "windows": "config//os:windows",
}

_CPU_CONSTRAINTS = {
    "amd64": "config//cpu:x86_64",
    "arm64": "config//cpu:arm64",
}

_OS_ORDER = ["darwin", "linux", "windows"]
_ARCH_ORDER = ["amd64", "arm64"]

def _normalize_version(version: str) -> str:
    if version.startswith("go"):
        version = version[2:]
    return go_latest_by_release_line.get(version, version)

def _split_platform(platform: str) -> (str, str):
    parts = platform.split("-")
    if len(parts) != 2:
        fail("invalid Go platform {}; expected GOOS-GOARCH".format(platform))
    return parts[0], parts[1]

def _archive_name(prefix: str, version: str, platform: str) -> str:
    return "{}_go{}.{}".format(prefix, version, platform)

def _platform_select(values):
    os_entries = {}
    for goos in _OS_ORDER:
        cpu_entries = {}
        for goarch in _ARCH_ORDER:
            platform = "{}-{}".format(goos, goarch)
            if platform in values:
                cpu_entries[_CPU_CONSTRAINTS[goarch]] = values[platform]
        if cpu_entries:
            os_entries[_OS_CONSTRAINTS[goos]] = select(cpu_entries)
    if not os_entries:
        fail("no Go platforms were provided")
    return select(os_entries)

def download_go_toolchains(
        version: str,
        name: str = "go",
        bootstrap_name: str = "go_bootstrap",
        distribution_name: str = "go_distr",
        bootstrap_distribution_name: str = "go_bootstrap_distr",
        archive_name_prefix: str = "archive",
        platforms: list[str] = go_supported_platforms,
        remote_download: bool = False,
        visibility = None):
    """Instantiate hermetic Go and Go bootstrap toolchains from Go SDK archives."""

    version = _normalize_version(version)
    release = go_releases.get(version)
    if release == None:
        fail("unsupported Go SDK version {}".format(version))

    go_roots = {}
    go_os_arch = {}
    env_go_arch = {}
    env_go_os = {}

    for platform in platforms:
        entry = release.get(platform)
        if entry == None:
            fail("unsupported Go SDK platform {} for version {}".format(platform, version))

        archive_name = _archive_name(archive_name_prefix, version, platform)
        _native.http_archive(
            name = archive_name,
            remote_download = remote_download,
            sha256 = entry["sha256"],
            strip_prefix = "go",
            type = entry["archive_type"],
            urls = [entry["url"]],
        )

        goos, goarch = _split_platform(platform)
        go_roots[platform] = ":" + archive_name
        go_os_arch[platform] = (goos, goarch)
        env_go_arch[platform] = goarch
        env_go_os[platform] = goos

    go_bootstrap_distr(
        name = bootstrap_distribution_name,
        go_os_arch = _platform_select(go_os_arch),
        go_root = _platform_select(go_roots),
    )

    go_bootstrap_toolchain(
        name = bootstrap_name,
        env_go_arch = _platform_select(env_go_arch),
        env_go_os = _platform_select(env_go_os),
        go_bootstrap_distr = ":" + bootstrap_distribution_name,
        visibility = visibility,
    )

    go_distr(
        name = distribution_name,
        go_os_arch = _platform_select(go_os_arch),
        go_root = _platform_select(go_roots),
        version = version,
    )

    go_toolchain(
        name = name,
        env_go_arch = _platform_select(env_go_arch),
        env_go_os = _platform_select(env_go_os),
        go_distr = ":" + distribution_name,
        visibility = visibility,
    )
