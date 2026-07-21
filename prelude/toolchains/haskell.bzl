# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//:prelude.bzl", _native = "native")
load("@prelude//haskell:toolchain.bzl", "HaskellPlatformInfo", "HaskellToolchainInfo")

GHC_ARCHIVE = {
    "sha256": "78975575b8125ecf1f50f78b1016b14ea6e87abbf1fc39797af469d029c5d737",
    "strip_prefix": "ghc-9.10.1-x86_64-unknown-linux",
    "url": "https://downloads.haskell.org/~ghc/9.10.1/ghc-9.10.1-x86_64-deb11-linux.tar.xz",
}

_GHC_TOOLS = {
    "ghc": "ghc_tool",
    "ghc-pkg": "ghc_pkg_tool",
    "haddock": "haddock_tool",
}

def download_ghc_tools(
        name: str,
        ghc_archive = GHC_ARCHIVE,
        remote_download: bool = True,
        tool_names = _GHC_TOOLS,
        visibility = None):
    archive = name + "_archive"

    _native.http_archive(
        name = archive,
        urls = [ghc_archive["url"]],
        sha256 = ghc_archive["sha256"],
        type = "tar.xz",
        strip_prefix = ghc_archive["strip_prefix"],
        remote_download = remote_download,
        sub_targets = {
            "ghc": ["bin/ghc"],
            "ghc-pkg": ["bin/ghc-pkg"],
            "haddock": ["bin/haddock"],
        },
        visibility = visibility,
    )

    for tool, tool_name in tool_names.items():
        _native.command_alias(
            name = tool_name,
            exe = ":{}[{}]".format(archive, tool),
            resources = [":{}".format(archive)],
            visibility = visibility,
        )

def _haskell_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    return [
        DefaultInfo(),
        HaskellToolchainInfo(
            compiler = ctx.attrs.compiler[RunInfo],
            packager = ctx.attrs.packager[RunInfo],
            linker = ctx.attrs.linker[RunInfo],
            haddock = ctx.attrs.haddock[RunInfo],
            compiler_flags = ctx.attrs.compiler_flags,
            linker_flags = ctx.attrs.linker_flags,
        ),
        HaskellPlatformInfo(
            name = ctx.attrs.platform_name,
        ),
    ]

def _system_haskell_toolchain(_ctx: AnalysisContext) -> list[Provider]:
    return [
        DefaultInfo(),
        HaskellToolchainInfo(
            compiler = "ghc",
            packager = "ghc-pkg",
            linker = "ghc",
            haddock = "haddock",
            compiler_flags = [],
            linker_flags = [],
        ),
        HaskellPlatformInfo(
            name = host_info().arch,
        ),
    ]

system_haskell_toolchain = rule(
    impl = _system_haskell_toolchain,
    attrs = {},
    is_toolchain_rule = True,
)

haskell_toolchain = rule(
    impl = _haskell_toolchain_impl,
    attrs = {
        "compiler": attrs.dep(providers = [RunInfo]),
        "compiler_flags": attrs.list(attrs.arg(), default = []),
        "haddock": attrs.dep(providers = [RunInfo]),
        "linker": attrs.dep(providers = [RunInfo]),
        "linker_flags": attrs.list(attrs.arg(), default = []),
        "packager": attrs.dep(providers = [RunInfo]),
        "platform_name": attrs.string(default = "x86_64"),
    },
    is_toolchain_rule = True,
)
