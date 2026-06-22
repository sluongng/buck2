# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//http_archive:exec_deps.bzl", "HttpArchiveExecDeps")
load("@prelude//http_archive:unarchive.bzl", "unarchive")

_PROTOBUF_RELEASES = {
    "35.0": {
        "linux-aarch_64": {
            "sha256": "36b518ac14d90351cc6598228ed2bbe5afe4e357b1af470b07e0ec1609875de2",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-aarch_64.zip",
        },
        "linux-x86_32": {
            "sha256": "3a24fbfd69a8c7e09db98eeed81da19cab2c08c5361dc3f599f693c0efb1cfc7",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-x86_32.zip",
        },
        "linux-x86_64": {
            "sha256": "a45cda0989c17dd950db55f6fbe1e5814c50fda08e87aa422980ac1f89dddbbc",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-x86_64.zip",
        },
        "osx-aarch_64": {
            "sha256": "45444963204757fd3e2fbe304bc1fdadfb488d8556ff099c4cc06575eab88976",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-osx-aarch_64.zip",
        },
        "osx-x86_64": {
            "sha256": "3580c2d115fccb5b0239960c8f70f8da14787b1973a46b2f39c315ad71c11e01",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-osx-x86_64.zip",
        },
        "win32": {
            "sha256": "99ff33772ca055cd096782d05395bbcc4b6187cce284d2a914fc3745d3ecb6b6",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-win32.zip",
        },
        "win64": {
            "sha256": "d1cede9e308cc3eb072392af1c02ccae4bdd3d2f374ec2970dbd8cdfdaa91363",
            "url": "https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-win64.zip",
        },
    },
}

GoProtoCompilerInfo = provider(
    fields = {
        "always_generates": provider_field(bool, default = False),
        "deps": provider_field(list[Dependency], default = []),
        "import_path_option": provider_field(bool, default = False),
        "options": provider_field(list[str], default = []),
        "plugin": provider_field(RunInfo),
        "plugin_name": provider_field(str),
        "suffix": provider_field(str),
        "suffixes": provider_field(list[str], default = []),
        "valid_archive": provider_field(bool, default = True),
    },
)

GoProtoToolchainInfo = provider(
    fields = {
        "include": provider_field(typing.Any, default = None),
        "protoc": provider_field(RunInfo),
    },
)

def _go_proto_compiler_impl(ctx: AnalysisContext) -> list[Provider]:
    return [
        GoProtoCompilerInfo(
            always_generates = ctx.attrs.always_generates,
            deps = ctx.attrs.deps,
            import_path_option = ctx.attrs.import_path_option,
            options = ctx.attrs.options,
            plugin = ctx.attrs.plugin[RunInfo],
            plugin_name = ctx.attrs.plugin_name,
            suffix = ctx.attrs.suffix,
            suffixes = ctx.attrs.suffixes,
            valid_archive = ctx.attrs.valid_archive,
        ),
        DefaultInfo(),
    ]

go_proto_compiler = rule(
    impl = _go_proto_compiler_impl,
    attrs = {
        "always_generates": attrs.bool(default = False),
        "deps": attrs.list(attrs.dep(), default = []),
        "import_path_option": attrs.bool(default = False),
        "options": attrs.list(attrs.string(), default = []),
        "plugin": attrs.exec_dep(providers = [RunInfo]),
        "plugin_name": attrs.string(),
        "suffix": attrs.string(default = ".pb.go"),
        "suffixes": attrs.list(attrs.string(), default = []),
        "valid_archive": attrs.bool(default = True),
    },
)

def _system_go_proto_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    return [
        DefaultInfo(),
        GoProtoToolchainInfo(
            include = ctx.attrs.include,
            protoc = RunInfo(ctx.attrs.protoc),
        ),
    ]

system_go_proto_toolchain = rule(
    impl = _system_go_proto_toolchain_impl,
    attrs = {
        "include": attrs.option(attrs.source(allow_directory = True), default = None),
        "protoc": attrs.string(default = "protoc"),
    },
    is_toolchain_rule = True,
)

def _remote_go_proto_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    archive = None
    if not ctx.attrs.remote_download:
        if len(ctx.attrs.urls) != 1:
            fail("remote_go_proto_toolchain local downloads require exactly one URL")
        archive = ctx.actions.declare_output("archive.zip", has_content_based_path = True)
        ctx.actions.download_file(
            archive.as_output(),
            ctx.attrs.urls[0],
            sha256 = ctx.attrs.sha256,
            has_content_based_path = True,
        )

    output, _ = unarchive(
        ctx,
        archive = archive,
        download_urls = ctx.attrs.urls,
        output_name = "protobuf",
        ext_type = "zip",
        excludes = [],
        remote_download = ctx.attrs.remote_download,
        sha1 = None,
        sha256 = ctx.attrs.sha256,
        size_bytes = None,
        strip_prefix = None,
        patch_args = [],
        patches = [],
        sub_targets = [],
        exec_deps = ctx.attrs.exec_deps[HttpArchiveExecDeps],
        prefer_local = False,
        resolve_static_crates = False,
        has_content_based_path = True,
    )
    protoc = output.project(ctx.attrs.protoc)
    include = output.project(ctx.attrs.include)
    return [
        DefaultInfo(
            default_output = protoc,
            sub_targets = {
                "include": [DefaultInfo(default_output = include)],
            },
        ),
        GoProtoToolchainInfo(
            include = include,
            protoc = RunInfo(protoc),
        ),
    ]

_remote_go_proto_toolchain = rule(
    impl = _remote_go_proto_toolchain_impl,
    attrs = {
        "exec_deps": attrs.exec_dep(
            providers = [HttpArchiveExecDeps],
            default = "prelude//http_archive/tools:exec_deps",
        ),
        "include": attrs.string(default = "include"),
        "protoc": attrs.string(default = "bin/protoc"),
        "remote_download": attrs.bool(default = True),
        "sha256": attrs.string(),
        "urls": attrs.list(attrs.string()),
    },
    is_toolchain_rule = True,
)

def _release(version: str, platform: str) -> dict[str, str]:
    if version not in _PROTOBUF_RELEASES:
        fail("unknown protobuf release {}; available versions: {}".format(
            version,
            ", ".join(_PROTOBUF_RELEASES.keys()),
        ))
    release = _PROTOBUF_RELEASES[version]
    if platform not in release:
        fail("protobuf release {} does not support platform {}; available platforms: {}".format(
            version,
            platform,
            ", ".join(release.keys()),
        ))
    return release[platform]

def _release_attr(version: str, platform: str, attr: str) -> str:
    return _release(version, platform)[attr]

def _protoc_path() -> typing.Any:
    return select({
        "config//os:windows": "bin/protoc.exe",
        "DEFAULT": "bin/protoc",
    })

def remote_go_proto_toolchain(
        name: str,
        version: str = "35.0",
        remote_download: bool = True,
        visibility: list[str] | None = None):
    _remote_go_proto_toolchain(
        name = name,
        protoc = _protoc_path(),
        remote_download = remote_download,
        sha256 = select({
            "config//os:linux": select({
                "config//cpu:arm64": _release_attr(version, "linux-aarch_64", "sha256"),
                "config//cpu:x86_32": _release_attr(version, "linux-x86_32", "sha256"),
                "config//cpu:x86_64": _release_attr(version, "linux-x86_64", "sha256"),
            }),
            "config//os:macos": select({
                "config//cpu:arm64": _release_attr(version, "osx-aarch_64", "sha256"),
                "config//cpu:x86_64": _release_attr(version, "osx-x86_64", "sha256"),
            }),
            "config//os:windows": select({
                "config//cpu:x86_32": _release_attr(version, "win32", "sha256"),
                "config//cpu:x86_64": _release_attr(version, "win64", "sha256"),
            }),
        }),
        urls = select({
            "config//os:linux": select({
                "config//cpu:arm64": [_release_attr(version, "linux-aarch_64", "url")],
                "config//cpu:x86_32": [_release_attr(version, "linux-x86_32", "url")],
                "config//cpu:x86_64": [_release_attr(version, "linux-x86_64", "url")],
            }),
            "config//os:macos": select({
                "config//cpu:arm64": [_release_attr(version, "osx-aarch_64", "url")],
                "config//cpu:x86_64": [_release_attr(version, "osx-x86_64", "url")],
            }),
            "config//os:windows": select({
                "config//cpu:x86_32": [_release_attr(version, "win32", "url")],
                "config//cpu:x86_64": [_release_attr(version, "win64", "url")],
            }),
        }),
        visibility = visibility,
    )
