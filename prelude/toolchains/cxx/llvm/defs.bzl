# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""C/C++ toolchain backed by hermetic-llvm prebuilt minimal LLVM archives."""

load("@prelude//cxx:cxx_toolchain_types.bzl", "BinaryUtilitiesInfo", "CCompilerInfo", "CxxCompilerInfo", "CxxInternalTools", "DepTrackingMode", "LinkerInfo", "LinkerType", "PicBehavior", "ShlibInterfacesMode", "StripFlagsInfo", "cxx_toolchain_infos")
load("@prelude//cxx:headers.bzl", "HeaderMode")
load("@prelude//cxx:linker.bzl", "is_pdb_generated")
load("@prelude//:native.bzl", _native = "native")
load("@prelude//linking:link_info.bzl", "LinkStyle")
load("@prelude//os_lookup:defs.bzl", "ScriptLanguage")
load("@prelude//utils:cmd_script.bzl", "cmd_script")
load(":releases.bzl", "llvm_minimal_releases")

LLVMDistributionInfo = provider(
    fields = {
        "arch": provider_field(str),
        "llvm_version": provider_field(str),
        "os": provider_field(str),
        "target": provider_field(str),
    },
)

def _host_arch() -> str:
    arch = host_info().arch
    if arch.is_x86_64:
        return "amd64"
    if arch.is_aarch64:
        return "arm64"
    fail("Unsupported host architecture for hermetic LLVM prebuilt toolchain")

def _host_os() -> str:
    os = host_info().os
    if os.is_linux:
        return "linux"
    if os.is_macos:
        return "darwin"
    if os.is_windows:
        return "windows"
    fail("Unsupported host OS for hermetic LLVM prebuilt toolchain")

def _default_target(os: str, arch: str) -> str:
    if os == "linux":
        return "{}-{}-musl".format(os, arch)
    return "{}-{}".format(os, arch)

def _repo_target(target: str) -> str:
    return target.replace("-musl", "")

def _release_for(llvm_version: str, target: str, suffix: str | None):
    release_key = "llvm-{}{}".format(llvm_version, suffix) if suffix else llvm_minimal_releases["latest_by_llvm_version"][llvm_version]
    return llvm_minimal_releases["releases"][release_key][target]

def download_llvm_distribution(
        name: str,
        llvm_version: str = "22.1.7",
        target: str | None = None,
        os: str | None = None,
        arch: str | None = None,
        suffix: str | None = None,
        remote_download: bool = True,
        visibility = None):
    os = os or _host_os()
    arch = arch or _host_arch()
    target = target or _default_target(os, arch)
    release = _release_for(llvm_version, target, suffix)
    archive_name = name + "-archive"

    _native.http_archive(
        name = archive_name,
        urls = [release["url"]],
        sha256 = release["sha256"],
        type = "tar.zst",
        remote_download = remote_download,
        visibility = visibility,
    )
    llvm_distribution(
        name = name,
        archive = ":" + archive_name,
        llvm_version = llvm_version,
        os = os,
        arch = arch,
        target = _repo_target(target),
        visibility = visibility,
    )

def _llvm_distribution_impl(ctx: AnalysisContext) -> list[Provider]:
    return [
        ctx.attrs.archive[DefaultInfo],
        LLVMDistributionInfo(
            arch = ctx.attrs.arch,
            llvm_version = ctx.attrs.llvm_version,
            os = ctx.attrs.os,
            target = ctx.attrs.target,
        ),
    ]

llvm_distribution = rule(
    impl = _llvm_distribution_impl,
    attrs = {
        "arch": attrs.string(),
        "archive": attrs.dep(providers = [DefaultInfo]),
        "llvm_version": attrs.string(),
        "os": attrs.string(),
        "target": attrs.string(),
    },
)

def _tool_path(ctx: AnalysisContext, tool: str):
    dist = ctx.attrs.distribution[LLVMDistributionInfo]
    root = ctx.attrs.distribution[DefaultInfo].default_outputs[0]
    suffix = ".exe" if dist.os == "windows" else ""
    return cmd_args(root, format = "{}/bin/" + tool + suffix)

def _tool(ctx: AnalysisContext, tool: str):
    dist = ctx.attrs.distribution[LLVMDistributionInfo]
    return cmd_script(
        actions = ctx.actions,
        name = tool,
        cmd = _tool_path(ctx, tool),
        language = ScriptLanguage("bat" if dist.os == "windows" else "sh"),
    )

def _linker_type(os: str) -> LinkerType:
    if os == "darwin":
        return LinkerType("darwin")
    if os == "windows":
        return LinkerType("windows")
    return LinkerType("gnu")

def _binary_extension(os: str) -> str:
    return "exe" if os == "windows" else ""

def _object_extension(os: str) -> str:
    return "obj" if os == "windows" else "o"

def _static_library_extension(os: str) -> str:
    return "lib" if os == "windows" else "a"

def _shared_library_name_default_prefix(os: str) -> str:
    return "" if os == "windows" else "lib"

def _shared_library_name_format(os: str) -> str:
    if os == "darwin":
        return "{}.dylib"
    if os == "windows":
        return "{}.dll"
    return "{}.so"

def _pic_behavior(os: str) -> PicBehavior:
    if os == "windows":
        return PicBehavior("not_supported")
    if os == "darwin":
        return PicBehavior("always_enabled")
    return PicBehavior("supported")

def _target_flags(ctx: AnalysisContext):
    flags = []
    if ctx.attrs.target != None:
        flags.extend(["-target", ctx.attrs.target])
    if ctx.attrs.sysroot != None:
        flags.append(cmd_args(ctx.attrs.sysroot, format = "--sysroot={}"))
    return flags

def _cxx_llvm_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    dist = ctx.attrs.distribution[LLVMDistributionInfo]
    os = dist.os
    target_flags = _target_flags(ctx)
    clang = _tool(ctx, "clang")
    clangxx = _tool(ctx, "clang++")
    llvm_ar = _tool(ctx, "llvm-ar")
    llvm_ranlib = _tool(ctx, "llvm-ranlib")
    linker_type = _linker_type(os)
    linker_flags = target_flags + ctx.attrs.linker_flags
    if os == "linux" and ctx.attrs.use_lld:
        linker_flags = linker_flags + ["-fuse-ld=lld"]

    return [ctx.attrs.distribution[DefaultInfo]] + cxx_toolchain_infos(
        platform_name = dist.target,
        internal_tools = ctx.attrs._cxx_internal_tools[CxxInternalTools],
        c_compiler_info = CCompilerInfo(
            compiler = RunInfo(args = cmd_args(clang)),
            compiler_type = "clang",
            compiler_flags = cmd_args(target_flags, ctx.attrs.c_compiler_flags),
            preprocessor_flags = cmd_args(ctx.attrs.c_preprocessor_flags),
        ),
        cxx_compiler_info = CxxCompilerInfo(
            compiler = RunInfo(args = cmd_args(clangxx)),
            compiler_type = "clang",
            compiler_flags = cmd_args(target_flags, ctx.attrs.cxx_compiler_flags),
            preprocessor_flags = cmd_args(ctx.attrs.cxx_preprocessor_flags),
        ),
        linker_info = LinkerInfo(
            archiver = RunInfo(args = cmd_args(llvm_ar)),
            archiver_type = "gnu",
            archiver_supports_argfiles = True,
            archive_objects_locally = False,
            binary_extension = _binary_extension(os),
            generate_linker_maps = False,
            independent_shlib_interface_linker_flags = [],
            is_pdb_generated = is_pdb_generated(linker_type, linker_flags),
            link_binaries_locally = False,
            link_libraries_locally = False,
            link_style = LinkStyle(ctx.attrs.link_style),
            link_weight = 1,
            linker = RunInfo(args = cmd_args(clangxx)),
            linker_flags = cmd_args(linker_flags),
            object_file_extension = _object_extension(os),
            post_linker_flags = cmd_args(ctx.attrs.post_linker_flags),
            shared_dep_runtime_ld_flags = ctx.attrs.shared_dep_runtime_ld_flags,
            shared_library_name_default_prefix = _shared_library_name_default_prefix(os),
            shared_library_name_format = _shared_library_name_format(os),
            shared_library_versioned_name_format = _shared_library_name_format(os) + ".{}",
            shlib_interfaces = ShlibInterfacesMode("disabled"),
            static_dep_runtime_ld_flags = ctx.attrs.static_dep_runtime_ld_flags,
            static_library_extension = _static_library_extension(os),
            static_pic_dep_runtime_ld_flags = ctx.attrs.static_pic_dep_runtime_ld_flags,
            type = linker_type,
            use_archiver_flags = True,
        ),
        binary_utilities_info = BinaryUtilitiesInfo(
            dwp = None,
            nm = RunInfo(args = cmd_args(_tool(ctx, "llvm-nm"))),
            objcopy = RunInfo(args = cmd_args(_tool(ctx, "llvm-objcopy"))),
            objdump = RunInfo(args = cmd_args(_tool(ctx, "llvm-objdump"))),
            ranlib = RunInfo(args = cmd_args(llvm_ranlib)),
            strip = RunInfo(args = cmd_args(_tool(ctx, "llvm-strip"))),
        ),
        cpp_dep_tracking_mode = DepTrackingMode("show_headers"),
        header_mode = HeaderMode("symlink_tree_only"),
        llvm_link = RunInfo(args = cmd_args(_tool(ctx, "llvm-link"))),
        pic_behavior = _pic_behavior(os),
        strip_flags_info = StripFlagsInfo(
            strip_all_flags = ctx.attrs.strip_all_flags,
            strip_debug_flags = ctx.attrs.strip_debug_flags,
            strip_non_global_flags = ctx.attrs.strip_non_global_flags,
        ),
        use_dep_files = True,
    )

cxx_llvm_toolchain = rule(
    impl = _cxx_llvm_toolchain_impl,
    attrs = {
        "c_compiler_flags": attrs.list(attrs.arg(), default = []),
        "c_preprocessor_flags": attrs.list(attrs.arg(), default = []),
        "cxx_compiler_flags": attrs.list(attrs.arg(), default = []),
        "cxx_preprocessor_flags": attrs.list(attrs.arg(), default = []),
        "distribution": attrs.exec_dep(providers = [DefaultInfo, LLVMDistributionInfo]),
        "link_style": attrs.enum(LinkStyle.values(), default = "static"),
        "linker_flags": attrs.list(attrs.arg(), default = []),
        "post_linker_flags": attrs.list(attrs.arg(), default = []),
        "shared_dep_runtime_ld_flags": attrs.list(attrs.arg(), default = []),
        "static_dep_runtime_ld_flags": attrs.list(attrs.arg(), default = []),
        "static_pic_dep_runtime_ld_flags": attrs.list(attrs.arg(), default = []),
        "strip_all_flags": attrs.option(attrs.list(attrs.arg()), default = None),
        "strip_debug_flags": attrs.option(attrs.list(attrs.arg()), default = None),
        "strip_non_global_flags": attrs.option(attrs.list(attrs.arg()), default = None),
        "sysroot": attrs.option(attrs.source(allow_directory = True), default = None),
        "target": attrs.option(attrs.string(), default = None),
        "use_lld": attrs.bool(default = True),
        "_cxx_internal_tools": attrs.default_only(attrs.dep(providers = [CxxInternalTools], default = "prelude//cxx/tools:internal_tools")),
    },
    is_toolchain_rule = True,
)
