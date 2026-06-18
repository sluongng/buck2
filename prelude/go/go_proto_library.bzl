# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//:paths.bzl", "paths")
load(
    "@prelude//cxx:preprocessor.bzl",
    "cxx_inherited_preprocessor_infos",
    "cxx_merge_cpreprocessors",
)
load("@prelude//decls:toolchains_common.bzl", "toolchains_common")
load(
    "@prelude//linking:link_groups.bzl",
    "merge_link_group_lib_info",
)
load(
    "@prelude//linking:link_info.bzl",
    "LibOutputStyle",
    "LinkInfo",
    "LinkInfos",
    "MergedLinkInfo",
    "create_merged_link_info_for_propagation",
)
load(
    "@prelude//linking:linkable_graph.bzl",
    "create_linkable_graph",
    "create_linkable_graph_node",
    "create_linkable_node",
)
load(
    "@prelude//linking:shared_libraries.bzl",
    "SharedLibraryInfo",
    "merge_shared_libraries",
)
load(
    "@prelude//utils:utils.bzl",
    "map_idx",
)
load(":compile.bzl", "GoPkgCompileInfo", "GoTestInfo")
load(":coverage.bzl", "GoCoverageMode")
load(":go_proto_toolchain.bzl", "GoProtoCompilerInfo", "GoProtoToolchainInfo", _go_proto_compiler = "go_proto_compiler")
load(":link.bzl", "GoPkgLinkInfo", "get_inherited_link_pkgs")
load(":package_builder.bzl", "GoBuildConfig", "GoSourceInputs", "declare_package_build")
load(":packages.bzl", "merge_pkgs")
load(":toolchain.bzl", "GoToolchainInfo", "get_toolchain_env_vars")

go_proto_compiler = _go_proto_compiler

def _plugin_flag(compiler: typing.Any) -> cmd_args:
    return cmd_args("--plugin=protoc-gen-{}=".format(compiler.plugin_name), compiler.plugin, delimiter = "")

def _mapping_flags(prefix: str, mappings: dict[str, str]) -> list[cmd_args]:
    flags = []
    for proto_path, import_path in mappings.items():
        flags.append(cmd_args("--{}_opt=M".format(prefix), proto_path, "=", import_path, delimiter = ""))
    return flags

def _generated_go_srcs(gen_dir: Artifact, src_paths: list[str], suffix: str) -> list[Artifact]:
    generated = []
    for src_path in src_paths:
        generated.append(gen_dir.project(paths.replace_extension(src_path, suffix)))
    return generated

def _proto_paths(name: str, path_attr: list[str], srcs: list[Artifact]) -> list[str]:
    if path_attr:
        if len(path_attr) != len(srcs):
            fail("go_proto_library {} length ({}) must match srcs length ({})".format(name, len(path_attr), len(srcs)))
        return path_attr
    return [src.short_path for src in srcs]

def _include_dir(ctx: AnalysisContext, src_paths: list[str], proto_src_paths: list[str]) -> Artifact:
    links = {}
    for idx, src in enumerate(ctx.attrs.srcs):
        if src_paths[idx] in links and links[src_paths[idx]] != src:
            fail("go_proto_library has multiple artifacts for proto import path {}".format(src_paths[idx]))
        links[src_paths[idx]] = src
    for idx, src in enumerate(ctx.attrs.proto_srcs):
        if proto_src_paths[idx] in links and links[proto_src_paths[idx]] != src:
            fail("go_proto_library has multiple artifacts for proto import path {}".format(proto_src_paths[idx]))
        links[proto_src_paths[idx]] = src
    return ctx.actions.symlinked_dir("proto_includes", links, has_content_based_path = True)

def _output_paths(paths: list[str], src_paths: list[str]) -> list[str]:
    return paths if paths else src_paths

def _legacy_compiler(plugin_name: str, dep: [Dependency, None], suffix: str, options: list[str] = []) -> typing.Any:
    if dep == None:
        fail("go_proto_library requires protoc_gen_{} when no compilers are provided".format(plugin_name))
    return struct(
        always_generates = False,
        deps = [],
        import_path_option = False,
        options = options,
        plugin = dep[RunInfo],
        plugin_name = plugin_name,
        suffix = suffix,
        suffixes = [],
        valid_archive = True,
    )

def _compiler_specs(ctx: AnalysisContext) -> list[typing.Any]:
    if ctx.attrs.compilers:
        return [compiler[GoProtoCompilerInfo] for compiler in ctx.attrs.compilers]

    compilers = [
        _legacy_compiler("go", ctx.attrs.protoc_gen_go, ".pb.go"),
    ]
    if ctx.attrs.grpc:
        compilers.append(_legacy_compiler(
            "go-grpc",
            ctx.attrs.protoc_gen_go_grpc,
            "_grpc.pb.go",
            options = ["require_unimplemented_servers=false"],
        ))
    if ctx.attrs.vtproto:
        compilers.append(_legacy_compiler(
            "go-vtproto",
            ctx.attrs.protoc_gen_go_vtproto,
            "_vtproto.pb.go",
            options = ["features=marshal+unmarshal+size+pool+clone"],
        ))
    return compilers

def _compiler_src_paths(ctx: AnalysisContext, compiler: typing.Any, src_paths: list[str]) -> list[str]:
    if compiler.plugin_name == "go-grpc":
        return _output_paths(ctx.attrs.grpc_src_paths, src_paths)
    if compiler.plugin_name == "go-vtproto":
        return _output_paths(ctx.attrs.vtproto_src_paths, src_paths)
    return src_paths

def _compiler_suffixes(compiler: typing.Any) -> list[str]:
    return compiler.suffixes if compiler.suffixes else [compiler.suffix]

def _protoc(ctx: AnalysisContext) -> cmd_args:
    if ctx.attrs.protoc != None:
        return cmd_args(ctx.attrs.protoc[RunInfo])
    if ctx.attrs.go_proto_toolchain != None:
        return cmd_args(ctx.attrs.go_proto_toolchain[GoProtoToolchainInfo].protoc)
    if ctx.attrs._go_proto_toolchain != None:
        return cmd_args(ctx.attrs._go_proto_toolchain[GoProtoToolchainInfo].protoc)
    return cmd_args("protoc")

def _proto_include(ctx: AnalysisContext) -> [Artifact, None]:
    if ctx.attrs.go_proto_toolchain != None:
        return ctx.attrs.go_proto_toolchain[GoProtoToolchainInfo].include
    if ctx.attrs._go_proto_toolchain != None:
        return ctx.attrs._go_proto_toolchain[GoProtoToolchainInfo].include
    return None

def _compiler_deps(compilers: list[typing.Any]) -> list[Dependency]:
    deps = []
    for compiler in compilers:
        deps.extend(compiler.deps)
    return deps

def _go_proto_library_impl(ctx: AnalysisContext) -> list[Provider]:
    go_toolchain = ctx.attrs._go_toolchain[GoToolchainInfo]
    gen_dir = ctx.actions.declare_output("go_proto_srcs", dir = True, has_content_based_path = True)
    if len(ctx.attrs.srcs) == 0:
        fail("go_proto_library requires at least one src")
    src_paths = _proto_paths("src_paths", ctx.attrs.src_paths, ctx.attrs.srcs)
    proto_src_paths = _proto_paths("proto_src_paths", ctx.attrs.proto_src_paths, ctx.attrs.proto_srcs)
    include_dir = _include_dir(ctx, src_paths, proto_src_paths)
    compilers = _compiler_specs(ctx)

    args = cmd_args(hidden = [include_dir])
    args.add(["-I", include_dir])
    proto_include = _proto_include(ctx)
    if proto_include != None:
        args.add(["-I", proto_include])
    generated_srcs = []
    for compiler in compilers:
        args.add(_plugin_flag(compiler))
        args.add(cmd_args("--{}_out=".format(compiler.plugin_name), gen_dir.as_output(), delimiter = ""))
        args.add("--{}_opt=paths=source_relative".format(compiler.plugin_name))
        for option in compiler.options:
            args.add("--{}_opt={}".format(compiler.plugin_name, option))
        if compiler.import_path_option:
            args.add("--{}_opt=import_path={}".format(compiler.plugin_name, ctx.attrs.import_path))
        args.add(_mapping_flags(compiler.plugin_name, ctx.attrs.import_mappings))
        for src_path in src_paths:
            args.add(cmd_args("--{}_opt=M".format(compiler.plugin_name), src_path, "=", ctx.attrs.import_path, delimiter = ""))
        compiler_src_paths = _compiler_src_paths(ctx, compiler, src_paths)
        for suffix in _compiler_suffixes(compiler):
            generated_srcs.extend(_generated_go_srcs(gen_dir, compiler_src_paths, suffix))

    args.add(src_paths)
    cmd = cmd_args([
        "sh",
        "-c",
        "mkdir -p \"$1\" && shift && exec \"$@\"",
        "sh",
        gen_dir.as_output(),
        _protoc(ctx),
        args,
    ])
    ctx.actions.run(cmd, env = get_toolchain_env_vars(go_toolchain), category = "go_proto", identifier = ctx.attrs.import_path)

    package_root = paths.dirname(generated_srcs[0].short_path)
    all_deps = ctx.attrs.deps + _compiler_deps(compilers)
    pkg, pkg_info, _ = declare_package_build(
        ctx = ctx,
        pkg_import_path = ctx.attrs.import_path,
        main = False,
        sources = GoSourceInputs(
            srcs = generated_srcs,
            embed_srcs = {},
            package_root = package_root,
        ),
        cgo_build_context = None,
        config = GoBuildConfig(
            compiler_flags = [],
            assembler_flags = [],
            build_tags = ctx.attrs._build_tags,
            coverage_enabled = False,
            coverage_mode = GoCoverageMode(ctx.attrs._coverage_mode) if ctx.attrs._coverage_mode else None,
            cgo_enabled = False,
        ),
        deps = all_deps,
    )

    pkgs = {
        ctx.attrs.import_path: pkg,
    }
    return [
        DefaultInfo(default_output = pkg.archive_file),
        GoPkgCompileInfo(pkgs = pkgs),
        GoPkgLinkInfo(pkgs = merge_pkgs([
            pkgs,
            get_inherited_link_pkgs(all_deps),
        ])),
        GoTestInfo(
            deps = all_deps,
            srcs = generated_srcs,
            pkg_import_path = ctx.attrs.import_path,
            coverage_enabled = False,
        ),
        create_merged_link_info_for_propagation(ctx, filter(None, [d.get(MergedLinkInfo) for d in all_deps])),
        merge_shared_libraries(
            ctx.actions,
            deps = filter(None, map_idx(SharedLibraryInfo, all_deps)),
        ),
        merge_link_group_lib_info(deps = all_deps),
        create_linkable_graph(
            ctx,
            node = create_linkable_graph_node(
                ctx,
                linkable_node = create_linkable_node(
                    ctx,
                    default_soname = None,
                    exported_deps = all_deps,
                    link_infos = _get_empty_link_infos(),
                ),
            ),
            deps = all_deps,
        ),
        cxx_merge_cpreprocessors(ctx.actions, [], cxx_inherited_preprocessor_infos(all_deps)),
        pkg_info,
    ]

def _get_empty_link_infos() -> dict[LibOutputStyle, LinkInfos]:
    infos = {}
    for output_style in LibOutputStyle:
        infos[output_style] = LinkInfos(default = LinkInfo())
    return infos

go_proto_library = rule(
    impl = _go_proto_library_impl,
    attrs = {
        "deps": attrs.list(attrs.dep(), default = []),
        "compilers": attrs.list(attrs.dep(providers = [GoProtoCompilerInfo]), default = []),
        "grpc": attrs.bool(default = False),
        "grpc_src_paths": attrs.list(attrs.string(), default = []),
        "go_proto_toolchain": attrs.option(attrs.dep(providers = [GoProtoToolchainInfo]), default = None),
        "import_mappings": attrs.dict(attrs.string(), attrs.string(), default = {}),
        "import_path": attrs.string(),
        "labels": attrs.list(attrs.string(), default = []),
        "proto_src_paths": attrs.list(attrs.string(), default = []),
        "proto_srcs": attrs.list(attrs.source(), default = []),
        "protoc": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "protoc_gen_go": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "protoc_gen_go_grpc": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "protoc_gen_go_vtproto": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "src_paths": attrs.list(attrs.string(), default = []),
        "srcs": attrs.list(attrs.source(), default = []),
        "vtproto": attrs.bool(default = False),
        "vtproto_src_paths": attrs.list(attrs.string(), default = []),
        "_build_tags": attrs.default_only(attrs.list(attrs.string(), default = [])),
        "_coverage_mode": attrs.default_only(attrs.option(attrs.string(), default = None)),
        "_go_proto_toolchain": attrs.default_only(toolchains_common.go_proto()),
        "_go_stdlib": attrs.default_only(attrs.dep(default = "prelude//go/tools:stdlib")),
        "_go_toolchain": toolchains_common.go(),
    },
)
