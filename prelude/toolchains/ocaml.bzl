# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load(
    "@prelude//ocaml:ocaml_toolchain_types.bzl",
    "OCamlPlatformInfo",
    "OCamlToolchainInfo",
)

def _optional_run_info(value):
    return value[RunInfo] if value != None else None

def _ocaml_toolchain_impl(ctx):
    return [
        DefaultInfo(),
        OCamlToolchainInfo(
            ocaml_compiler = ctx.attrs.ocaml_compiler[RunInfo],
            binutils_ld = _optional_run_info(ctx.attrs.binutils_ld),
            binutils_as = _optional_run_info(ctx.attrs.binutils_as),
            dep_tool = ctx.attrs.dep_tool[RunInfo],
            yacc_compiler = ctx.attrs.yacc_compiler[RunInfo],
            interop_includes = None,
            menhir_compiler = ctx.attrs.menhir_compiler[RunInfo],
            lex_compiler = ctx.attrs.lex_compiler[RunInfo],
            libc = None,
            ocaml_bytecode_compiler = ctx.attrs.ocaml_bytecode_compiler[RunInfo],
            debug = _optional_run_info(ctx.attrs.debug),
            warnings_flags = ctx.attrs.warnings_flags,
            ocaml_compiler_flags = ctx.attrs.ocaml_compiler_flags,
            ocamlc_flags = ctx.attrs.ocamlc_flags,
            ocamlopt_flags = ctx.attrs.ocamlopt_flags,
            runtime_dep_link_flags = ctx.attrs.runtime_dep_link_flags,
            runtime_dep_link_extras = ctx.attrs.runtime_dep_link_extras,
        ),
        OCamlPlatformInfo(name = ctx.attrs.platform_name),
    ]

def _system_ocaml_toolchain_impl(_ctx):
    """
    A very simple toolchain that is hardcoded to the current environment.
    """

    return [
        DefaultInfo(),
        OCamlToolchainInfo(
            ocaml_compiler = RunInfo(args = ["ocamlopt.opt"]),
            # "Partial linking" (via `ocamlopt.opt -output-obj`) emits calls to
            # `ld -r -o`. If not `None`, this is the `ld` that will be invoked;
            # the default is to use whatever `ld` is in the environment. See
            # [Note: What is `binutils_ld`?] in `providers.bzl`.
            binutils_ld = None,
            # `ocamlopt.opt` makes calls to `as`. If this config parameter is
            # `None` those calls will resolve to whatever `as` is in the
            # environment. If not `None` then the provided value will be what's
            # invoked.
            binutils_as = None,
            dep_tool = RunInfo(args = ["ocamldep.opt"]),
            yacc_compiler = RunInfo(args = ["ocamlyacc"]),
            interop_includes = None,
            menhir_compiler = RunInfo(args = ["menhir"]),
            lex_compiler = RunInfo(args = ["ocamllex.opt"]),
            libc = None,
            ocaml_bytecode_compiler = RunInfo(args = ["ocamlc.opt"]),
            # `ocamldebug` is bytecode intended to be run by `ocamlrun`. There
            # is no "debugger" executable (but then `debug` is not referenced by
            # the ocaml build rules) so `None` will do for this.
            debug = None,
            warnings_flags = "-4-29-35-41-42-44-45-48-50-58-70",
            ocaml_compiler_flags = [],  # e.g. "-opaque"
            ocamlc_flags = [],
            ocamlopt_flags = [],
            # We don't expect /opt/homebrew/lib to exist on Linux but that's not
            # a problem. On macOS (aarch64 at least) we expect zstd to live in
            # /opt/homebrew/lib.
            runtime_dep_link_flags = ["-ldl", "-lpthread", "-L/opt/homebrew/lib", "-lzstd"],
            runtime_dep_link_extras = [],
        ),
        OCamlPlatformInfo(name = "x86_64"),
    ]

system_ocaml_toolchain = rule(
    impl = _system_ocaml_toolchain_impl,
    attrs = {},
    is_toolchain_rule = True,
)

ocaml_toolchain = rule(
    impl = _ocaml_toolchain_impl,
    attrs = {
        "binutils_as": attrs.option(attrs.dep(providers = [RunInfo]), default = None),
        "binutils_ld": attrs.option(attrs.dep(providers = [RunInfo]), default = None),
        "debug": attrs.option(attrs.dep(providers = [RunInfo]), default = None),
        "dep_tool": attrs.dep(providers = [RunInfo]),
        "lex_compiler": attrs.dep(providers = [RunInfo]),
        "menhir_compiler": attrs.dep(providers = [RunInfo]),
        "ocaml_bytecode_compiler": attrs.dep(providers = [RunInfo]),
        "ocaml_compiler": attrs.dep(providers = [RunInfo]),
        "ocaml_compiler_flags": attrs.list(attrs.arg(), default = []),
        "ocamlc_flags": attrs.list(attrs.arg(), default = []),
        "ocamlopt_flags": attrs.list(attrs.arg(), default = []),
        "platform_name": attrs.string(default = "x86_64"),
        "runtime_dep_link_extras": attrs.list(attrs.arg(), default = []),
        "runtime_dep_link_flags": attrs.list(attrs.arg(), default = ["-ldl", "-lpthread", "-lzstd"]),
        "warnings_flags": attrs.string(default = "-4-29-35-41-42-44-45-48-50-58-70"),
        "yacc_compiler": attrs.dep(providers = [RunInfo]),
    },
    is_toolchain_rule = True,
)
