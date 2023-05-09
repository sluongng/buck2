# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under both the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree and the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree.

load(
    "@prelude//utils:utils.bzl",
    "expect",
)
load("@prelude//decls:go_common.bzl", "go_common")
load("@prelude//decls/toolchains_common.bzl", "toolchains_common")
load(":compile.bzl", "compile", "get_filtered_srcs")
load(":link.bzl", "link")
load(":toolchain.bzl", "GoToolchainInfo")

def go_binary_impl(ctx: "context") -> ["provider"]:
    lib = compile(
        ctx,
        "main",
        get_filtered_srcs(ctx, ctx.attrs.srcs),
        deps = ctx.attrs.deps,
        compile_flags = ctx.attrs.compiler_flags,
    )
    (bin, runtime_files) = link(ctx, lib, deps = ctx.attrs.deps, link_mode = ctx.attrs.link_mode)

    hidden = []
    for resource in ctx.attrs.resources:
        if type(resource) == "artifact":
            hidden.append(resource)
        else:
            # Otherwise, this is a dependency, so extract the resource and other
            # resources from the `DefaultInfo` provider.
            info = resource[DefaultInfo]
            expect(
                len(info.default_outputs) == 1,
                "expected exactly one default output from {} ({})"
                    .format(resource, info.default_outputs),
            )
            [resource] = info.default_outputs
            other = info.other_outputs

            hidden.append(resource)
            hidden.extend(other)

    return [
        DefaultInfo(
            default_output = bin,
            other_outputs = hidden + runtime_files,
        ),
        RunInfo(args = cmd_args(bin).hidden(hidden + runtime_files)),
    ]

def go_tool_binary_impl(ctx: "context") -> ["provider"]:
    go_toolchain = ctx.attrs._go_toolchain[GoToolchainInfo]
    out = ctx.actions.declare_output("main")

    build_script, _ = ctx.actions.write(
        "build.sh",
        [
            cmd_args(['GOCACHE="$(mktemp -d)"']),
            cmd_args(["trap", '"rm -rf $GOCACHE"', "EXIT"], delimiter = " "),
            cmd_args([go_toolchain.go, "build", "-o", out.as_output(), "-trimpath"] + ctx.attrs.srcs, delimiter = " "),
        ],
        is_executable = True,
        allow_args = True,
    )
    ctx.actions.run(
        cmd_args(["/bin/sh", build_script])
            .hidden(go_toolchain.go, out.as_output(), ctx.attrs.srcs),
        category = "go_tool_binary",
    )

    return [
        DefaultInfo(default_output = out),
        RunInfo(args = cmd_args(out)),
    ]

go_tool_binary = rule(
    impl = go_tool_binary_impl,
    attrs = {
        "_go_toolchain": toolchains_common.go(),
    } | go_common.srcs_arg(),
)
