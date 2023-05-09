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
    gocache = ctx.actions.declare_output("gocache", dir = True)

    build_script = ctx.actions.write(
        "build.sh",
        [
            cmd_args(["export ", 'GOCACHE="${PWD}/', gocache.as_output(), '"'], delimiter = ""),
            cmd_args(["mkdir", "-p", "$GOCACHE"], delimiter = " "),
            cmd_args([go_toolchain.go, "build", "-x", "-o", out.as_output(), "-trimpath"] + ctx.attrs.srcs, delimiter = " "),
        ],
        is_executable = True,
    )
    ctx.actions.run(
        cmd_args(["/bin/sh", build_script])
            .hidden(gocache.as_output(), go_toolchain.go, out.as_output(), ctx.attrs.srcs),
        category = "go_tool_binary",
        no_outputs_cleanup = True,  # Preserve GOCACHE for subsequent runs
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
