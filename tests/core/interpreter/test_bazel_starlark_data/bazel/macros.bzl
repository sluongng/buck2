load(":defs.bzl", "declare_genrule")


def bazel_genrule(name, outs, cmd, srcs = None):
    declare_genrule(
        name = name,
        outs = outs,
        cmd = cmd,
        srcs = srcs,
    )
