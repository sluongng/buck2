def declare_genrule(name, outs, cmd, srcs = None):
    if srcs == None:
        native.genrule(
            name = name,
            outs = outs,
            cmd = cmd,
        )
    else:
        native.genrule(
            name = name,
            outs = outs,
            cmd = cmd,
            srcs = srcs,
        )
