def declare_bad():
    native.genrule(
        name = "bad",
        outs = [],
        cmd = "true",
    )
