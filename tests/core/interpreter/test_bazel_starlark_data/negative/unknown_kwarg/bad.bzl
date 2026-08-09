def declare_bad():
    native.genrule(
        name = "bad",
        outs = ["bad.txt"],
        cmd = "touch $@",
        unknown = True,
    )
