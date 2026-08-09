def declare_bad():
    native.genrule(
        name = "bad",
        outs = ["one.txt", "two.txt"],
        cmd = "touch one.txt two.txt",
    )
