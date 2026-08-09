genrule(
    name = "bad",
    outs = ["bad.txt"],
    cmd = "touch $@",
)

bad = True
