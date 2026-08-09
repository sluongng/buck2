def buck_message(prefix: str) -> str:
    return f"{prefix}"


def _buck_write_impl(ctx):
    output = ctx.actions.write(
        "buck.txt",
        ctx.attrs.content,
        has_content_based_path = False,
    )
    return [DefaultInfo(default_output = output)]


buck_write = rule(
    impl = _buck_write_impl,
    attrs = {"content": attrs.string()},
)
