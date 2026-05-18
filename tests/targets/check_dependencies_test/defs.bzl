def _impl(_ctx):
    return [DefaultInfo()]

dependency_fixture_rule = rule(
    impl = _impl,
    attrs = {
        "deps": attrs.list(attrs.dep(), default = []),
    },
)
