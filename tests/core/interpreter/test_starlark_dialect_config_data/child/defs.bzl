def target_name(prefix: str) -> str:
    return f"{prefix}_ok"

simple = rule(impl = lambda _ctx: [DefaultInfo()], attrs = {})
