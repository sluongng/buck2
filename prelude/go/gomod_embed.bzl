# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _gomod_embed_file_impl(ctx: AnalysisContext) -> list[Provider]:
    out = ctx.actions.write(ctx.attrs.out, ctx.attrs.content)
    return [DefaultInfo(default_output = out)]

gomod_embed_file = rule(
    impl = _gomod_embed_file_impl,
    attrs = {
        "content": attrs.string(default = "unknown\n"),
        "out": attrs.string(),
    },
)
