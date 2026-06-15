# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _producer_impl(ctx):
    out = ctx.actions.declare_output("producer.txt", has_content_based_path = False)
    ctx.actions.run(
        [
            "/bin/sh",
            "-c",
            "printf 'generated input for action rewind\\n' > \"$1\"",
            "--",
            out.as_output(),
        ],
        category = "produce",
    )
    return [DefaultInfo(default_output = out)]

producer = rule(
    impl = _producer_impl,
    attrs = {},
)

def _consumer_impl(ctx):
    inp = ctx.attrs.dep[DefaultInfo].default_outputs[0]
    out = ctx.actions.declare_output("consumer.txt", has_content_based_path = False)
    ctx.actions.run(
        [
            "/bin/sh",
            "-c",
            "printf 'consumer saw: ' > \"$2\"; cat \"$1\" >> \"$2\"",
            "--",
            inp,
            out.as_output(),
        ],
        category = "consume",
    )
    return [DefaultInfo(default_output = out)]

consumer = rule(
    impl = _consumer_impl,
    attrs = {
        "dep": attrs.dep(),
    },
)
