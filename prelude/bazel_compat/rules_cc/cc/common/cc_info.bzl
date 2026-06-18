# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

CcInfo = provider(
    fields = {
        "compilation_context": provider_field(typing.Any, default = None),
        "debug_context": provider_field(typing.Any, default = None),
        "linking_context": provider_field(typing.Any, default = None),
    },
)

CcToolchainInfo = provider(
    fields = {
        "all_files": provider_field(typing.Any, default = None),
        "compiler": provider_field(typing.Any, default = None),
        "target_cpu": provider_field(typing.Any, default = None),
        "toolchain_id": provider_field(typing.Any, default = None),
    },
)
