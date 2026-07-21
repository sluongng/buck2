# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load(
    ":find_cc_toolchain.bzl",
    _CC_TOOLCHAIN_TYPE = "CC_TOOLCHAIN_TYPE",
    _find_cc_toolchain = "find_cc_toolchain",
    _use_cc_toolchain = "use_cc_toolchain",
)

CC_TOOLCHAIN_TYPE = _CC_TOOLCHAIN_TYPE
find_cc_toolchain = _find_cc_toolchain
use_cc_toolchain = _use_cc_toolchain
