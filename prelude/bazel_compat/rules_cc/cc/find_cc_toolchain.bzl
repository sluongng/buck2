# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

CC_TOOLCHAIN_TYPE = "@rules_cc//cc:toolchain_type"

def find_cc_toolchain(_ctx):
    fail("rules_cc find_cc_toolchain is not implemented in Buck2's Bazel compatibility layer yet; use a Buck cxx toolchain dependency instead")

def use_cc_toolchain():
    return []
