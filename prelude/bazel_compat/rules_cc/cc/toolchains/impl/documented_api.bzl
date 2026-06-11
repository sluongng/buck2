# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load(
    "@prelude//bazel_compat/rules_cc/cc/toolchains:_stubs.bzl",
    _cc_args = "cc_args",
    _cc_args_list = "cc_args_list",
    _cc_feature = "cc_feature",
    _cc_feature_set = "cc_feature_set",
    _cc_tool = "cc_tool",
    _cc_tool_map = "cc_tool_map",
    _cc_toolchain = "cc_toolchain",
)

cc_args = _cc_args
cc_args_list = _cc_args_list
cc_feature = _cc_feature
cc_feature_set = _cc_feature_set
cc_tool = _cc_tool
cc_tool_map = _cc_tool_map
cc_toolchain = _cc_toolchain
