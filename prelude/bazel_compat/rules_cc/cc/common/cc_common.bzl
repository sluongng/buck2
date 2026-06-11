# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load(":cc_info.bzl", "CcInfo")

def _empty_compilation_context(**_kwargs):
    return struct()

def _empty_linking_context(**_kwargs):
    return struct()

def _merge_cc_infos(cc_infos = [], **_kwargs):
    if len(cc_infos) == 1:
        return cc_infos[0]
    return CcInfo(
        compilation_context = struct(cc_infos = cc_infos),
        linking_context = struct(cc_infos = cc_infos),
    )

def _unsupported(name):
    def inner(*_args, **_kwargs):
        fail("rules_cc cc_common.{} is not implemented in Buck2's Bazel compatibility layer yet".format(name))

    return inner

cc_common = struct(
    action_is_enabled = _unsupported("action_is_enabled"),
    compile = _unsupported("compile"),
    configure_features = _unsupported("configure_features"),
    create_compilation_context = _empty_compilation_context,
    create_linking_context = _empty_linking_context,
    create_linking_context_from_compilation_outputs = _unsupported("create_linking_context_from_compilation_outputs"),
    get_memory_inefficient_command_line = _unsupported("get_memory_inefficient_command_line"),
    get_tool_for_action = _unsupported("get_tool_for_action"),
    is_enabled = _unsupported("is_enabled"),
    link = _unsupported("link"),
    merge_cc_infos = _merge_cc_infos,
)
