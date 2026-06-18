# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

RulesCcToolchainMetadataInfo = provider(
    fields = {
        "kind": provider_field(str),
    },
)

def _metadata_impl(ctx: AnalysisContext) -> list[Provider]:
    return [
        DefaultInfo(),
        RulesCcToolchainMetadataInfo(kind = ctx.attrs.kind),
    ]

_COMMON_ATTRS = {
    "actions": attrs.any(default = []),
    "allowlist_include_directories": attrs.any(default = []),
    "all_of": attrs.any(default = []),
    "any_of": attrs.any(default = []),
    "args": attrs.any(default = []),
    "artifact_name_patterns": attrs.any(default = []),
    "compiler": attrs.option(attrs.string(), default = None),
    "data": attrs.any(default = []),
    "dynamic_runtime_lib": attrs.any(default = None),
    "enabled_features": attrs.any(default = []),
    "env": attrs.any(default = {}),
    "feature_name": attrs.option(attrs.string(), default = None),
    "format": attrs.any(default = {}),
    "implies": attrs.any(default = []),
    "known_features": attrs.any(default = []),
    "module_map": attrs.any(default = None),
    "none_of": attrs.any(default = []),
    "overrides": attrs.any(default = None),
    "requires_any_of": attrs.any(default = []),
    "requires_none_of": attrs.any(default = []),
    "requires_not_none": attrs.any(default = None),
    "requires_true": attrs.any(default = None),
    "src": attrs.any(default = None),
    "static_runtime_lib": attrs.any(default = None),
    "supports_header_parsing": attrs.bool(default = False),
    "supports_param_files": attrs.bool(default = False),
    "tool": attrs.any(default = None),
    "tool_map": attrs.any(default = []),
    "tools": attrs.any(default = []),
    "variables": attrs.any(default = []),
}

def _stub_rule(kind):
    attrs_dict = dict(_COMMON_ATTRS)
    attrs_dict["kind"] = attrs.string(default = kind)
    return rule(
        impl = _metadata_impl,
        attrs = attrs_dict,
    )

cc_args = _stub_rule("cc_args")
cc_args_list = _stub_rule("cc_args_list")
cc_feature = _stub_rule("cc_feature")
cc_feature_set = _stub_rule("cc_feature_set")
cc_tool = _stub_rule("cc_tool")
cc_tool_map = _stub_rule("cc_tool_map")
cc_toolchain = _stub_rule("cc_toolchain")
