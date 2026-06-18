# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""Small Bazel rules_cc facade over Buck's C++ rules.

This is deliberately a BUILD-rule compatibility layer. It does not try to make
Buck targets provide Bazel's full CcInfo graph yet.
"""

load("@prelude//:native.bzl", _native = "native")
load(
    "@prelude//bazel_compat/rules_cc/cc/common:cc_common.bzl",
    _cc_common = "cc_common",
)
load(
    "@prelude//bazel_compat/rules_cc/cc/common:cc_info.bzl",
    _CcInfo = "CcInfo",
    _CcToolchainInfo = "CcToolchainInfo",
)

cc_common = _cc_common
CcInfo = _CcInfo
CcToolchainInfo = _CcToolchainInfo

def _as_list(value):
    if value == None:
        return []
    return value

def _define_flags(defines):
    flags = []
    for define in _as_list(defines):
        flags.append("-D" + define)
    return flags

def _include_flags(flag, includes):
    flags = []
    for include in _as_list(includes):
        flags.extend([flag, include])
    return flags

def _fail_if_set(attr, value):
    if value != None and value != [] and value != {} and value != False:
        fail("rules_cc compatibility does not support `{}` yet".format(attr))

def _linkage_from_linkstatic(linkstatic):
    if linkstatic == None:
        return None
    return "static" if linkstatic else "shared"

def _link_style_from_linkstatic(linkstatic):
    if linkstatic == None:
        return None
    return "static" if linkstatic else "shared"

def _clean_kwargs(kwargs):
    return {k: v for k, v in kwargs.items() if v != None}

def cc_library(
        name,
        srcs = [],
        hdrs = [],
        textual_hdrs = [],
        deps = [],
        implementation_deps = [],
        data = [],
        copts = [],
        defines = [],
        local_defines = [],
        includes = [],
        quote_includes = [],
        system_includes = [],
        linkopts = [],
        linkstatic = None,
        alwayslink = False,
        strip_include_prefix = None,
        include_prefix = None,
        **kwargs):
    _fail_if_set("data", data)
    _fail_if_set("strip_include_prefix", strip_include_prefix)
    _fail_if_set("include_prefix", include_prefix)

    _native.cxx_library(**_clean_kwargs(dict(
        name = name,
        srcs = srcs,
        exported_headers = hdrs + textual_hdrs,
        deps = deps + implementation_deps,
        compiler_flags = copts,
        preprocessor_flags = _define_flags(local_defines),
        exported_preprocessor_flags = _define_flags(defines) + _include_flags("-iquote", quote_includes),
        public_include_directories = includes,
        public_system_include_directories = system_includes,
        exported_linker_flags = linkopts,
        link_whole = alwayslink if alwayslink else None,
        preferred_linkage = _linkage_from_linkstatic(linkstatic),
        **kwargs
    )))

def cc_binary(
        name,
        srcs = [],
        deps = [],
        data = [],
        copts = [],
        defines = [],
        linkopts = [],
        linkstatic = None,
        malloc = None,
        stamp = None,
        **kwargs):
    _fail_if_set("malloc", malloc)
    _fail_if_set("stamp", stamp)

    _native.cxx_binary(**_clean_kwargs(dict(
        name = name,
        srcs = srcs,
        deps = deps,
        resources = data,
        compiler_flags = copts,
        preprocessor_flags = _define_flags(defines),
        linker_flags = linkopts,
        link_style = _link_style_from_linkstatic(linkstatic),
        **kwargs
    )))

def cc_test(
        name,
        srcs = [],
        deps = [],
        data = [],
        copts = [],
        defines = [],
        linkopts = [],
        args = [],
        env = {},
        **kwargs):
    _native.cxx_test(**_clean_kwargs(dict(
        name = name,
        srcs = srcs,
        deps = deps,
        resources = data,
        compiler_flags = copts,
        preprocessor_flags = _define_flags(defines),
        linker_flags = linkopts,
        args = args,
        env = env,
        **kwargs
    )))

def cc_import(
        name,
        hdrs = [],
        includes = [],
        static_library = None,
        shared_library = None,
        interface_library = None,
        system_provided = False,
        deps = [],
        alwayslink = False,
        **kwargs):
    _fail_if_set("interface_library", interface_library)
    _fail_if_set("alwayslink", alwayslink)
    if system_provided and shared_library != None:
        fail("rules_cc compatibility cannot model system_provided with shared_library yet")

    _native.prebuilt_cxx_library(**_clean_kwargs(dict(
        name = name,
        exported_headers = hdrs,
        header_dirs = includes,
        static_lib = static_library,
        shared_lib = shared_library,
        exported_deps = deps,
        **kwargs
    )))

def fdo_prefetch_hints(**_kwargs):
    fail("rules_cc fdo_prefetch_hints is not implemented in Buck2's Bazel compatibility layer yet")

def fdo_profile(**_kwargs):
    fail("rules_cc fdo_profile is not implemented in Buck2's Bazel compatibility layer yet")

def memprof_profile(**_kwargs):
    fail("rules_cc memprof_profile is not implemented in Buck2's Bazel compatibility layer yet")

def propeller_optimize(**_kwargs):
    fail("rules_cc propeller_optimize is not implemented in Buck2's Bazel compatibility layer yet")
