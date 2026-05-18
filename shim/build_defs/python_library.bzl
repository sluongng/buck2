# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@shim//build_defs/lib:oss.bzl", "translate_target")

def _fix_label(label):
    if label.startswith("fbcode//buck2/"):
        return "root//" + label.removeprefix("fbcode//buck2/")
    if label.startswith("//buck2/"):
        return "root//" + label.removeprefix("//buck2/")
    return translate_target(label)

def _fix_labels(labels):
    return [_fix_label(label) for label in (labels or [])]

def _base_module_for_package():
    package = native.package_name()
    if package.startswith("tests/"):
        return "buck2." + package.replace("/", ".")
    return None

def python_library(srcs = [], deps = [], visibility = ["PUBLIC"], **kwargs):
    if "base_module" not in kwargs:
        base_module = _base_module_for_package()
        if base_module != None:
            kwargs["base_module"] = base_module

    # @lint-ignore BUCKLINT: avoid "Direct usage of native rules is not allowed."
    native.python_library(
        deps = _fix_labels(deps),
        srcs = srcs,
        visibility = visibility,
        **kwargs
    )
