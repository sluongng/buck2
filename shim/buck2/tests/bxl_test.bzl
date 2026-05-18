# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@fbcode//buck2/tests:buck_e2e.bzl", "buck2_e2e_test")
load("@fbcode_macros//build_defs:export_files.bzl", "export_file")

def bxl_test(src, name = None, labels = None, buck_args = None, bxl_args = None, env = None, **kwargs):
    if ":" in src:
        fail("src cannot be a target. Found {} for src".format(src))
    if not src.endswith(".bxl"):
        fail("src must end in .bxl. Found {} for src".format(src))

    name = name or src
    export_file_name = "{}.{}.export_file".format(src, name)
    export_file(name = export_file_name, src = src, mode = "reference")

    merged_env = dict(env or {})
    merged_env["BXL_MAIN"] = "{}//{}/{}:test".format(native.get_cell_name(), native.package_name(), src)
    merged_env["_BXL_SRC"] = "$(location :{})".format(export_file_name)
    if buck_args:
        merged_env["BUCK_ARGS"] = " ".join(buck_args)
    if bxl_args:
        merged_env["BXL_ARGS"] = " ".join(bxl_args)

    buck2_e2e_test(
        name = name,
        env = merged_env,
        labels = ["bxl_test"] + (labels or []),
        srcs = {
            "test_bxl_template.py": "root//tests/e2e_util:test_bxl_template.py",
        },
        test_with_compiled_buck2 = True,
        test_with_deployed_buck2 = False,
        **kwargs
    )
