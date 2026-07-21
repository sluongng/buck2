# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@fbcode//buck2/tests:buck_e2e.bzl", "buck2_e2e_test")

def _fix_label(label):
    return label.replace("fbcode//buck2/", "root//").replace("//buck2/", "root//")

def _fix_labels(labels):
    return [_fix_label(label) for label in labels]

def _dependency_test(name, contacts, labels = None, env = None, **kwargs):
    buck2_e2e_test(
        name = name,
        contacts = contacts,
        env = env or {},
        labels = labels or [],
        srcs = {
            "test_bxl_check_dependencies_template.py": "root//tests/e2e_util:test_bxl_check_dependencies_template.py",
        },
        test_with_compiled_buck2 = True,
        test_with_deployed_buck2 = False,
        use_buck_api = False,
        **kwargs
    )

def _fixture_kwargs(fixed_target):
    fixture_prefix = "root//tests/targets/check_dependencies_test:"
    if fixed_target.startswith(fixture_prefix):
        return {
            "data": ":check_dependencies_oss_data",
            "env": {
                "BUCK2_OSS_CHECK_DEPENDENCIES_DATA": "1",
                "BUCK2_OSS_CHECK_DEPENDENCIES_DATA_DIR": "test_check_dependencies_oss_data",
            },
            "target": "root//:" + fixed_target.removeprefix(fixture_prefix),
        }
    return {}

def check_dependencies_test(
        name,
        target,
        contacts,
        mode,
        allowlist_patterns = None,
        blocklist_patterns = None,
        expect_failure_msg = None,
        env = None,
        labels = [],
        target_deps = True,
        deps_filter_pattern = None,
        deps_exclude_pattern = None,
        **kwargs):
    fixed_target = _fix_label(target)
    fixture = _fixture_kwargs(fixed_target)

    _dependency_test(
        name = name,
        contacts = contacts,
        env = {
            "ALLOWLIST": ",".join(allowlist_patterns or []),
            "BXL_MAIN": "root//tests/check_dependencies_test.bxl:test",
            "BLOCKLIST": ",".join(blocklist_patterns or []),
            "DEPS_EXCLUDE_PATTERN": deps_exclude_pattern or "",
            "DEPS_FILTER_PATTERN": deps_filter_pattern or "",
            "EXPECT_FAILURE_MSG": expect_failure_msg or "",
            "CHECK_DEPENDENCIES_TEST_FBCODE_BUILD_MODE": "--config=project.ignore=",
            "FLAVOR": "check_dependencies_test",
            "TARGET": fixture.get("target", fixed_target),
            "TARGET_DEPS": str(target_deps).lower(),
            "VERIFICATION_MODE": mode,
        } | fixture.get("env", {}) | (env or {}),
        labels = ["check_dependencies_test"] + labels,
        data = fixture.get("data"),
        **kwargs
    )

def assert_dependencies_test(name, target, contacts, expected_deps, expect_failure_msg = None, labels = [], **kwargs):
    _dependency_test(
        name = name,
        contacts = contacts,
        env = {
            "BXL_MAIN": "root//tests/assert_dependencies_test.bxl:test",
            "DEPS": ",".join(_fix_labels(expected_deps)),
            "EXPECT_FAILURE_MSG": expect_failure_msg or "",
            "FLAVOR": "assert_dependencies_test",
            "TARGET": _fix_label(target),
        },
        labels = labels + ["assert_dependencies_test"],
        **kwargs
    )

def audit_dependents_test(name, target, contacts, source_target, allowlist_patterns, expect_failure_msg = None, labels = [], **kwargs):
    _dependency_test(
        name = name,
        contacts = contacts,
        env = {
            "ALLOWLIST": ",".join(allowlist_patterns or []),
            "BXL_MAIN": "root//tests/audit_dependents_test.bxl:test",
            "EXPECT_FAILURE_MSG": expect_failure_msg or "",
            "FLAVOR": "audit_dependents_test",
            "SOURCE_TARGET": _fix_label(source_target),
            "TARGET": _fix_label(target),
        },
        labels = labels + ["audit_dependents_test"],
        **kwargs
    )

def check_mutually_exclusive_dependencies_test(
        name,
        target,
        contacts,
        mutually_exclusive_group,
        expect_failure_msg = None,
        labels = [],
        **kwargs):
    _dependency_test(
        name = name,
        contacts = contacts,
        env = {
            "BXL_MAIN": "root//tests/check_mutually_exclusive_dependencies_test.bxl:test",
            "EXPECT_FAILURE_MSG": expect_failure_msg or "",
            "FLAVOR": "check_mutually_exclusive_dependencies_test",
            "MUTUALLY_EXCLUSIVE_GROUP": ",".join(_fix_labels(mutually_exclusive_group)),
            "TARGET": _fix_label(target),
            "TARGET_DEPS": "true",
        },
        labels = labels + ["check_mutually_exclusive_dependencies_test"],
        **kwargs
    )
