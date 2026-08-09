/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use super::Fixture;
use super::FixtureKind;
use super::assert_fixtures;

const RULES_CC_0_2_22_CPP_FIXTURES: &[Fixture] = &[
    Fixture {
        module: "rules_cc",
        version: "0.2.22",
        path: "cc/cc_binary.bzl",
        kind: FixtureKind::Bzl,
        sha256: "8dcd829755abc3feaaf32bf3c8cda0904b8c1b5d350060e768054eb60c22f523",
        source: include_str!("../../testcases/bcr/rules_cc/0.2.22/cpp/source/cc/cc_binary.bzl"),
    },
    Fixture {
        module: "rules_cc",
        version: "0.2.22",
        path: "cc/private/link/cpp_link_action.bzl",
        kind: FixtureKind::Bzl,
        sha256: "a8eac69bb859c5aa9354e42b0a126cdfbe051c464938ba6d70a3cdeca278dbc5",
        source: include_str!(
            "../../testcases/bcr/rules_cc/0.2.22/cpp/source/cc/private/link/cpp_link_action.bzl"
        ),
    },
    Fixture {
        module: "rules_cc",
        version: "0.2.22",
        path: "cc/private/toolchain_config/configure_features.bzl",
        kind: FixtureKind::Bzl,
        sha256: "89c22b65f459cf7de09563836ae30c3d89228445a12031fa80855f487be602c5",
        source: include_str!(
            "../../testcases/bcr/rules_cc/0.2.22/cpp/source/cc/private/toolchain_config/configure_features.bzl"
        ),
    },
];

#[test]
fn latest_rules_cc_cpp_sources_parse_with_bazel_bzl_policy() {
    assert_fixtures(RULES_CC_0_2_22_CPP_FIXTURES);
}
