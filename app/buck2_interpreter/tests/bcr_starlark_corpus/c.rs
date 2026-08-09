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

const FIXTURES: &[Fixture] = &[
    Fixture {
        module: "rules_cc",
        version: "0.2.22",
        path: "cc/private/compile/compile_build_variables.bzl",
        kind: FixtureKind::Bzl,
        sha256: "ce81d304a5a3a37334fb4e28b897f84037f4d451e988541a377bb93b90f5d2f8",
        source: include_str!(
            "../../testcases/bcr/rules_cc/0.2.22/c/source/cc/private/compile/compile_build_variables.bzl"
        ),
    },
    Fixture {
        module: "rules_cc",
        version: "0.2.22",
        path: "cc/action_names.bzl",
        kind: FixtureKind::Bzl,
        sha256: "5be05896333f8806c824e97d2b9c6bd4a5e0b03c3cb4f96988f1263359d0a57d",
        source: include_str!("../../testcases/bcr/rules_cc/0.2.22/c/source/cc/action_names.bzl"),
    },
];

#[test]
fn latest_rules_cc_c_sources_parse_with_bazel_policy() {
    assert_fixtures(FIXTURES);
}
