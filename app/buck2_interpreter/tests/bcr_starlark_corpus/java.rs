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

const RULES_JAVA_9_7_1: &[Fixture] = &[
    Fixture {
        module: "rules_java",
        version: "9.7.1",
        path: "java/extensions.bzl",
        kind: FixtureKind::Bzl,
        sha256: "7ed7f0baab45ab5bd7925ba9c77ecd98de6e72228fd3d4ee2422f0c5a7a0aaae",
        source: include_str!("../../testcases/bcr/rules_java/9.7.1/source/java/extensions.bzl"),
    },
    Fixture {
        module: "rules_java",
        version: "9.7.1",
        path: "java/bazel/rules/bazel_java_library.bzl",
        kind: FixtureKind::Bzl,
        sha256: "97f87b1bc3c6a5faa186e26d325578e2aca274a8131755a7805976b7ee5fb2f7",
        source: include_str!(
            "../../testcases/bcr/rules_java/9.7.1/source/java/bazel/rules/bazel_java_library.bzl"
        ),
    },
    Fixture {
        module: "rules_java",
        version: "9.7.1",
        path: "java/common/rules/java_runtime.bzl",
        kind: FixtureKind::Bzl,
        sha256: "12106b7039255da331be1d6a182fc314835f7c4fcf83bdf044ab5528ac562898",
        source: include_str!(
            "../../testcases/bcr/rules_java/9.7.1/source/java/common/rules/java_runtime.bzl"
        ),
    },
    Fixture {
        module: "rules_java",
        version: "9.7.1",
        path: "java/common/rules/BUILD",
        kind: FixtureKind::Build,
        sha256: "10670193ef22f102c6274ceb517fae77e9426b14e44df3d6d76378f591bed6e4",
        source: include_str!("../../testcases/bcr/rules_java/9.7.1/source/java/common/rules/BUILD"),
    },
];

#[test]
fn latest_rules_java_release_parses() {
    assert_fixtures(RULES_JAVA_9_7_1);
}
