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

const RULES_GO_0_62_0: &[Fixture] = &[
    Fixture {
        module: "rules_go",
        version: "0.62.0",
        path: "examples/basic_gazelle/BUILD.bazel",
        kind: FixtureKind::Build,
        sha256: "e49d0eb00edea0571a72079765a1203d9032b4cdcf3b11c9719967eaeb727169",
        source: include_str!(
            "../../testcases/bcr/rules_go/0.62.0/source/examples/basic_gazelle/BUILD.bazel"
        ),
    },
    Fixture {
        module: "rules_go",
        version: "0.62.0",
        path: "go/private/providers.bzl",
        kind: FixtureKind::Bzl,
        sha256: "8a0e1d37080ed5ed75c6b7fde33f56812302d9f2fc0afd8188d6411f4fcdb416",
        source: include_str!("../../testcases/bcr/rules_go/0.62.0/source/go/private/providers.bzl"),
    },
    Fixture {
        module: "rules_go",
        version: "0.62.0",
        path: "go/private/rules/library.bzl",
        kind: FixtureKind::Bzl,
        sha256: "5ce524ff9cdea71a0a463b92295c4f124e629535406295d9e9a92f6aa1e47a46",
        source: include_str!(
            "../../testcases/bcr/rules_go/0.62.0/source/go/private/rules/library.bzl"
        ),
    },
    Fixture {
        module: "rules_go",
        version: "0.62.0",
        path: "go/private/rules/transition.bzl",
        kind: FixtureKind::Bzl,
        sha256: "e4f5a4b44419bd8c9e5a508bb489df651649f11669ba14d0acf9ca52b525809a",
        source: include_str!(
            "../../testcases/bcr/rules_go/0.62.0/source/go/private/rules/transition.bzl"
        ),
    },
    Fixture {
        module: "rules_go",
        version: "0.62.0",
        path: "go/private/extensions.bzl",
        kind: FixtureKind::Bzl,
        sha256: "10becf03bb5646057f6572359dbce4c72d647a7e9331cffe590d39a59050f11c",
        source: include_str!(
            "../../testcases/bcr/rules_go/0.62.0/source/go/private/extensions.bzl"
        ),
    },
];

#[test]
fn latest_rules_go_sources_parse_with_bazel_policy() {
    assert_fixtures(RULES_GO_0_62_0);
}
