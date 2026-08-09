/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_interpreter::dialect::StarlarkDialect;
use buck2_interpreter::file_type::StarlarkFileType;
use sha2::Digest;
use sha2::Sha256;
use starlark::syntax::AstModule;

#[path = "bcr_starlark_corpus/c.rs"]
mod c;
#[path = "bcr_starlark_corpus/cpp.rs"]
mod cpp;
#[path = "bcr_starlark_corpus/go.rs"]
mod go;
#[path = "bcr_starlark_corpus/java.rs"]
mod java;

#[derive(Copy, Clone, Debug)]
enum FixtureKind {
    Bzl,
    Build,
}

impl FixtureKind {
    fn file_type(self) -> StarlarkFileType {
        match self {
            Self::Bzl => StarlarkFileType::Bzl,
            Self::Build => StarlarkFileType::Buck,
        }
    }
}

#[derive(Copy, Clone, Debug)]
#[allow(dead_code)]
struct Fixture {
    module: &'static str,
    version: &'static str,
    path: &'static str,
    kind: FixtureKind,
    sha256: &'static str,
    source: &'static str,
}

#[allow(dead_code)]
fn assert_fixtures(fixtures: &[Fixture]) {
    assert!(!fixtures.is_empty(), "a BCR corpus group must not be empty");

    for fixture in fixtures {
        let actual_sha256 = format!("{:x}", Sha256::digest(fixture.source.as_bytes()));
        assert_eq!(
            fixture.sha256, actual_sha256,
            "vendored fixture drifted: {}@{} {}",
            fixture.module, fixture.version, fixture.path
        );

        let dialect = StarlarkDialect::Bazel
            .parser_dialect(fixture.kind.file_type(), false)
            .unwrap();
        AstModule::parse(fixture.path, fixture.source.to_owned(), &dialect).unwrap_or_else(
            |error| {
                panic!(
                    "latest BCR syntax fixture failed: {}@{} {} ({:?})\n{}",
                    fixture.module, fixture.version, fixture.path, fixture.kind, error
                )
            },
        );
    }
}

fn parse(source: &str, kind: FixtureKind) -> Result<AstModule, String> {
    let dialect = StarlarkDialect::Bazel
        .parser_dialect(kind.file_type(), false)
        .unwrap();
    AstModule::parse("negative_control", source.to_owned(), &dialect)
        .map_err(|error| error.to_string())
}

#[test]
fn bcr_corpus_uses_production_bazel_file_policies() {
    parse(
        "load(\"//:defs.bzl\", \"x\")\ndef f(*, x):\n    return x\n",
        FixtureKind::Bzl,
    )
    .unwrap();
    parse("x = 1\nx = 2\n", FixtureKind::Build).unwrap();

    assert!(parse("x = 1\nload(\"//:defs.bzl\", \"x\")\n", FixtureKind::Bzl).is_err());
    assert!(parse("x = 1\nx = 2\n", FixtureKind::Bzl).is_err());
    assert!(parse("def f():\n    pass\n", FixtureKind::Build).is_err());
    assert!(parse("f(*args)\n", FixtureKind::Build).is_err());
}
