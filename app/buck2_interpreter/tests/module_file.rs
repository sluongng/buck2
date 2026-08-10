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
use buck2_interpreter::module_file::parse_module_file;

fn parse(source: &str, dialect: StarlarkDialect) -> Result<(), String> {
    parse_module_file("MODULE.bazel", source.to_owned(), dialect)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn assert_rejected(source: &str, expected: &str) {
    let error = parse(source, StarlarkDialect::Bazel).unwrap_err();
    assert!(
        error.contains(expected),
        "expected error containing `{expected}` for:\n{source}\nactual error:\n{error}"
    );
}

#[test]
fn parses_the_supported_module_subset() {
    parse(
        "pass\nxs = [x for x in [1, 2] if x]\nmodule(name = \"root\", **{\"version\": \"1.0\"})",
        StarlarkDialect::Bazel,
    )
    .unwrap();
}

#[test]
fn rejects_nonliteral_argument_expansion() {
    assert_rejected("module(*args)", "does not allow `*args`");
    assert_rejected(
        "module(**kwargs)",
        "allows `**kwargs` only when the expanded expression is a literal dict",
    );
}

#[test]
fn rejects_bytes_and_include_comprehension_bindings() {
    assert_rejected("module(name = b\"root\")", "does not allow bytes literals");
    assert_rejected(
        "include(b\"fragment.MODULE.bazel\")",
        "does not allow bytes literals",
    );
    assert_rejected(
        "xs = [1 for include in []]",
        "may only be rebound by a simple top-level assignment",
    );
}

#[test]
fn accepts_pass_but_rejects_top_level_control_flow() {
    parse("pass", StarlarkDialect::Bazel).unwrap();
    assert_rejected(
        "if True:\n  pass",
        "contains a statement that is not allowed",
    );
    assert_rejected(
        "for x in []:\n  pass",
        "contains a statement that is not allowed",
    );
    assert_rejected("break", "`break` cannot be used outside of a `for` loop");
    assert_rejected(
        "continue",
        "`continue` cannot be used outside of a `for` loop",
    );
    assert_rejected(
        "return",
        "`return` cannot be used outside of a `def` function",
    );
}

#[test]
fn recognizes_but_does_not_evaluate_include() {
    assert_rejected(
        "include(\"fragment.MODULE.bazel\")",
        "is recognized but is not supported by Buck2 yet",
    );
    assert_rejected(
        "wrapper(include(\"fragment.MODULE.bazel\"))",
        "may only be called directly at top level",
    );
}

#[test]
fn rejects_module_files_in_the_buck2_dialect() {
    let error = parse("module(name = \"root\")", StarlarkDialect::Buck2).unwrap_err();
    assert!(error.contains("require `[buck2] starlark_dialect = bazel`"));
}
