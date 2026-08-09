/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use starlark::environment::GlobalsBuilder;
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::values::Value;
use starlark::values::list::AllocList;
use starlark::values::list_or_tuple::UnpackListOrTuple;

use crate::interpreter::build_context::BuildContext;

fn single_output(mut outs: Vec<String>) -> buck2_error::Result<String> {
    if outs.len() != 1 {
        return Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "Bazel genrule `outs` must contain exactly one output, got {}",
            outs.len()
        ));
    }

    Ok(outs.pop().expect("length was checked above"))
}

/// Translate the deliberately small subset of Bazel make variables supported by the adapter.
///
/// Bazel's make-variable expansion turns `$$` into one literal dollar for the shell. Recognize the
/// escape as a pair before variables so the escaped dollar cannot begin an expansion accidentally.
fn translate_genrule_cmd(cmd: &str) -> buck2_error::Result<String> {
    let mut chars = cmd.chars();
    let mut translated = String::with_capacity(cmd.len());

    while let Some(c) = chars.next() {
        if c != '$' {
            translated.push(c);
            continue;
        }

        match chars.next() {
            Some('$') => translated.push('$'),
            Some('@') => translated.push_str("${OUT}"),
            Some('(') => {
                let mut variable = String::new();
                let mut terminated = false;
                for c in chars.by_ref() {
                    if c == ')' {
                        terminated = true;
                        break;
                    }
                    variable.push(c);
                }

                if !terminated {
                    return Err(buck2_error::buck2_error!(
                        buck2_error::ErrorTag::Input,
                        "unterminated Bazel genrule make variable `$({}`; supported variables are `$@` and `$(SRCS)`",
                        variable
                    ));
                }

                if variable == "SRCS" {
                    translated.push_str("${SRCS}");
                } else {
                    return Err(buck2_error::buck2_error!(
                        buck2_error::ErrorTag::Input,
                        "unsupported Bazel genrule make variable `$({})`; supported variables are `$@` and `$(SRCS)`",
                        variable
                    ));
                }
            }
            Some(other) => {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "unsupported Bazel genrule dollar expression `${}`; supported expressions are `$$`, `$@`, and `$(SRCS)`",
                    other
                ));
            }
            None => {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "dangling `$` in Bazel genrule command; supported expressions are `$$`, `$@`, and `$(SRCS)`"
                ));
            }
        }
    }

    Ok(translated)
}

#[starlark_module]
fn register_genrule(builder: &mut GlobalsBuilder) {
    /// Declare the bounded Bazel-compatible genrule supported by Buck2's Bazel mode.
    fn genrule<'v>(
        #[starlark(require = named)] name: &str,
        #[starlark(require = named)] outs: UnpackListOrTuple<String>,
        #[starlark(require = named)] cmd: &str,
        #[starlark(require = named, default = UnpackListOrTuple::default())]
        srcs: UnpackListOrTuple<String>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let out = single_output(outs.items)?;
        let cmd = translate_genrule_cmd(cmd)?;
        let heap = eval.heap();
        let srcs = heap.alloc(AllocList(
            srcs.items.iter().map(|src| heap.alloc_str(src).to_value()),
        ));
        let named = [
            ("name", heap.alloc_str(name).to_value()),
            ("out", heap.alloc_str(&out).to_value()),
            ("cmd", heap.alloc_str(&cmd).to_value()),
            ("srcs", srcs),
        ];
        // Add the backend's owning heap to the evaluator heap before converting the resulting
        // frozen value to the evaluator's `'v` lifetime. This avoids coupling the callable to the
        // extra-context borrow.
        let backend = BuildContext::from_context(eval)?
            .bazel_genrule_backend()?
            .owned_value(eval.frozen_heap())
            .unpack_frozen()
            .expect("OwnedFrozenValue must produce a frozen value")
            .to_value();

        eval.eval_function(backend, &[], &named)
    }
}

pub(crate) fn register_bazel_build_globals(builder: &mut GlobalsBuilder) {
    register_genrule(builder);
}

pub(crate) fn register_bazel_bzl_globals(builder: &mut GlobalsBuilder) {
    builder.namespace("native", register_genrule);
}

#[cfg(test)]
mod tests {
    use starlark::assert::Assert;

    use super::register_genrule;
    use super::single_output;
    use super::translate_genrule_cmd;

    #[test]
    fn translates_supported_make_variables() {
        assert_eq!(
            "cp ${SRCS} ${OUT}",
            translate_genrule_cmd("cp $(SRCS) $@").unwrap()
        );
    }

    #[test]
    fn preserves_escaped_dollars_before_recognizing_variables() {
        assert_eq!(
            "echo $@ $(SRCS) $$ ${OUT}",
            translate_genrule_cmd("echo $$@ $$(SRCS) $$$$ $@").unwrap()
        );
    }

    #[test]
    fn rejects_unsupported_or_malformed_dollar_expressions() {
        for cmd in [
            "echo $(location //foo)",
            "echo $HOME",
            "echo $(SRCS",
            "echo $",
        ] {
            let error = translate_genrule_cmd(cmd).unwrap_err().to_string();
            assert!(
                error.contains("Bazel genrule"),
                "unexpected error for {cmd:?}: {error}"
            );
        }
    }

    #[test]
    fn requires_exactly_one_output() {
        assert_eq!(
            "out.txt",
            single_output(vec!["out.txt".to_owned()]).unwrap()
        );
        assert!(
            single_output(Vec::new())
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
        assert!(
            single_output(vec!["one".to_owned(), "two".to_owned()])
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }

    #[test]
    fn genrule_surface_is_named_only_and_closed() {
        let mut a = Assert::new();
        a.globals_add(register_genrule);
        a.fail(
            r#"genrule("target", ["out"], "echo")"#,
            "Missing named-only parameter",
        );
        a.fail(
            r#"genrule(name = "target", outs = ["out"], cmd = "echo", unknown = True)"#,
            "extra named parameter",
        );
        a.fail(
            r#"genrule(name = "target", outs = "out", cmd = "echo")"#,
            "Type of parameter `outs` doesn't match",
        );
        a.fail(
            r#"genrule(name = 1, outs = ["out"], cmd = "echo")"#,
            "Type of parameter `name` doesn't match",
        );
        a.fail(
            r#"genrule(name = "target", outs = ["out"], cmd = 1)"#,
            "Type of parameter `cmd` doesn't match",
        );
        a.fail(
            r#"genrule(name = "target", outs = ["out"], cmd = "echo", srcs = "src")"#,
            "Type of parameter `srcs` doesn't match",
        );
    }
}
