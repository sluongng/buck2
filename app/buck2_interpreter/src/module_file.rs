/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Parser and static syntax validation for Buck2's supported Bazel
//! `MODULE.bazel` subset.
//!
//! This module deliberately stops before evaluation. It provides a static gate
//! for the subset that a later Bzlmod evaluator can consume, while recognizing
//! unsupported directives explicitly.

use starlark::syntax::AstModule;
use starlark::syntax::ast::Argument;
use starlark::syntax::ast::AssignP;
use starlark::syntax::ast::AssignTarget;
use starlark::syntax::ast::AstAssignTarget;
use starlark::syntax::ast::AstExpr;
use starlark::syntax::ast::AstLiteral;
use starlark::syntax::ast::AstNoPayload;
use starlark::syntax::ast::AstStmt;
use starlark::syntax::ast::CallArgsP;
use starlark::syntax::ast::Clause;
use starlark::syntax::ast::Expr;
use starlark::syntax::ast::ForClauseP;
use starlark::syntax::ast::Stmt;

use crate::dialect::StarlarkDialect;
use crate::file_type::StarlarkFileType;

#[derive(Debug, buck2_error::Error)]
#[buck2(input)]
enum ModuleFileValidationError {
    #[error("MODULE.bazel does not allow `*args` in function calls")]
    StarArgs,
    #[error("MODULE.bazel allows `**kwargs` only when the expanded expression is a literal dict")]
    NonLiteralKwArgs,
    #[error("MODULE.bazel does not allow bytes literals")]
    BytesLiteral,
    #[error(
        "the Bazel MODULE.bazel `include` directive requires exactly one positional string literal argument"
    )]
    InvalidInclude,
    #[error("the Bazel MODULE.bazel `include` directive may only be called directly at top level")]
    NestedInclude,
    #[error("the Bazel MODULE.bazel `include` directive cannot be used as a value")]
    IndirectInclude,
    #[error(
        "the Bazel MODULE.bazel `include` name may only be rebound by a simple top-level assignment"
    )]
    InvalidIncludeBinding,
    #[error(
        "the Bazel MODULE.bazel `include` directive is recognized but is not supported by Buck2 yet"
    )]
    UnsupportedInclude,
    #[error(
        "MODULE.bazel contains a statement that is not allowed in Buck2's supported MODULE.bazel subset"
    )]
    DisallowedStatement,
}

/// Parse and statically validate a `MODULE.bazel` file without evaluating it.
pub fn parse_module_file(
    filename: &str,
    content: String,
    starlark_dialect: StarlarkDialect,
) -> buck2_error::Result<AstModule> {
    let dialect = starlark_dialect.parser_dialect(StarlarkFileType::Module, false)?;
    let ast = AstModule::parse(filename, content, &dialect)?;
    validate_module_file(&ast)?;
    Ok(ast)
}

/// Apply MODULE-specific checks that the generic Starlark dialect cannot express.
pub fn validate_module_file(ast: &AstModule) -> buck2_error::Result<()> {
    validate_no_bytes(ast.statement())?;
    let mut include_is_builtin = true;
    validate_stmt(ast.statement(), &mut include_is_builtin)
}

fn validate_no_bytes(stmt: &AstStmt) -> buck2_error::Result<()> {
    stmt.node.visit_expr_result(validate_no_bytes_expr)
}

fn validate_no_bytes_expr(expr: &AstExpr) -> buck2_error::Result<()> {
    if matches!(expr.node, Expr::Literal(AstLiteral::Bytes(_))) {
        return Err(ModuleFileValidationError::BytesLiteral.into());
    }
    expr.node.visit_expr_err(validate_no_bytes_expr)
}

fn validate_stmt(stmt: &AstStmt, include_is_builtin: &mut bool) -> buck2_error::Result<()> {
    match &stmt.node {
        Stmt::Statements(statements) => {
            for statement in statements {
                validate_stmt(statement, include_is_builtin)?;
            }
        }
        Stmt::Expression(expr) => validate_top_level_expr(expr, *include_is_builtin)?,
        // Bazel's MODULE syntax checker explicitly permits a top-level no-op.
        Stmt::Pass => {}
        Stmt::Assign(AssignP { lhs, ty: _, rhs }) => {
            // Bazel classifies the RHS while the built-in `include` name is still
            // visible, then lets a simple assignment shadow it.
            validate_expr(rhs, *include_is_builtin)?;
            if is_simple_include_assignment(lhs) {
                *include_is_builtin = false;
            } else {
                validate_assign_target(lhs, *include_is_builtin)?;
            }
        }
        Stmt::AssignModify(lhs, _, rhs) => {
            // Match Bazel's assignment visitor: classify the RHS before a
            // simple target shadows the built-in directive.
            validate_expr(rhs, *include_is_builtin)?;
            if is_simple_include_assignment(lhs) {
                *include_is_builtin = false;
            } else {
                validate_assign_target(lhs, *include_is_builtin)?;
            }
        }
        // The supported MODULE subset rejects control flow. Return an input
        // error as a defense in depth for callers of `validate_module_file`
        // that supply an AST parsed with some other dialect.
        Stmt::Break
        | Stmt::Continue
        | Stmt::Return(_)
        | Stmt::If(_, _)
        | Stmt::IfElse(_, _)
        | Stmt::For(_)
        | Stmt::Def(_)
        | Stmt::Load(_) => return Err(ModuleFileValidationError::DisallowedStatement.into()),
    }
    Ok(())
}

fn validate_top_level_expr(expr: &AstExpr, include_is_builtin: bool) -> buck2_error::Result<()> {
    if include_is_builtin {
        if let Expr::Call(function, args) = &expr.node {
            if is_include_identifier(function) {
                return validate_include_directive(args);
            }
        }
    }
    validate_expr(expr, include_is_builtin)
}

fn validate_include_directive(args: &CallArgsP<AstNoPayload>) -> buck2_error::Result<()> {
    match args.args.as_slice() {
        [arg] => match &arg.node {
            Argument::Positional(expr)
                if matches!(expr.node, Expr::Literal(AstLiteral::String(_))) =>
            {
                Err(ModuleFileValidationError::UnsupportedInclude.into())
            }
            _ => Err(ModuleFileValidationError::InvalidInclude.into()),
        },
        _ => Err(ModuleFileValidationError::InvalidInclude.into()),
    }
}

fn validate_expr(expr: &AstExpr, include_is_builtin: bool) -> buck2_error::Result<()> {
    match &expr.node {
        Expr::Identifier(identifier)
            if include_is_builtin && identifier.node.ident == "include" =>
        {
            Err(ModuleFileValidationError::IndirectInclude.into())
        }
        Expr::Call(function, args) => {
            if include_is_builtin && is_include_identifier(function) {
                return Err(ModuleFileValidationError::NestedInclude.into());
            }

            validate_expr(function, include_is_builtin)?;
            for arg in &args.args {
                match &arg.node {
                    Argument::Args(_) => {
                        return Err(ModuleFileValidationError::StarArgs.into());
                    }
                    Argument::KwArgs(expanded) => {
                        if !matches!(expanded.node, Expr::Dict(_)) {
                            return Err(ModuleFileValidationError::NonLiteralKwArgs.into());
                        }
                        validate_expr(expanded, include_is_builtin)?;
                    }
                    Argument::Positional(value) | Argument::Named(_, value) => {
                        validate_expr(value, include_is_builtin)?;
                    }
                }
            }
            Ok(())
        }
        Expr::ListComprehension(value, first_for, clauses) => {
            validate_comprehension(first_for, clauses, include_is_builtin)?;
            validate_expr(value, include_is_builtin)
        }
        Expr::DictComprehension(key_and_value, first_for, clauses) => {
            validate_comprehension(first_for, clauses, include_is_builtin)?;
            validate_expr(&key_and_value.0, include_is_builtin)?;
            validate_expr(&key_and_value.1, include_is_builtin)
        }
        _ => expr
            .node
            .visit_expr_err(|child| validate_expr(child, include_is_builtin)),
    }
}

fn validate_comprehension(
    first_for: &ForClauseP<AstNoPayload>,
    clauses: &[Clause],
    include_is_builtin: bool,
) -> buck2_error::Result<()> {
    validate_for_clause(first_for, include_is_builtin)?;
    for clause in clauses {
        match clause {
            Clause::For(for_clause) => validate_for_clause(for_clause, include_is_builtin)?,
            Clause::If(condition) => validate_expr(condition, include_is_builtin)?,
        }
    }
    Ok(())
}

fn validate_for_clause(
    for_clause: &ForClauseP<AstNoPayload>,
    include_is_builtin: bool,
) -> buck2_error::Result<()> {
    validate_expr(&for_clause.over, include_is_builtin)?;
    validate_assign_target(&for_clause.var, include_is_builtin)
}

fn validate_assign_target(
    target: &AstAssignTarget,
    include_is_builtin: bool,
) -> buck2_error::Result<()> {
    match &target.node {
        AssignTarget::Identifier(identifier)
            if include_is_builtin && identifier.node.ident == "include" =>
        {
            Err(ModuleFileValidationError::InvalidIncludeBinding.into())
        }
        AssignTarget::Identifier(_) => Ok(()),
        AssignTarget::Tuple(targets) => {
            for target in targets {
                validate_assign_target(target, include_is_builtin)?;
            }
            Ok(())
        }
        AssignTarget::Dot(receiver, _) => validate_expr(receiver, include_is_builtin),
        AssignTarget::Index(receiver_and_index) => {
            validate_expr(&receiver_and_index.0, include_is_builtin)?;
            validate_expr(&receiver_and_index.1, include_is_builtin)
        }
    }
}

fn is_simple_include_assignment(target: &AstAssignTarget) -> bool {
    matches!(
        &target.node,
        AssignTarget::Identifier(identifier) if identifier.node.ident == "include"
    )
}

fn is_include_identifier(expr: &AstExpr) -> bool {
    matches!(
        &expr.node,
        Expr::Identifier(identifier) if identifier.node.ident == "include"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> buck2_error::Result<AstModule> {
        parse_module_file("MODULE.bazel", source.to_owned(), StarlarkDialect::Bazel)
    }

    fn assert_rejected(source: &str, expected: &str) {
        let error = parse(source).unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected error containing `{expected}` for:\n{source}\nactual error:\n{error}"
        );
    }

    #[test]
    fn accepts_supported_module_expression_syntax() {
        parse(
            r#"
pass
values = [x for x in [1, 2] if x]
mapping = {x: x for x in values}
selected = values if values else [0]
module(name = "root", **{"version": "1.0"})
"#,
        )
        .unwrap();
    }

    #[test]
    fn rejects_constructs_excluded_from_module_files() {
        assert_rejected("load(\":defs.bzl\", \"x\")", "not allowed in this dialect");
        assert_rejected("def f():\n  pass", "not allowed in this dialect");
        assert_rejected("x = lambda: 1", "not allowed in this dialect");
        assert_rejected("x: int = 1", "type annotation");
        assert_rejected("x = f\"{y}\"", "f-string");
        assert_rejected("x = b\"bytes\"", "does not allow bytes literals");
        assert_rejected("x = 1\nx = 2", "redeclared at top level");
        assert_rejected(
            "if True:\n  x = 1",
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
        assert_rejected(
            "xs = [1 for include in []]",
            "may only be rebound by a simple top-level assignment",
        );
        assert_rejected("f(*args)", "does not allow `*args`");
        assert_rejected(
            "f(**kwargs)",
            "allows `**kwargs` only when the expanded expression is a literal dict",
        );
        assert_rejected(
            "f(**dict(x = 1))",
            "allows `**kwargs` only when the expanded expression is a literal dict",
        );
    }

    #[test]
    fn classifies_include_before_it_is_rebound() {
        assert_rejected(
            "include(\"fragment.MODULE.bazel\")",
            "is recognized but is not supported by Buck2 yet",
        );
        assert_rejected(
            "include()",
            "requires exactly one positional string literal",
        );
        assert_rejected(
            "include(\"a\", \"b\")",
            "requires exactly one positional string literal",
        );
        assert_rejected(
            "include(path = \"fragment.MODULE.bazel\")",
            "requires exactly one positional string literal",
        );
        assert_rejected(
            "include(fragment)",
            "requires exactly one positional string literal",
        );
        assert_rejected("saved = include", "cannot be used as a value");
        assert_rejected(
            "wrapper(include(\"fragment.MODULE.bazel\"))",
            "may only be called directly at top level",
        );
        assert_rejected(
            "include = include(\"fragment.MODULE.bazel\")",
            "may only be called directly at top level",
        );
    }

    #[test]
    fn simple_assignment_shadows_the_include_directive() {
        parse("include = function\ninclude(dynamic_path)").unwrap();
        parse("include += function\ninclude(dynamic_path)").unwrap();
        assert_rejected(
            "include += include(\"fragment.MODULE.bazel\")",
            "may only be called directly at top level",
        );
    }

    #[test]
    fn buck2_dialect_cannot_parse_module_files() {
        let error = parse_module_file(
            "MODULE.bazel",
            "module(name = \"root\")".to_owned(),
            StarlarkDialect::Buck2,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("require `[buck2] starlark_dialect = bazel`"));
    }
}
