/*
 * Copyright 2018 The Starlark in Rust Authors.
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! AST for parsed starlark files.

use std::collections::HashMap;
use std::collections::HashSet;

use crate::codemap::Span;
use crate::syntax::DialectTypes;
use crate::syntax::ast::ArgumentP;
use crate::syntax::ast::AssignP;
use crate::syntax::ast::AstArgument;
use crate::syntax::ast::AstAssignIdent;
use crate::syntax::ast::AstAssignTarget;
use crate::syntax::ast::AstExpr;
use crate::syntax::ast::AstLiteral;
use crate::syntax::ast::AstParameter;
use crate::syntax::ast::AstStmt;
use crate::syntax::ast::CallArgsP;
use crate::syntax::ast::DefP;
use crate::syntax::ast::Expr;
use crate::syntax::ast::ForP;
use crate::syntax::ast::LambdaP;
use crate::syntax::ast::ParameterP;
use crate::syntax::ast::Stmt;
use crate::syntax::call::CallArgsUnpack;
use crate::syntax::def::DefParams;
use crate::syntax::state::ParserState;
use crate::syntax::top_level_stmts::top_level_stmts;

impl Expr {
    /// We want to check a function call is well-formed.
    /// Our eventual plan is to follow the Python invariants, but for now, we are closer
    /// to the Starlark invariants.
    ///
    /// Python invariants are no positional arguments after named arguments,
    /// no *args after **kwargs, no repeated argument names.
    ///
    /// Starlark invariants are the above, plus at most one *args and the *args must appear
    /// after all positional and named arguments. The spec is silent on whether you are allowed
    /// multiple **kwargs.
    ///
    /// We allow at most one **kwargs.
    pub(crate) fn check_call(
        f: AstExpr,
        args: Vec<AstArgument>,
        parser_state: &mut ParserState<'_>,
    ) -> Expr {
        let args = CallArgsP { args };

        if let Err(e) = CallArgsUnpack::unpack(&args, parser_state.codemap) {
            parser_state.errors.push(e);
        }

        Expr::Call(Box::new(f), args)
    }
}

/// Validate all statements only occur where they are allowed to.
pub(crate) fn validate_module(stmt: &AstStmt, parser_state: &mut ParserState) {
    fn validate_loads_first(stmt: &AstStmt, parser_state: &mut ParserState) {
        if !parser_state.dialect.require_load_statements_first {
            return;
        }

        let mut first_non_load = None;
        for stmt in top_level_stmts(stmt) {
            let is_string_literal = matches!(
                &stmt.node,
                Stmt::Expression(expr)
                    if matches!(&expr.node, Expr::Literal(AstLiteral::String(_)))
            );
            if is_string_literal {
                continue;
            }

            if matches!(&stmt.node, Stmt::Load(_)) {
                if first_non_load.is_some() {
                    parser_state.error(
                        stmt.span,
                        "load statements must appear before any other statement",
                    );
                }
            } else if first_non_load.is_none() {
                first_non_load = Some(stmt.span);
            }
        }
    }

    fn validate_top_level_bindings(stmt: &AstStmt, parser_state: &mut ParserState) {
        if parser_state.dialect.allow_toplevel_rebinding {
            return;
        }

        fn bind(
            ident: &AstAssignIdent,
            bindings: &mut HashMap<String, Span>,
            parser_state: &mut ParserState,
        ) {
            if let Some(previous) = bindings.get(&ident.node.ident) {
                parser_state.error(
                    ident.span,
                    format_args!("'{}' redeclared at top level", ident.node.ident),
                );
                parser_state.error(
                    *previous,
                    format_args!("'{}' previously declared here", ident.node.ident),
                );
            } else {
                bindings.insert(ident.node.ident.clone(), ident.span);
            }
        }

        fn bind_target(
            target: &AstAssignTarget,
            bindings: &mut HashMap<String, Span>,
            parser_state: &mut ParserState,
        ) {
            target
                .node
                .visit_lvalue(|ident| bind(ident, bindings, parser_state));
        }

        fn visit(
            stmt: &AstStmt,
            bindings: &mut HashMap<String, Span>,
            parser_state: &mut ParserState,
        ) {
            match &stmt.node {
                Stmt::Statements(stmts) => {
                    for stmt in stmts {
                        visit(stmt, bindings, parser_state);
                    }
                }
                Stmt::Assign(AssignP { lhs, .. }) | Stmt::AssignModify(lhs, ..) => {
                    bind_target(lhs, bindings, parser_state);
                }
                Stmt::Def(DefP { name, .. }) => {
                    bind(name, bindings, parser_state);
                    // Function-local bindings do not participate in module rebinding checks.
                }
                Stmt::Load(load) => {
                    for arg in &load.args {
                        bind(&arg.local, bindings, parser_state);
                    }
                }
                Stmt::If(_, body) => visit(body, bindings, parser_state),
                Stmt::IfElse(_, bodies) => {
                    visit(&bodies.0, bindings, parser_state);
                    visit(&bodies.1, bindings, parser_state);
                }
                Stmt::For(ForP { var, body, .. }) => {
                    bind_target(var, bindings, parser_state);
                    visit(body, bindings, parser_state);
                }
                Stmt::Break
                | Stmt::Continue
                | Stmt::Pass
                | Stmt::Return(_)
                | Stmt::Expression(_) => {}
            }
        }

        visit(stmt, &mut HashMap::new(), parser_state);
    }

    fn validate_params(params: &[AstParameter], parser_state: &mut ParserState) {
        if !parser_state.dialect.enable_keyword_only_arguments {
            for param in params {
                if let ParameterP::NoArgs = &param.node {
                    parser_state.error(
                        param.span,
                        "* keyword-only-arguments is not allowed in this dialect",
                    );
                }
            }
        }
        if !parser_state.dialect.enable_positional_only_arguments {
            for param in params {
                if let ParameterP::Slash = &param.node {
                    parser_state.error(
                        param.span,
                        "/ positional-only-arguments is not allowed in this dialect",
                    );
                }
            }
        }
        if let Err(e) = DefParams::unpack(params, parser_state.codemap) {
            parser_state.errors.push(e);
        }
    }

    // Inside a for, we allow continue/break, unless we go beneath a def.
    // Inside a def, we allow return.
    // All load's must occur at the top-level.
    // At the top-level we only allow for/if when the dialect permits it.
    fn f(
        stmt: &AstStmt,
        parser_state: &mut ParserState,
        top_level: bool,
        inside_for: bool,
        inside_def: bool,
    ) {
        let span = stmt.span;

        match &stmt.node {
            Stmt::Def(DefP { params, body, .. }) => {
                if !parser_state.dialect.enable_def {
                    parser_state.error(span, "`def` is not allowed in this dialect");
                }
                validate_params(params, parser_state);
                f(body, parser_state, false, false, true)
            }
            Stmt::For(ForP { body, .. }) => {
                if top_level && !parser_state.dialect.enable_top_level_stmt {
                    parser_state.error(span, "`for` cannot be used outside `def` in this dialect")
                } else {
                    f(body, parser_state, false, true, inside_def)
                }
            }
            Stmt::If(..) | Stmt::IfElse(..) => {
                if top_level && !parser_state.dialect.enable_top_level_stmt {
                    parser_state.error(span, "`if` cannot be used outside `def` in this dialect")
                } else {
                    stmt.node
                        .visit_stmt(|x| f(x, parser_state, false, inside_for, inside_def))
                }
            }
            Stmt::Break if !inside_for => {
                parser_state.error(span, "`break` cannot be used outside of a `for` loop")
            }
            Stmt::Continue if !inside_for => {
                parser_state.error(span, "`continue` cannot be used outside of a `for` loop")
            }
            Stmt::Return(_) if !inside_def => {
                parser_state.error(span, "`return` cannot be used outside of a `def` function")
            }
            Stmt::Load(load) => {
                if !top_level {
                    parser_state.error(span, "`load` must only occur at the top of a module");
                }
                if !parser_state.dialect.enable_load {
                    parser_state.error(span, "`load` is not allowed in this dialect");
                }

                let mut locals = HashSet::new();
                for arg in &load.args {
                    if !parser_state.dialect.allow_load_private_symbols
                        && arg.their.node.starts_with('_')
                    {
                        parser_state.error(
                            arg.their.span,
                            format_args!(
                                "symbol '{}' is private and cannot be imported",
                                arg.their.node
                            ),
                        );
                    }
                    if !locals.insert(&arg.local.node.ident)
                        && !parser_state.dialect.allow_load_duplicate_local_bindings
                    {
                        parser_state.error(
                            arg.local.span,
                            format_args!(
                                "load statement defines '{}' more than once",
                                arg.local.node.ident
                            ),
                        );
                    }
                }
            }
            _ => stmt
                .node
                .visit_stmt(|x| f(x, parser_state, top_level, inside_for, inside_def)),
        }
    }

    fn expr(x: &AstExpr, parser_state: &mut ParserState) {
        match &x.node {
            Expr::Literal(AstLiteral::Ellipsis) => {
                if parser_state.dialect.enable_types == DialectTypes::Disable {
                    parser_state.error(x.span, "`...` is not allowed in this dialect");
                }
            }
            Expr::Lambda(LambdaP { params, .. }) => {
                if !parser_state.dialect.enable_lambda {
                    parser_state.error(x.span, "`lambda` is not allowed in this dialect");
                }
                validate_params(params, parser_state);
            }
            Expr::Call(_, args) if !parser_state.dialect.allow_call_star_args => {
                for arg in &args.args {
                    match &arg.node {
                        ArgumentP::Args(_) => parser_state.error(
                            arg.span,
                            "`*args` call arguments are not allowed in this dialect",
                        ),
                        ArgumentP::KwArgs(_) => parser_state.error(
                            arg.span,
                            "`**kwargs` call arguments are not allowed in this dialect",
                        ),
                        ArgumentP::Positional(_) | ArgumentP::Named(_, _) => {}
                    }
                }
            }
            _ => {}
        }
        x.node.visit_expr(|x| expr(x, parser_state));
    }

    validate_loads_first(stmt, parser_state);

    f(stmt, parser_state, true, false, false);

    validate_top_level_bindings(stmt, parser_state);

    stmt.visit_expr(|x| expr(x, parser_state));
}

#[cfg(test)]
mod tests {
    use crate::syntax::AstModule;
    use crate::syntax::Dialect;

    fn assert_validation_error(
        source: &str,
        dialect: &Dialect,
        expected_message: &str,
        expected_source_span: &str,
    ) {
        let error = match AstModule::parse("test.bzl", source.to_owned(), dialect) {
            Ok(_) => panic!("expected parse failure for:\n{source}"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains(expected_message),
            "expected error containing `{expected_message}` for:\n{source}\nactual error:\n{message}",
        );
        let span = error
            .span()
            .unwrap_or_else(|| panic!("expected a source span for:\n{source}\nerror:\n{message}"));
        assert_eq!(span.source_span(), expected_source_span);
    }

    #[test]
    fn call_star_arguments_are_dialect_controlled() {
        AstModule::parse(
            "test.bzl",
            "f(*args, **kwargs)".to_owned(),
            &Dialect::Standard,
        )
        .unwrap();

        let dialect = Dialect {
            allow_call_star_args: false,
            ..Dialect::Standard
        };
        assert_validation_error(
            "f(1, *args)",
            &dialect,
            "`*args` call arguments are not allowed in this dialect",
            "*args",
        );
        assert_validation_error(
            "f(1, **kwargs)",
            &dialect,
            "`**kwargs` call arguments are not allowed in this dialect",
            "**kwargs",
        );
    }

    #[test]
    fn private_imports_check_the_original_symbol() {
        let dialect = Dialect {
            allow_load_private_symbols: false,
            ..Dialect::Standard
        };
        assert_validation_error(
            "load(\":defs.bzl\", alias = \"_private\")",
            &dialect,
            "symbol '_private' is private and cannot be imported",
            "\"_private\"",
        );

        AstModule::parse(
            "test.bzl",
            "load(\":defs.bzl\", _local = \"public\")".to_owned(),
            &dialect,
        )
        .unwrap();
    }

    #[test]
    fn duplicate_local_binding_in_one_load_is_dialect_controlled() {
        AstModule::parse(
            "test.bzl",
            "load(\":defs.bzl\", \"x\", x = \"y\")".to_owned(),
            &Dialect::Standard,
        )
        .unwrap();

        let dialect = Dialect {
            allow_load_duplicate_local_bindings: false,
            ..Dialect::Standard
        };
        assert_validation_error(
            "load(\":defs.bzl\", \"x\", x = \"y\")",
            &dialect,
            "load statement defines 'x' more than once",
            "x",
        );
    }

    #[test]
    fn loads_first_ignores_string_literal_statements() {
        let dialect = Dialect {
            require_load_statements_first: true,
            ..Dialect::Standard
        };
        AstModule::parse(
            "test.bzl",
            "\"module doc\"\nload(\":one.bzl\", \"one\")\n\"another string\"\nload(\":two.bzl\", \"two\")"
                .to_owned(),
            &dialect,
        )
        .unwrap();

        assert_validation_error(
            "\"module doc\"\nx = 1\n\"another string\"\nload(\":defs.bzl\", \"x\")",
            &dialect,
            "load statements must appear before any other statement",
            "load(\":defs.bzl\", \"x\")",
        );

        AstModule::parse(
            "test.bzl",
            "x = 1\nload(\":defs.bzl\", \"y\")".to_owned(),
            &Dialect::Standard,
        )
        .unwrap();
    }

    #[test]
    fn top_level_bindings_cannot_be_redeclared() {
        let dialect = Dialect {
            enable_top_level_stmt: true,
            allow_toplevel_rebinding: false,
            ..Dialect::Standard
        };
        for (source, span) in [
            ("x = 1\nx = 2", "x"),
            ("x, y = (1, 2)\ny, z = (3, 4)", "y"),
            ("x = 1\nx += 2", "x"),
            ("x = 1\ndef x():\n  pass", "x"),
            ("x = 1\nload(\":defs.bzl\", \"x\")", "\"x\""),
            ("x = 1\nif True:\n  x = 2", "x"),
            ("x = 1\nfor x in []:\n  pass", "x"),
        ] {
            assert_validation_error(source, &dialect, "redeclared at top level", span);
        }
    }

    #[test]
    fn top_level_rebinding_check_does_not_enter_functions() {
        let dialect = Dialect {
            allow_toplevel_rebinding: false,
            ..Dialect::Standard
        };
        AstModule::parse(
            "test.bzl",
            "def f():\n  x = 1\n  x = 2".to_owned(),
            &dialect,
        )
        .unwrap();
    }
}
