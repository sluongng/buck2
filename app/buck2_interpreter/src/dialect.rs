/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use allocative::Allocative;
use dupe::Dupe;
use pagable::Pagable;
use starlark::syntax::Dialect;
use starlark::syntax::DialectTypes;

use crate::file_type::StarlarkFileType;

/// Selects the Starlark language dialect used to parse each type of input file.
#[derive(Copy, Clone, Dupe, Debug, Eq, PartialEq, Hash, Allocative, Pagable)]
pub enum StarlarkDialect {
    Buck2,
    Bazel,
}

impl StarlarkDialect {
    fn bazel_unavailable_error() -> buck2_error::Error {
        buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "Bazel Starlark dialect is not yet available"
        )
    }

    pub fn from_config_value(value: Option<&str>) -> buck2_error::Result<Self> {
        match value {
            None | Some("buck2") => Ok(Self::Buck2),
            Some("bazel") => Ok(Self::Bazel),
            Some(value) => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid value for buckconfig `[buck2] starlark_dialect`. Got `{}`. Expected one of `buck2` or `bazel`.",
                value
            )),
        }
    }

    /// Rejects modes whose parser, validation, and globals are not yet available together.
    pub fn require_available(self) -> buck2_error::Result<()> {
        match self {
            Self::Buck2 => Ok(()),
            Self::Bazel => Err(Self::bazel_unavailable_error()),
        }
    }

    pub fn parser_dialect(
        self,
        file_type: StarlarkFileType,
        disable_starlark_types: bool,
    ) -> buck2_error::Result<Dialect> {
        match self {
            Self::Buck2 => Ok(buck2_parser_dialect(file_type, disable_starlark_types)),
            Self::Bazel => Ok(bazel_parser_dialect(file_type, disable_starlark_types)),
        }
    }

    /// Permissive union used only when the debugger reparses source files for breakpoint
    /// resolution. Production parsing still applies the exact dialect for each file type.
    pub fn debugger_parser_dialect(self) -> buck2_error::Result<Dialect> {
        match self {
            Self::Buck2 => Ok(Dialect {
                enable_def: true,
                enable_lambda: true,
                enable_load: true,
                enable_keyword_only_arguments: true,
                enable_types: DialectTypes::ParseOnly,
                enable_load_reexport: false,
                enable_top_level_stmt: true,
                ..Dialect::Standard
            }),
            Self::Bazel => Ok(Dialect {
                enable_def: true,
                enable_lambda: true,
                enable_load: true,
                enable_keyword_only_arguments: true,
                enable_positional_only_arguments: false,
                enable_types: DialectTypes::ParseOnly,
                enable_load_reexport: false,
                enable_top_level_stmt: true,
                enable_f_strings: buck2_core::is_open_source(),
                allow_load_private_symbols: true,
                allow_toplevel_rebinding: true,
                require_load_statements_first: false,
                allow_call_star_args: true,
                allow_load_duplicate_local_bindings: true,
                ..Dialect::Standard
            }),
        }
    }
}

fn bazel_parser_dialect(file_type: StarlarkFileType, disable_starlark_types: bool) -> Dialect {
    let build_dialect = Dialect {
        enable_def: false,
        enable_lambda: false,
        enable_load: true,
        enable_keyword_only_arguments: true,
        enable_positional_only_arguments: false,
        enable_types: DialectTypes::Disable,
        enable_load_reexport: false,
        enable_top_level_stmt: false,
        enable_f_strings: false,
        allow_load_private_symbols: false,
        allow_toplevel_rebinding: true,
        require_load_statements_first: false,
        allow_call_star_args: false,
        allow_load_duplicate_local_bindings: false,
        ..Dialect::Standard
    };
    let bzl_dialect = Dialect {
        enable_def: true,
        enable_lambda: true,
        enable_load: true,
        enable_keyword_only_arguments: true,
        enable_positional_only_arguments: false,
        enable_types: DialectTypes::Disable,
        enable_load_reexport: false,
        enable_top_level_stmt: false,
        enable_f_strings: false,
        allow_load_private_symbols: false,
        allow_toplevel_rebinding: false,
        require_load_statements_first: true,
        allow_call_star_args: true,
        allow_load_duplicate_local_bindings: false,
        ..Dialect::Standard
    };

    match file_type {
        StarlarkFileType::Buck => build_dialect,
        StarlarkFileType::Bzl => bzl_dialect,
        // BXL and PACKAGE files remain Buck2-specific even when Bazel mode is selected.
        StarlarkFileType::Bxl | StarlarkFileType::Package => {
            buck2_parser_dialect(file_type, disable_starlark_types)
        }
        StarlarkFileType::Json | StarlarkFileType::Toml => Dialect::Standard,
    }
}

fn buck2_parser_dialect(file_type: StarlarkFileType, disable_starlark_types: bool) -> Dialect {
    let enable_f_strings = buck2_core::is_open_source();
    let buck_dialect = Dialect {
        enable_def: false,
        enable_lambda: true,
        enable_load: true,
        enable_keyword_only_arguments: false,
        enable_types: DialectTypes::Disable,
        enable_load_reexport: false,
        enable_top_level_stmt: false,
        enable_f_strings,
        ..Dialect::Standard
    };
    let package_dialect = Dialect {
        enable_def: false,
        enable_lambda: true,
        enable_load: true,
        enable_keyword_only_arguments: false,
        enable_types: DialectTypes::Disable,
        enable_load_reexport: false,
        enable_top_level_stmt: false,
        enable_f_strings,
        ..Dialect::Standard
    };
    let bzl_dialect = Dialect {
        enable_def: true,
        enable_lambda: true,
        enable_load: true,
        enable_keyword_only_arguments: true,
        enable_types: if disable_starlark_types {
            DialectTypes::ParseOnly
        } else {
            DialectTypes::Enable
        },
        enable_load_reexport: false,
        enable_top_level_stmt: true,
        enable_f_strings,
        ..Dialect::Standard
    };
    let bxl_dialect = Dialect {
        enable_def: true,
        enable_lambda: true,
        enable_load: true,
        enable_keyword_only_arguments: true,
        enable_types: if disable_starlark_types {
            DialectTypes::ParseOnly
        } else {
            DialectTypes::Enable
        },
        enable_load_reexport: false,
        enable_top_level_stmt: true,
        enable_f_strings,
        ..Dialect::Standard
    };

    match file_type {
        StarlarkFileType::Bzl => bzl_dialect,
        StarlarkFileType::Buck => buck_dialect,
        StarlarkFileType::Package => package_dialect,
        StarlarkFileType::Bxl => bxl_dialect,
        StarlarkFileType::Json | StarlarkFileType::Toml => Dialect::Standard,
    }
}

#[cfg(test)]
mod tests {
    use starlark::syntax::AstModule;

    use super::*;

    fn expected_bazel_build_dialect() -> Dialect {
        Dialect {
            enable_def: false,
            enable_lambda: false,
            enable_load: true,
            enable_keyword_only_arguments: true,
            enable_positional_only_arguments: false,
            enable_types: DialectTypes::Disable,
            enable_load_reexport: false,
            enable_top_level_stmt: false,
            enable_f_strings: false,
            allow_load_private_symbols: false,
            allow_toplevel_rebinding: true,
            require_load_statements_first: false,
            allow_call_star_args: false,
            allow_load_duplicate_local_bindings: false,
            ..Dialect::Standard
        }
    }

    fn expected_bazel_bzl_dialect() -> Dialect {
        Dialect {
            enable_def: true,
            enable_lambda: true,
            enable_load: true,
            enable_keyword_only_arguments: true,
            enable_positional_only_arguments: false,
            enable_types: DialectTypes::Disable,
            enable_load_reexport: false,
            enable_top_level_stmt: false,
            enable_f_strings: false,
            allow_load_private_symbols: false,
            allow_toplevel_rebinding: false,
            require_load_statements_first: true,
            allow_call_star_args: true,
            allow_load_duplicate_local_bindings: false,
            ..Dialect::Standard
        }
    }

    fn parse(source: &str, dialect: &Dialect) -> Result<AstModule, String> {
        AstModule::parse("test.bzl", source.to_owned(), dialect).map_err(|e| e.to_string())
    }

    fn assert_parse_error(source: &str, dialect: &Dialect, expected: &str) {
        let error = parse(source, dialect).unwrap_err();
        assert!(
            error.contains(expected),
            "expected error containing `{expected}` for:\n{source}\nactual error:\n{error}"
        );
    }

    fn expected_buck_file_dialect() -> Dialect {
        Dialect {
            enable_def: false,
            enable_lambda: true,
            enable_load: true,
            enable_keyword_only_arguments: false,
            enable_positional_only_arguments: false,
            enable_types: DialectTypes::Disable,
            enable_load_reexport: false,
            enable_top_level_stmt: false,
            enable_f_strings: buck2_core::is_open_source(),
            ..Dialect::Standard
        }
    }

    fn expected_extension_dialect(disable_starlark_types: bool) -> Dialect {
        Dialect {
            enable_def: true,
            enable_lambda: true,
            enable_load: true,
            enable_keyword_only_arguments: true,
            enable_positional_only_arguments: false,
            enable_types: if disable_starlark_types {
                DialectTypes::ParseOnly
            } else {
                DialectTypes::Enable
            },
            enable_load_reexport: false,
            enable_top_level_stmt: true,
            enable_f_strings: buck2_core::is_open_source(),
            ..Dialect::Standard
        }
    }

    #[test]
    fn buck2_parser_dialects_match_the_existing_configuration() {
        for disable_starlark_types in [false, true] {
            let dialect = StarlarkDialect::Buck2;

            assert_eq!(
                expected_extension_dialect(disable_starlark_types),
                dialect
                    .parser_dialect(StarlarkFileType::Bzl, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_extension_dialect(disable_starlark_types),
                dialect
                    .parser_dialect(StarlarkFileType::Bxl, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_buck_file_dialect(),
                dialect
                    .parser_dialect(StarlarkFileType::Buck, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_buck_file_dialect(),
                dialect
                    .parser_dialect(StarlarkFileType::Package, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                Dialect::Standard,
                dialect
                    .parser_dialect(StarlarkFileType::Json, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                Dialect::Standard,
                dialect
                    .parser_dialect(StarlarkFileType::Toml, disable_starlark_types)
                    .unwrap()
            );
        }
    }

    #[test]
    fn buck2_debugger_dialect_matches_the_existing_configuration() {
        assert_eq!(
            Dialect {
                enable_def: true,
                enable_lambda: true,
                enable_load: true,
                enable_keyword_only_arguments: true,
                enable_types: DialectTypes::ParseOnly,
                enable_load_reexport: false,
                enable_top_level_stmt: true,
                ..Dialect::Standard
            },
            StarlarkDialect::Buck2.debugger_parser_dialect().unwrap()
        );
    }

    #[test]
    fn bazel_parser_dialects_match_the_pinned_configuration() {
        for disable_starlark_types in [false, true] {
            let dialect = StarlarkDialect::Bazel;
            assert_eq!(
                expected_bazel_build_dialect(),
                dialect
                    .parser_dialect(StarlarkFileType::Buck, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_bazel_bzl_dialect(),
                dialect
                    .parser_dialect(StarlarkFileType::Bzl, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_extension_dialect(disable_starlark_types),
                dialect
                    .parser_dialect(StarlarkFileType::Bxl, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                expected_buck_file_dialect(),
                dialect
                    .parser_dialect(StarlarkFileType::Package, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                Dialect::Standard,
                dialect
                    .parser_dialect(StarlarkFileType::Json, disable_starlark_types)
                    .unwrap()
            );
            assert_eq!(
                Dialect::Standard,
                dialect
                    .parser_dialect(StarlarkFileType::Toml, disable_starlark_types)
                    .unwrap()
            );
        }
    }

    #[test]
    fn bazel_build_parser_accepts_and_rejects_the_pinned_syntax() {
        let dialect = expected_bazel_build_dialect();
        parse(
            "load(\":defs.bzl\", \"rule\")\nxs = [x for x in [1, 2] if x]\nys = {x: x for x in xs}\nz = 1 if xs else 2\nrule(name = \"x\")",
            &dialect,
        )
        .unwrap();
        // Bazel BUILD files permit late loads and top-level rebinding.
        parse("x = 1\nx = 2\nload(\":defs.bzl\", \"rule\")", &dialect).unwrap();

        assert_parse_error("def f():\n  pass", &dialect, "not allowed in this dialect");
        assert_parse_error("x = lambda: 1", &dialect, "not allowed in this dialect");
        assert_parse_error("x: int = 1", &dialect, "type annotation");
        assert_parse_error("x = f\"{y}\"", &dialect, "f-string");
        assert_parse_error(
            "if True:\n  x = 1",
            &dialect,
            "`if` cannot be used outside `def`",
        );
        assert_parse_error(
            "for x in []:\n  pass",
            &dialect,
            "`for` cannot be used outside `def`",
        );
        assert_parse_error(
            "load(\":defs.bzl\", \"_private\")",
            &dialect,
            "is private and cannot be imported",
        );
        assert_parse_error(
            "f(*args)",
            &dialect,
            "`*args` call arguments are not allowed",
        );
        assert_parse_error(
            "f(**kwargs)",
            &dialect,
            "`**kwargs` call arguments are not allowed",
        );
    }

    #[test]
    fn bazel_bzl_parser_accepts_and_rejects_the_pinned_syntax() {
        let dialect = expected_bazel_bzl_dialect();
        parse(
            "\"doc\"\nload(\":defs.bzl\", \"public\")\ndef f(x, *, y = 1):\n  return (lambda z: z)(x)\nf(*args, **kwargs)",
            &dialect,
        )
        .unwrap();

        assert_parse_error(
            "def f(x, /):\n  pass",
            &dialect,
            "positional-only-arguments is not allowed",
        );
        assert_parse_error("def f(x: int):\n  pass", &dialect, "type annotation");
        assert_parse_error("x = f\"{y}\"", &dialect, "f-string");
        assert_parse_error(
            "if True:\n  x = 1",
            &dialect,
            "`if` cannot be used outside `def`",
        );
        assert_parse_error(
            "for x in []:\n  pass",
            &dialect,
            "`for` cannot be used outside `def`",
        );
        assert_parse_error(
            "load(\":defs.bzl\", \"_private\")",
            &dialect,
            "is private and cannot be imported",
        );
        assert_parse_error(
            "x = 1\nload(\":defs.bzl\", \"public\")",
            &dialect,
            "load statements must appear before any other statement",
        );
        assert_parse_error("x = 1\nx = 2", &dialect, "redeclared at top level");
        assert_parse_error(
            "load(\":defs.bzl\", \"x\", x = \"y\")",
            &dialect,
            "load statement defines 'x' more than once",
        );
    }

    #[test]
    fn bazel_debugger_dialect_is_a_parser_only_union() {
        let dialect = StarlarkDialect::Bazel.debugger_parser_dialect().unwrap();
        assert_eq!(
            Dialect {
                enable_def: true,
                enable_lambda: true,
                enable_load: true,
                enable_keyword_only_arguments: true,
                enable_positional_only_arguments: false,
                enable_types: DialectTypes::ParseOnly,
                enable_load_reexport: false,
                enable_top_level_stmt: true,
                enable_f_strings: buck2_core::is_open_source(),
                allow_load_private_symbols: true,
                allow_toplevel_rebinding: true,
                require_load_statements_first: false,
                allow_call_star_args: true,
                allow_load_duplicate_local_bindings: true,
                ..Dialect::Standard
            },
            dialect
        );

        // The debugger does not know the source file kind while resolving breakpoints, so
        // this parser-only union must accept Buck2 BXL/PACKAGE syntax retained in Bazel mode.
        let buck_bxl_source = if buck2_core::is_open_source() {
            "x = 1\nif True:\n  x = 2\ndef typed(v: int) -> int:\n  return v\nmessage = f\"{x}\"\nload(\":defs.bzl\", \"_private\", _private = \"other\")"
        } else {
            "x = 1\nif True:\n  x = 2\ndef typed(v: int) -> int:\n  return v\nload(\":defs.bzl\", \"_private\", _private = \"other\")"
        };
        parse(buck_bxl_source, &dialect).unwrap();

        // It must simultaneously accept the syntax used by Bazel BUILD and .bzl files.
        parse(
            "x = 1\nx = 2\ndef f(v, *, default = 1):\n  return (lambda z: z)(v)\nf(*args, **kwargs)\nload(\":defs.bzl\", \"public\")",
            &dialect,
        )
        .unwrap();
    }

    #[test]
    fn starlark_dialect_config_values_are_strict() {
        assert_eq!(
            StarlarkDialect::Buck2,
            StarlarkDialect::from_config_value(None).unwrap()
        );
        assert_eq!(
            StarlarkDialect::Buck2,
            StarlarkDialect::from_config_value(Some("buck2")).unwrap()
        );
        assert_eq!(
            StarlarkDialect::Bazel,
            StarlarkDialect::from_config_value(Some("bazel")).unwrap()
        );

        let error = StarlarkDialect::from_config_value(Some("Bazel"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("[buck2] starlark_dialect"));
        assert!(error.contains("Expected one of `buck2` or `bazel`"));

        let error = StarlarkDialect::from_config_value(Some(""))
            .unwrap_err()
            .to_string();
        assert!(error.contains("Got ``"));
    }

    #[test]
    fn bazel_mode_is_explicitly_unavailable() {
        let error = StarlarkDialect::Bazel
            .require_available()
            .unwrap_err()
            .to_string();
        assert_eq!("Bazel Starlark dialect is not yet available", error);

        assert!(
            StarlarkDialect::Bazel
                .parser_dialect(StarlarkFileType::Buck, false)
                .is_ok()
        );
        assert!(StarlarkDialect::Bazel.debugger_parser_dialect().is_ok());
    }
}
