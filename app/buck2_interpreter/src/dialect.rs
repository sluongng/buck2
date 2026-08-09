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
            Self::Bazel => Err(Self::bazel_unavailable_error()),
        }
    }

    /// Dialect used when the debugger reparses source files for breakpoint resolution.
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
            Self::Bazel => Err(Self::bazel_unavailable_error()),
        }
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
    use super::*;

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

        let parser_error = StarlarkDialect::Bazel
            .parser_dialect(StarlarkFileType::Buck, false)
            .unwrap_err()
            .to_string();
        assert_eq!("Bazel Starlark dialect is not yet available", parser_error);

        let debugger_error = StarlarkDialect::Bazel
            .debugger_parser_dialect()
            .unwrap_err()
            .to_string();
        assert_eq!(
            "Bazel Starlark dialect is not yet available",
            debugger_error
        );
    }
}
