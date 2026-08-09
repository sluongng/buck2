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
}

impl StarlarkDialect {
    pub fn parser_dialect(
        self,
        file_type: StarlarkFileType,
        disable_starlark_types: bool,
    ) -> Dialect {
        match self {
            Self::Buck2 => buck2_parser_dialect(file_type, disable_starlark_types),
        }
    }

    /// Dialect used when the debugger reparses source files for breakpoint resolution.
    pub fn debugger_parser_dialect(self) -> Dialect {
        match self {
            Self::Buck2 => Dialect {
                enable_def: true,
                enable_lambda: true,
                enable_load: true,
                enable_keyword_only_arguments: true,
                enable_types: DialectTypes::ParseOnly,
                enable_load_reexport: false,
                enable_top_level_stmt: true,
                ..Dialect::Standard
            },
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
                dialect.parser_dialect(StarlarkFileType::Bzl, disable_starlark_types)
            );
            assert_eq!(
                expected_extension_dialect(disable_starlark_types),
                dialect.parser_dialect(StarlarkFileType::Bxl, disable_starlark_types)
            );
            assert_eq!(
                expected_buck_file_dialect(),
                dialect.parser_dialect(StarlarkFileType::Buck, disable_starlark_types)
            );
            assert_eq!(
                expected_buck_file_dialect(),
                dialect.parser_dialect(StarlarkFileType::Package, disable_starlark_types)
            );
            assert_eq!(
                Dialect::Standard,
                dialect.parser_dialect(StarlarkFileType::Json, disable_starlark_types)
            );
            assert_eq!(
                Dialect::Standard,
                dialect.parser_dialect(StarlarkFileType::Toml, disable_starlark_types)
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
            StarlarkDialect::Buck2.debugger_parser_dialect()
        );
    }
}
