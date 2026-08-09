/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_core::bzl::ImportPath;
use buck2_core::cells::build_file_cell::BuildFileCell;
use buck2_hash::IntentionallyStdHashSet;
use buck2_interpreter::dialect::StarlarkDialect;
use buck2_interpreter::file_type::StarlarkFileType;
use buck2_interpreter::import_paths::HasImportPaths;
use buck2_interpreter::load_module::INTERPRETER_CALCULATION_IMPL;
use buck2_interpreter::load_module::InterpreterCalculation;
use buck2_interpreter::paths::path::StarlarkPath;
use buck2_interpreter::prelude_path::PreludePath;
use buck2_interpreter_for_build::interpreter::global_interpreter_state::HasGlobalInterpreterState;
use dice::DiceComputations;
use dice::DiceTransaction;
use starlark::environment::Globals;

/// The environment in which a Starlark file is evaluated.
pub(crate) struct Environment {
    /// The globals that are driven from Rust.
    pub(crate) globals: Globals,
    /// The effective dialect for this path. Configured prelude sources remain
    /// trusted Buck2 implementation files even when user files select Bazel.
    pub(crate) effective_dialect: StarlarkDialect,
    /// The path to the prelude, if the prelude is loaded in this file.
    /// Note that in a BUCK file the `native` value is also exploded into the top-level.
    prelude: Option<PreludePath>,
    /// A path that is implicitly loaded as additional globals.
    preload: Option<ImportPath>,
}

impl Environment {
    pub(crate) async fn new(
        path: StarlarkPath<'_>,
        dice: &mut DiceComputations<'_>,
    ) -> buck2_error::Result<Environment> {
        let cell = path.cell();
        let path_type = path.file_type();
        let calculation = INTERPRETER_CALCULATION_IMPL.get()?;

        // Configured prelude load files remain trusted Buck implementation
        // sources even when user BUILD and `.bzl` files select Bazel.
        let configured_prelude = calculation.prelude_import(dice).await?;
        let effective_dialect = dice
            .get_global_interpreter_state()
            .await?
            .effective_dialect(path);

        // Find the information from the globals.
        let globals = calculation
            .global_env_for_file_type(dice, path_type, effective_dialect)
            .await?;

        // Bazel BUILD and .bzl files use a deliberately closed environment.
        // In particular, Buck's prelude and root imports must not leak rule or
        // helper names into lint/typecheck merely because the project defines
        // them. BXL and PACKAGE retain their existing Buck semantics.
        let use_buck_implicit_imports = effective_dialect != StarlarkDialect::Bazel
            || !matches!(path_type, StarlarkFileType::Buck | StarlarkFileType::Bzl);

        // Next grab the prelude, unless we are in the prelude cell and not a build file
        let prelude = if use_buck_implicit_imports {
            match configured_prelude {
                Some(prelude)
                    if path_type == StarlarkFileType::Buck
                        || prelude.import_path().cell() != cell =>
                {
                    Some(prelude)
                }
                _ => None,
            }
        } else {
            None
        };

        // Now grab the pre-load things
        let preload = if use_buck_implicit_imports {
            dice.import_paths_for_cell(BuildFileCell::new(cell))
                .await?
                .root_import()
                .cloned()
        } else {
            None
        };

        Ok(Environment {
            globals,
            effective_dialect,
            prelude,
            preload,
        })
    }

    pub(crate) async fn get_names(
        &self,
        path_type: StarlarkFileType,
        dice: &DiceTransaction,
    ) -> buck2_error::Result<IntentionallyStdHashSet<String>> {
        let mut dice = dice.ctx();
        let mut names = IntentionallyStdHashSet::new();

        for x in self.globals.names() {
            names.insert(x.as_str().to_owned());
        }

        if let Some(prelude) = &self.prelude {
            let m = dice
                .get_loaded_module_from_import_path(prelude.import_path())
                .await?;
            for x in m.env().names() {
                names.insert(x.as_str().to_owned());
            }
            if path_type == StarlarkFileType::Buck {
                for (name, _value) in m.extra_globals_from_prelude_for_buck_files()? {
                    names.insert(name.to_owned());
                }
            }
        }

        if let Some(preload) = &self.preload {
            let m = dice.get_loaded_module_from_import_path(preload).await?;
            for x in m.env().names() {
                names.insert(x.as_str().to_owned());
            }
        }

        Ok(names)
    }
}
