/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_common::dice::cells::HasCellResolver;
use buck2_core::cells::CellResolver;
use buck2_interpreter::dialect::StarlarkDialect;
use buck2_interpreter::dice::starlark_types::GetStarlarkTypes;
use buck2_interpreter::file_type::StarlarkFileType;
use buck2_interpreter::paths::path::StarlarkPath;
use dice::DiceComputations;
use dice::Key;
use dice::OkPagableValueSerialize;
use dice::ValueSerialize;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use dupe::ResultDupedErrExt;
use pagable::Pagable;
use pagable::pagable_typetag;
use starlark::environment::GlobalFrozenHeapName;
use starlark::environment::Globals;

use crate::interpreter::configuror::BuildInterpreterConfiguror;
use crate::interpreter::context::HasInterpreterContext;
use crate::interpreter::globals::base_globals;
use crate::interpreter::globals::bazel_build_globals;
use crate::interpreter::globals::bazel_bzl_globals;

pagable::static_str!(BUCK2_GLOBAL_ENV_HEAP_NAME = concat!(module_path!(), "::buck2_global_env"));
pagable::static_str!(
    BAZEL_BUILD_GLOBAL_ENV_HEAP_NAME = concat!(module_path!(), "::bazel_build_global_env")
);
pagable::static_str!(
    BAZEL_BZL_GLOBAL_ENV_HEAP_NAME = concat!(module_path!(), "::bazel_bzl_global_env")
);

/// Information shared across interpreters. Contains no cell-specific
/// information.
#[derive(Allocative, pagable::Pagable)]
pub struct GlobalInterpreterState {
    pub cell_resolver: CellResolver,

    /// The GlobalEnvironment contains all the globally available symbols
    /// (primarily starlark stdlib and Buck-provided functions).
    pub global_env: Globals,

    /// Clean Bazel globals used only for user BUILD files when Bazel mode is selected.
    bazel_build_env: Globals,

    /// Clean Bazel globals used only for user `.bzl` files when Bazel mode is selected.
    bazel_bzl_env: Globals,

    /// Interpreter Configurer
    pub configuror: Arc<BuildInterpreterConfiguror>,

    /// Check types in Starlark (or just parse and ignore).
    pub disable_starlark_types: bool,

    /// Language dialect used to parse Starlark input files.
    pub starlark_dialect: StarlarkDialect,

    /// Static typechecking for bzl and bxl files.
    pub unstable_typecheck: bool,
}

impl GlobalInterpreterState {
    pub fn new(
        cell_resolver: CellResolver,
        interpreter_configuror: Arc<BuildInterpreterConfiguror>,
        starlark_dialect: StarlarkDialect,
        disable_starlark_types: bool,
        unstable_typecheck: bool,
    ) -> buck2_error::Result<Self> {
        if starlark_dialect == StarlarkDialect::Bazel
            && let Some(prelude) = interpreter_configuror.prelude_import()
            && prelude.prelude_cell() == cell_resolver.root_cell()
            && prelude.import_path().path_parent().path().is_empty()
        {
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Bazel Starlark mode requires the configured Buck2 prelude to use a dedicated cell or subdirectory; a root-level prelude would make every project file part of the trusted backend"
            ));
        }

        let global_env = base_globals()
            .with(|g| {
                if let Some(additional_globals) = interpreter_configuror.additional_globals() {
                    additional_globals.0.apply(g);
                }
            })
            .build_named(GlobalFrozenHeapName {
                name: BUCK2_GLOBAL_ENV_HEAP_NAME,
            });

        let bazel_build_env = bazel_build_globals().build_named(GlobalFrozenHeapName {
            name: BAZEL_BUILD_GLOBAL_ENV_HEAP_NAME,
        });
        let bazel_bzl_env = bazel_bzl_globals().build_named(GlobalFrozenHeapName {
            name: BAZEL_BZL_GLOBAL_ENV_HEAP_NAME,
        });

        Ok(Self {
            cell_resolver,
            global_env,
            bazel_build_env,
            bazel_bzl_env,
            configuror: interpreter_configuror,
            starlark_dialect,
            disable_starlark_types,
            unstable_typecheck,
        })
    }

    pub fn configuror(&self) -> &Arc<BuildInterpreterConfiguror> {
        &self.configuror
    }

    pub fn globals(&self, file_type: StarlarkFileType) -> &Globals {
        self.globals_for_dialect(file_type, self.starlark_dialect)
    }

    pub fn globals_for_dialect(
        &self,
        file_type: StarlarkFileType,
        effective_dialect: StarlarkDialect,
    ) -> &Globals {
        match (effective_dialect, file_type) {
            (_, StarlarkFileType::Module) => {
                unreachable!("MODULE.bazel files do not use build-file globals")
            }
            (StarlarkDialect::Bazel, StarlarkFileType::Buck) => &self.bazel_build_env,
            (StarlarkDialect::Bazel, StarlarkFileType::Bzl) => &self.bazel_bzl_env,
            _ => &self.global_env,
        }
    }

    pub fn is_trusted_prelude(&self, path: StarlarkPath<'_>) -> bool {
        self.configuror
            .prelude_import()
            .is_some_and(|prelude| prelude.is_prelude_path(path.path().as_ref()))
    }

    pub fn uses_bazel_user_environment(&self, path: StarlarkPath<'_>) -> bool {
        self.starlark_dialect == StarlarkDialect::Bazel
            && matches!(path, StarlarkPath::BuildFile(_) | StarlarkPath::LoadFile(_))
            && !self.is_trusted_prelude(path)
    }

    pub fn effective_dialect(&self, path: StarlarkPath<'_>) -> StarlarkDialect {
        if self.is_trusted_prelude(path) {
            StarlarkDialect::Buck2
        } else {
            self.starlark_dialect
        }
    }

    pub fn globals_for_path(&self, path: StarlarkPath<'_>) -> &Globals {
        if self.is_trusted_prelude(path) {
            self.buck2_globals()
        } else {
            self.globals(path.file_type())
        }
    }

    pub fn buck2_globals(&self) -> &Globals {
        &self.global_env
    }
}

#[async_trait]
pub trait HasGlobalInterpreterState<'d> {
    async fn get_global_interpreter_state(
        &mut self,
    ) -> buck2_error::Result<&'d Arc<GlobalInterpreterState>>;
}

#[async_trait]
impl<'d> HasGlobalInterpreterState<'d> for DiceComputations<'d> {
    async fn get_global_interpreter_state(
        &mut self,
    ) -> buck2_error::Result<&'d Arc<GlobalInterpreterState>> {
        #[derive(
            Clone,
            derive_more::Display,
            Dupe,
            Debug,
            Eq,
            Hash,
            PartialEq,
            Allocative,
            Pagable
        )]
        #[display("{:?}", self)]
        #[pagable_typetag(dice::DiceKeyDyn)]
        struct GisKey();

        #[async_trait]
        impl Key for GisKey {
            type Value = buck2_error::Result<Arc<GlobalInterpreterState>>;
            async fn compute(
                &self,
                ctx: &mut DiceComputations,
                _cancellation: &CancellationContext,
            ) -> Self::Value {
                let interpreter_configuror = ctx.get_interpreter_configuror().await?.dupe();
                let cell_resolver = ctx.get_cell_resolver().await?.dupe();
                let disable_starlark_types = ctx.get_disable_starlark_types().await?;
                let unstable_typecheck = ctx.get_unstable_typecheck().await?;
                let starlark_dialect = interpreter_configuror.starlark_dialect();

                Ok(Arc::new(GlobalInterpreterState::new(
                    cell_resolver,
                    interpreter_configuror,
                    starlark_dialect,
                    disable_starlark_types,
                    unstable_typecheck,
                )?))
            }

            fn equality(_: &Self::Value, _: &Self::Value) -> bool {
                false
            }

            fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
                OkPagableValueSerialize::<Self::Value>::new()
            }
        }

        self.compute(&GisKey()).await?.as_ref().duped_err()
    }
}
