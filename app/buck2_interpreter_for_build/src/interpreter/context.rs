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
use derive_more::Display;
use dice::DiceComputations;
use dice::DiceTransactionUpdater;
use dice::InjectedKey;
use dice::PagableValueSerialize;
use dice::ValueSerialize;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::interpreter::configuror::BuildInterpreterConfiguror;

#[derive(Clone, Dupe, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
#[display("{:?}", self)]
#[pagable_typetag(dice::DiceKeyDyn)]
struct BuildContextKey();

impl InjectedKey for BuildContextKey {
    type Value = Arc<BuildInterpreterConfiguror>;

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

#[async_trait]
pub trait HasInterpreterContext<'d> {
    async fn get_interpreter_configuror(
        &mut self,
    ) -> buck2_error::Result<&'d Arc<BuildInterpreterConfiguror>>;
}

#[async_trait]
impl<'d> HasInterpreterContext<'d> for DiceComputations<'d> {
    async fn get_interpreter_configuror(
        &mut self,
    ) -> buck2_error::Result<&'d Arc<BuildInterpreterConfiguror>> {
        Ok(self.compute(&BuildContextKey()).await?)
    }
}

pub trait SetInterpreterContext {
    fn set_interpreter_context(
        &mut self,
        interpreter_configuror: Arc<BuildInterpreterConfiguror>,
    ) -> buck2_error::Result<()>;
}

impl SetInterpreterContext for DiceTransactionUpdater {
    fn set_interpreter_context(
        &mut self,
        interpreter_configuror: Arc<BuildInterpreterConfiguror>,
    ) -> buck2_error::Result<()> {
        Ok(self.changed_to(vec![(BuildContextKey(), interpreter_configuror)])?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use buck2_core::pattern::pattern::InferTargetNames;
    use buck2_core::target::label::interner::ConcurrentTargetLabelInterner;
    use buck2_interpreter::dialect::StarlarkDialect;
    use buck2_interpreter::extra::InterpreterHostArchitecture;
    use buck2_interpreter::extra::InterpreterHostPlatform;

    use super::*;

    fn configuror(
        starlark_dialect: StarlarkDialect,
        global_target_interner: Arc<ConcurrentTargetLabelInterner>,
    ) -> Arc<BuildInterpreterConfiguror> {
        BuildInterpreterConfiguror::new(
            starlark_dialect,
            None,
            InterpreterHostPlatform::Linux,
            InterpreterHostArchitecture::X86_64,
            None,
            false,
            false,
            InferTargetNames::No,
            None,
            global_target_interner,
        )
        .unwrap()
    }

    #[test]
    fn starlark_dialect_participates_in_dice_identity() {
        let global_target_interner = Arc::new(ConcurrentTargetLabelInterner::default());
        let buck2 = configuror(StarlarkDialect::Buck2, global_target_interner.clone());
        let buck2_again = configuror(StarlarkDialect::Buck2, global_target_interner.clone());
        let bazel = configuror(StarlarkDialect::Bazel, global_target_interner);

        assert!(BuildContextKey::equality(&buck2, &buck2_again));
        assert!(!BuildContextKey::equality(&buck2, &bazel));
    }
}
