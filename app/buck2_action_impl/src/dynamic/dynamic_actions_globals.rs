/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

use std::cell::OnceCell;

use buck2_build_api::interpreter::rule_defs::artifact::starlark_artifact::StarlarkArtifact;
use buck2_build_api::interpreter::rule_defs::artifact::starlark_artifact_value::StarlarkArtifactValue;
use buck2_build_api::interpreter::rule_defs::artifact::starlark_declared_artifact::StarlarkDeclaredArtifact;
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::none::NoneType;
use starlark::values::starlark_value_as_type::StarlarkValueAsType;
use starlark::values::typing::StarlarkCallable;
use starlark::values::FrozenValue;
use starlark_map::small_map::SmallMap;

use crate::dynamic::dynamic_actions::StarlarkDynamicActions;
use crate::dynamic::dynamic_actions_callable::DynamicActionsCallable;
use crate::dynamic::dynamic_actions_callable::FrozenStarlarkDynamicActionsCallable;

#[starlark_module]
pub(crate) fn register_dynamic_action(globals: &mut GlobalsBuilder) {
    /// Create new dynamic action callable. Returned object will be callable,
    /// and the result of calling it can be passed to `ctx.actions.dynamic_output_new`.
    fn dynamic_actions<'v>(
        #[starlark(require = named)] r#impl: StarlarkCallable<
            'v,
            (
                FrozenValue,
                SmallMap<StarlarkArtifact, StarlarkArtifactValue>,
                SmallMap<StarlarkArtifact, StarlarkDeclaredArtifact>,
                FrozenValue,
            ),
            NoneType,
        >,
    ) -> anyhow::Result<DynamicActionsCallable<'v>> {
        Ok(DynamicActionsCallable {
            implementation: r#impl,
            name: OnceCell::new(),
        })
    }

    const DynamicActions: StarlarkValueAsType<StarlarkDynamicActions> = StarlarkValueAsType::new();
    const DynamicActionsCallable: StarlarkValueAsType<FrozenStarlarkDynamicActionsCallable> =
        StarlarkValueAsType::new();
}
