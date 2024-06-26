/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

use buck2_common::legacy_configs::init::SystemWarningConfig;
use crossterm::style::Stylize;
use superconsole::Component;
use superconsole::Dimensions;
use superconsole::DrawMode;
use superconsole::Line;
use superconsole::Lines;
use superconsole::Span;

use crate::subscribers::system_warning::check_memory_pressure;
use crate::subscribers::system_warning::system_memory_exceeded_msg;

/// This component is used to display system warnings for a command e.g. memory pressure, low disk space etc.
pub(crate) struct SystemWarningComponent<'a, T> {
    pub(crate) last_snapshot_tuple: &'a Option<(T, buck2_data::Snapshot)>,
    pub(crate) system_warning_config: &'a SystemWarningConfig,
}

fn warning_styled(text: &str) -> anyhow::Result<Line> {
    Ok(Line::from_iter([Span::new_styled(
        text.to_owned().yellow(),
    )?]))
}

impl<'a, T> Component for SystemWarningComponent<'a, T> {
    fn draw_unchecked(&self, _dimensions: Dimensions, _mode: DrawMode) -> anyhow::Result<Lines> {
        let mut lines = Vec::new();

        if let Some(memory_pressure) = check_memory_pressure(
            self.last_snapshot_tuple,
            self.system_warning_config.memory_pressure_threshold_percent,
        ) {
            lines.push(warning_styled(&system_memory_exceeded_msg(
                &memory_pressure,
            ))?);
        }

        Ok(Lines(lines))
    }
}
