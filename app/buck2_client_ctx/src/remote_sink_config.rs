/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_common::invocation_paths::InvocationPaths;
#[cfg(not(fbcode_build))]
use buck2_error::ErrorTag;
use buck2_events::sink::remote::RemoteEventConfig;

#[cfg(not(fbcode_build))]
struct BuckconfigBesSettings {
    bes_backend: Option<String>,
    bes_headers: Vec<(String, String)>,
    bes_results_url: Option<String>,
}

#[cfg(not(fbcode_build))]
pub fn with_buckconfig_overrides(
    paths: Option<&InvocationPaths>,
    mut config: RemoteEventConfig,
) -> RemoteEventConfig {
    match read_buckconfig_bes_settings(paths) {
        Ok(settings) => {
            if let Some(bes_backend) = settings.bes_backend {
                config.bes_backend = Some(bes_backend);
            }
            config.bes_headers = settings.bes_headers;
            config
        }
        Err(e) => {
            tracing::warn!(
                "Failed to read BES buckconfig settings, falling back to defaults: {:#}",
                e
            );
            config
        }
    }
}

#[cfg(not(fbcode_build))]
pub fn bes_results_url(paths: Option<&InvocationPaths>) -> Option<String> {
    match read_buckconfig_bes_settings(paths) {
        Ok(settings) => settings.bes_results_url,
        Err(e) => {
            tracing::warn!(
                "Failed to read BES buckconfig settings, falling back to defaults: {:#}",
                e
            );
            None
        }
    }
}

#[cfg(fbcode_build)]
pub fn with_buckconfig_overrides(
    _paths: Option<&InvocationPaths>,
    config: RemoteEventConfig,
) -> RemoteEventConfig {
    config
}

#[cfg(fbcode_build)]
pub fn bes_results_url(_paths: Option<&InvocationPaths>) -> Option<String> {
    None
}

#[cfg(not(fbcode_build))]
fn read_buckconfig_bes_settings(
    paths: Option<&InvocationPaths>,
) -> buck2_error::Result<BuckconfigBesSettings> {
    use buck2_common::legacy_configs::cells::BuckConfigBasedCells;
    use buck2_common::legacy_configs::key::BuckconfigKeyRef;

    let Some(paths) = paths else {
        return Ok(BuckconfigBesSettings {
            bes_backend: None,
            bes_headers: Vec::new(),
            bes_results_url: None,
        });
    };
    let fs = paths.project_root();

    let legacy_cells =
        futures::executor::block_on(BuckConfigBasedCells::parse_with_config_args(fs, &[]))?;
    let cells = &legacy_cells.cell_resolver;
    let root_config =
        futures::executor::block_on(legacy_cells.parse_single_cell(cells.root_cell(), fs))?;

    let bes_results_url = root_config
        .get(BuckconfigKeyRef {
            section: "bes",
            property: "results_url",
        })
        .or_else(|| {
            root_config.get(BuckconfigKeyRef {
                section: "bes",
                property: "result_url",
            })
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    Ok(BuckconfigBesSettings {
        bes_backend: root_config
            .get(BuckconfigKeyRef {
                section: "bes",
                property: "backend",
            })
            .map(str::to_owned),
        bes_headers: parse_bes_headers(root_config.parse_list::<String>(BuckconfigKeyRef {
            section: "bes",
            property: "header",
        })?)?,
        bes_results_url,
    })
}

#[cfg(not(fbcode_build))]
fn parse_bes_headers(
    raw_headers: Option<Vec<String>>,
) -> buck2_error::Result<Vec<(String, String)>> {
    let mut headers = Vec::new();
    for raw_header in raw_headers.unwrap_or_default() {
        let (key, value) = raw_header.split_once('=').ok_or_else(|| {
            buck2_error::buck2_error!(
                ErrorTag::Input,
                "Invalid `bes.header` entry `{}` (expected `NAME=VALUE`)",
                raw_header
            )
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Input,
                "Invalid `bes.header` entry `{}` (header name is empty)",
                raw_header
            ));
        }
        headers.push((key.to_owned(), value.to_owned()));
    }
    Ok(headers)
}
