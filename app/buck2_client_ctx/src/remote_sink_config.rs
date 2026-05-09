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
#[cfg(not(fbcode_build))]
use buck2_events::sink::remote::BesEventFormat;
use buck2_events::sink::remote::RemoteEventConfig;

#[cfg(not(fbcode_build))]
struct BuckconfigBesSettings {
    bes_backend: Option<String>,
    bes_headers: Vec<(String, String)>,
    bes_event_format: Option<BesEventFormat>,
    bazel_artifact_upload: Option<bool>,
    bazel_artifact_upload_backend: Option<String>,
    bazel_artifact_upload_instance_name: Option<String>,
    bazel_artifact_uri_authority: Option<String>,
    bazel_artifact_upload_max_bytes: Option<usize>,
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
            if let Some(bes_event_format) = settings.bes_event_format {
                config.event_format = bes_event_format;
            }
            if let Some(bazel_artifact_upload) = settings.bazel_artifact_upload {
                config.bazel_artifact_upload = bazel_artifact_upload;
            }
            if let Some(backend) = settings.bazel_artifact_upload_backend {
                config.bazel_artifact_upload_backend = Some(backend);
            }
            if let Some(instance_name) = settings.bazel_artifact_upload_instance_name {
                config.bazel_artifact_upload_instance_name = Some(instance_name);
            }
            if let Some(authority) = settings.bazel_artifact_uri_authority {
                config.bazel_artifact_uri_authority = Some(authority);
            }
            if let Some(max_bytes) = settings.bazel_artifact_upload_max_bytes {
                config.bazel_artifact_upload_max_bytes = max_bytes;
            }
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
            bes_event_format: None,
            bazel_artifact_upload: None,
            bazel_artifact_upload_backend: None,
            bazel_artifact_upload_instance_name: None,
            bazel_artifact_uri_authority: None,
            bazel_artifact_upload_max_bytes: None,
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
        bes_event_format: root_config.parse::<BesEventFormat>(BuckconfigKeyRef {
            section: "bes",
            property: "event_format",
        })?,
        bazel_artifact_upload: root_config.parse::<bool>(BuckconfigKeyRef {
            section: "bes",
            property: "bazel_artifact_upload",
        })?,
        bazel_artifact_upload_backend: root_config
            .get(BuckconfigKeyRef {
                section: "bes",
                property: "bazel_artifact_upload_backend",
            })
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
        bazel_artifact_upload_instance_name: root_config
            .get(BuckconfigKeyRef {
                section: "bes",
                property: "bazel_artifact_upload_instance_name",
            })
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
        bazel_artifact_uri_authority: root_config
            .get(BuckconfigKeyRef {
                section: "bes",
                property: "bazel_artifact_uri_authority",
            })
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
        bazel_artifact_upload_max_bytes: root_config.parse::<usize>(BuckconfigKeyRef {
            section: "bes",
            property: "bazel_artifact_upload_max_bytes",
        })?,
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
