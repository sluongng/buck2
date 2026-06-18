/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A Sink for forwarding events directly to Remote service.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use fbinit::FacebookInit;

#[cfg(not(fbcode_build))]
pub use crate::sink::scribe::BesEventFormat;
pub use crate::sink::scribe::RemoteEventConfig;
pub use crate::sink::scribe::RemoteEventSink;
pub(crate) use crate::sink::scribe::scribe_category;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    config: RemoteEventConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(not(fbcode_build))]
    if !config.bes_enabled() {
        return Ok(None);
    }
    Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: RemoteEventConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(fb, config)
    } else {
        Ok(None)
    }
}

/// Whether or not remote event logging is enabled for this process. It must be explicitly disabled via `disable()`.
static REMOTE_EVENT_SINK_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns whether this process should actually write to remote sink, even if it is fully supported by the platform and
/// binary.
pub fn is_enabled() -> bool {
    REMOTE_EVENT_SINK_ENABLED.load(Ordering::Relaxed)
}

/// Disables remote event logging for this process. Remote event logging must be disabled explicitly on startup, otherwise it is
/// on by default.
pub fn disable() {
    REMOTE_EVENT_SINK_ENABLED.store(false, Ordering::Relaxed);
}

#[cfg(not(fbcode_build))]
pub fn expand_bes_config_env_vars(raw: &str) -> String {
    expand_bes_config_env_vars_with(raw, |name| std::env::var(name).ok())
}

#[cfg(not(fbcode_build))]
pub fn expand_bes_config_env_vars_with<F>(raw: &str, mut env: F) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    let mut expanded = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '$' {
            expanded.push(ch);
            continue;
        }

        match chars.peek().copied() {
            Some('{') => {
                chars.next();
                let mut name = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '}' {
                        closed = true;
                        break;
                    }
                    name.push(c);
                }

                if closed && is_env_name(&name) {
                    expanded.push_str(&env(&name).unwrap_or_default());
                } else {
                    expanded.push('$');
                    expanded.push('{');
                    expanded.push_str(&name);
                    if closed {
                        expanded.push('}');
                    }
                }
            }
            Some(c) if is_env_name_start(c) => {
                let mut name = String::new();
                while let Some(c) = chars.peek().copied() {
                    if !is_env_name_char(c) {
                        break;
                    }
                    name.push(c);
                    chars.next();
                }
                expanded.push_str(&env(&name).unwrap_or_default());
            }
            _ => expanded.push('$'),
        }
    }

    expanded
}

#[cfg(not(fbcode_build))]
fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(is_env_name_start) && chars.all(is_env_name_char)
}

#[cfg(not(fbcode_build))]
fn is_env_name_start(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic()
}

#[cfg(not(fbcode_build))]
fn is_env_name_char(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

#[cfg(all(test, not(fbcode_build)))]
mod tests {
    use super::*;

    #[test]
    fn expands_bes_config_env_vars() {
        assert_eq!(
            expand_bes_config_env_vars_with(
                "key=$BUILDBUDDY_API_KEY,run=${BUILDBUDDY_RUN_ID},missing=$MISSING,literal=$9",
                test_env,
            ),
            "key=secret,run=run-id,missing=,literal=$9",
        );
    }

    fn test_env(name: &str) -> Option<String> {
        match name {
            "BUILDBUDDY_API_KEY" => Some("secret".to_owned()),
            "BUILDBUDDY_RUN_ID" => Some("run-id".to_owned()),
            _ => None,
        }
    }
}
