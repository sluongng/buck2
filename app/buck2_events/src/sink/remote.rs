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
