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

#[cfg(fbcode_build)]
mod fbcode {
    pub use scribe_client::ScribeConfig;

    pub use crate::sink::scribe::RemoteEventSink;
    pub(crate) use crate::sink::scribe::scribe_category;
}

#[cfg(not(fbcode_build))]
mod oss {
    use std::time::Duration;

    use crate::sink::bes::BesEventSink;

    // Alias for compatibility with existing code
    pub type RemoteEventSink = BesEventSink;

    #[derive(Default)]
    pub struct ScribeConfig {
        pub buffer_size: usize,
        pub retry_backoff: Duration,
        pub retry_attempts: usize,
        pub message_batch_size: Option<usize>,
        pub thrift_timeout: Duration,
    }
}

#[cfg(fbcode_build)]
pub use fbcode::*;
#[cfg(not(fbcode_build))]
pub use oss::*;

// Re-export BES components for OSS builds
#[cfg(not(fbcode_build))]
pub use crate::sink::bes::{BesConfig, BesEventSink};

/// Unified configuration for remote event sinks
pub enum RemoteEventSinkConfig {
    #[cfg(fbcode_build)]
    Scribe(ScribeConfig),
    #[cfg(not(fbcode_build))]
    Bes(BesConfig),
}

impl From<ScribeConfig> for RemoteEventSinkConfig {
    #[cfg(fbcode_build)]
    fn from(config: ScribeConfig) -> Self {
        RemoteEventSinkConfig::Scribe(config)
    }

    #[cfg(not(fbcode_build))]
    fn from(_config: ScribeConfig) -> Self {
        unreachable!("ScribeConfig should not be used in OSS builds")
    }
}

#[cfg(not(fbcode_build))]
impl From<BesConfig> for RemoteEventSinkConfig {
    fn from(config: BesConfig) -> Self {
        RemoteEventSinkConfig::Bes(config)
    }
}

/// Create a new remote event sink from configuration (lower-level interface)
pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: RemoteEventSinkConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if !is_enabled() {
        return Ok(None);
    }

    match config {
        #[cfg(fbcode_build)]
        RemoteEventSinkConfig::Scribe(scribe_config) => Ok(Some(RemoteEventSink::new(
            fb,
            scribe_category()?,
            scribe_config,
        )?)),
        #[cfg(not(fbcode_build))]
        RemoteEventSinkConfig::Bes(bes_config) => {
            let _ = fb; // fb not used in OSS builds
            Ok(Some(RemoteEventSink::new(bes_config)?))
        }
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
