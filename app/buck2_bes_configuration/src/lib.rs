/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! BES (Build Event Service) configuration for Buck2
//!
//! This crate provides configuration support for BES, which is used in OSS builds.
//! For fbcode builds, Scribe is used instead of BES, so BES configuration is always disabled.

use std::str::FromStr;
use std::time::SystemTime;

use allocative::Allocative;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;

static BUCK2_BES_CFG_SECTION: &str = "buck2_bes";

/// Common interface for BES configuration that both fbcode and OSS builds implement.
/// Note: BES is only actually used in OSS builds. Fbcode builds use Scribe instead
/// and will always return disabled/None for BES configuration.
pub trait BesConfigurationImpl: Sized {
    fn from_legacy_config(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Self>;
    fn is_enabled(&self) -> bool;
    fn endpoint(&self) -> Option<&str>;
    fn project(&self) -> Option<&str>;
    fn build_id(&self) -> Option<&str>;
    fn invocation_id(&self) -> Option<&str>;
    fn buffer_size(&self) -> Option<usize>;
    fn retry_attempts(&self) -> Option<usize>;
    fn http_headers(&self) -> &[HttpHeader];
    fn connection_timeout_ms(&self) -> Option<u64>;
    fn request_timeout_ms(&self) -> Option<u64>;
    fn tls_enabled(&self) -> bool;
}

#[allow(unused)]
mod fbcode {
    use super::*;

    /// BES configuration for fbcode builds - BES is not used in fbcode (uses Scribe instead)
    /// This is a stub implementation that always returns disabled
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct BesConfiguration {
        // Empty struct - BES not supported in fbcode
    }

    impl BesConfigurationImpl for BesConfiguration {
        fn from_legacy_config(_legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
            // BES is not supported in fbcode builds - always return disabled configuration
            Ok(Self {})
        }

        fn is_enabled(&self) -> bool {
            // BES is never enabled in fbcode - use Scribe instead
            false
        }

        fn endpoint(&self) -> Option<&str> {
            None
        }

        fn project(&self) -> Option<&str> {
            None
        }

        fn build_id(&self) -> Option<&str> {
            None
        }

        fn invocation_id(&self) -> Option<&str> {
            None
        }

        fn buffer_size(&self) -> Option<usize> {
            None
        }

        fn retry_attempts(&self) -> Option<usize> {
            None
        }

        fn http_headers(&self) -> &[HttpHeader] {
            &[]
        }

        fn connection_timeout_ms(&self) -> Option<u64> {
            None
        }

        fn request_timeout_ms(&self) -> Option<u64> {
            None
        }

        fn tls_enabled(&self) -> bool {
            true
        }
    }
}

#[allow(unused)]
mod not_fbcode {
    use super::*;

    /// BES configuration for OSS builds - this is where BES is actually used
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct BesConfiguration(pub Buck2OssBesConfiguration);

    impl BesConfigurationImpl for BesConfiguration {
        fn from_legacy_config(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
            Ok(Self(Buck2OssBesConfiguration::from_legacy_config(
                legacy_config,
            )?))
        }

        fn is_enabled(&self) -> bool {
            self.0.enabled
        }

        fn endpoint(&self) -> Option<&str> {
            self.0.endpoint.as_deref()
        }

        fn project(&self) -> Option<&str> {
            self.0.project.as_deref()
        }

        fn build_id(&self) -> Option<&str> {
            self.0.build_id.as_deref()
        }

        fn invocation_id(&self) -> Option<&str> {
            self.0.invocation_id.as_deref()
        }

        fn buffer_size(&self) -> Option<usize> {
            self.0.buffer_size
        }

        fn retry_attempts(&self) -> Option<usize> {
            self.0.retry_attempts
        }

        fn http_headers(&self) -> &[HttpHeader] {
            &self.0.http_headers
        }

        fn connection_timeout_ms(&self) -> Option<u64> {
            self.0.connection_timeout_ms
        }

        fn request_timeout_ms(&self) -> Option<u64> {
            self.0.request_timeout_ms
        }

        fn tls_enabled(&self) -> bool {
            self.0.tls
        }
    }
}

/// Default constants for BES configuration
pub mod constants {
    /// Default buffer size for batching events
    pub const DEFAULT_BUFFER_SIZE: usize = 100;
    /// Default number of retry attempts for failed requests
    pub const DEFAULT_RETRY_ATTEMPTS: usize = 3;
    /// Default connection timeout in milliseconds
    pub const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 10000;
    /// Default request timeout in milliseconds
    pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30000;
    /// Default message size limit (1 MiB)
    pub const DEFAULT_MESSAGE_SIZE_LIMIT: usize = 1024 * 1024;
    /// Default maximum batch size for events
    pub const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
}

/// A configuration used only in our OSS builds. We still compile this always, which lets us
/// gate less code behind fbcode_build.
#[derive(Clone, Debug, Default, Allocative)]
pub struct Buck2OssBesConfiguration {
    /// Whether BES is enabled
    pub enabled: bool,
    /// Build Event Service endpoint URL
    pub endpoint: Option<String>,
    /// BES project identifier
    pub project: Option<String>,
    /// BES build identifier
    pub build_id: Option<String>,
    /// BES invocation identifier
    pub invocation_id: Option<String>,
    /// Buffer size for batching events
    pub buffer_size: Option<usize>,
    /// Number of retry attempts for failed requests
    pub retry_attempts: Option<usize>,
    /// Whether to use TLS (when endpoint is http:// vs https://)
    pub tls: bool,
    /// Additional HTTP headers to include in requests. This is a comma-separated list of `Header:
    /// Value` pairs. Minimal validation of those headers is done here.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub http_headers: Vec<HttpHeader>,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: Option<u64>,
    /// Request timeout in milliseconds
    pub request_timeout_ms: Option<u64>,
    /// Message size limit in bytes (configurable)
    pub message_size_limit: Option<usize>,
    /// Maximum batch size for events (configurable)
    pub max_batch_size: Option<usize>,
}

#[derive(Clone, Debug, Default, Allocative)]
pub struct HttpHeader {
    pub key: String,
    pub value: String,
}

impl FromStr for HttpHeader {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, ':');
        match (iter.next(), iter.next()) {
            (Some(key), Some(value)) => {
                let key = key.trim();
                let value = value.trim();
                if key.is_empty() {
                    return Err(buck2_error::buck2_error!(
                        buck2_error::ErrorTag::Input,
                        "Header key cannot be empty: `{}`",
                        s
                    ));
                }
                Ok(Self {
                    key: key.to_owned(),
                    value: value.to_owned(),
                })
            }
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid header (expect name and value separated by `:`): `{}`",
                s
            )),
        }
    }
}

impl Buck2OssBesConfiguration {
    pub fn from_legacy_config(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        let endpoint = legacy_config.parse(BuckconfigKeyRef {
            section: BUCK2_BES_CFG_SECTION,
            property: "endpoint",
        })?;

        // BES is enabled if endpoint is provided, unless explicitly disabled
        let enabled = if endpoint.is_some() {
            legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_BES_CFG_SECTION,
                    property: "enabled",
                })?
                .unwrap_or(true) // Default to enabled if endpoint is provided
        } else {
            false // Disabled if no endpoint
        };

        Ok(Self {
            enabled,
            endpoint,
            project: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "project",
            })?,
            build_id: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "build_id",
            })?,
            invocation_id: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "invocation_id",
            })?,
            buffer_size: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_BES_CFG_SECTION,
                    property: "buffer_size",
                })?
                .or(Some(constants::DEFAULT_BUFFER_SIZE)),
            retry_attempts: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_BES_CFG_SECTION,
                    property: "retry_attempts",
                })?
                .or(Some(constants::DEFAULT_RETRY_ATTEMPTS)),
            tls: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_BES_CFG_SECTION,
                    property: "tls",
                })?
                .unwrap_or(true),
            http_headers: legacy_config
                .parse_list(BuckconfigKeyRef {
                    section: BUCK2_BES_CFG_SECTION,
                    property: "http_headers",
                })?
                .unwrap_or_default(),
            connection_timeout_ms: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "connection_timeout_ms",
            })?,
            request_timeout_ms: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "request_timeout_ms",
            })?,
            message_size_limit: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "message_size_limit",
            })?,
            max_batch_size: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_BES_CFG_SECTION,
                property: "max_batch_size",
            })?,
        })
    }
}

#[cfg(fbcode_build)]
pub use fbcode::BesConfiguration;
#[cfg(not(fbcode_build))]
pub use not_fbcode::BesConfiguration;

/// Helper function to create BES configuration from legacy config
pub fn bes_config_from_legacy(
    legacy_config: &LegacyBuckConfig,
) -> buck2_error::Result<BesConfiguration> {
    BesConfiguration::from_legacy_config(legacy_config)
}

#[cfg(not(fbcode_build))]
impl BesConfiguration {
    /// Get BES configuration values with proper defaults for creating a BesConfig
    pub fn get_bes_config_values(
        &self,
        trace_id: Option<&str>,
    ) -> Option<(String, String, String, String)> {
        if !self.is_enabled() {
            return None;
        }

        let endpoint = self.endpoint()?.to_string();
        let project = self.project().unwrap_or("buck2").to_string();
        let build_id = match self.build_id() {
            Some(id) => id.to_string(),
            None => {
                // Generate a unique build ID if not provided
                format!(
                    "build-{}",
                    SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                )
            }
        };
        let invocation_id = match self.invocation_id().or(trace_id) {
            Some(id) => id.to_string(),
            None => {
                // Generate a unique invocation ID if not provided
                format!(
                    "invocation-{}",
                    SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                )
            }
        };

        Some((endpoint, project, build_id, invocation_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_header_parsing() {
        // Valid header
        let header = "Authorization: Bearer token123"
            .parse::<HttpHeader>()
            .unwrap();
        assert_eq!(header.key, "Authorization");
        assert_eq!(header.value, "Bearer token123");

        // Valid header with extra spaces
        let header = "  Content-Type  :  application/json  "
            .parse::<HttpHeader>()
            .unwrap();
        assert_eq!(header.key, "Content-Type");
        assert_eq!(header.value, "application/json");

        // Invalid header without colon
        assert!("Invalid Header".parse::<HttpHeader>().is_err());

        // Invalid header with empty key
        assert!(": value".parse::<HttpHeader>().is_err());
    }

    #[cfg(not(fbcode_build))]
    #[test]
    fn test_bes_configuration_with_empty_config() {
        use buck2_common::legacy_configs::configs::LegacyBuckConfig;

        // Test with empty config - should be disabled
        let config = LegacyBuckConfig::empty();
        let bes_config = BesConfiguration::from_legacy_config(&config).unwrap();
        assert!(!bes_config.is_enabled());
        assert!(bes_config.endpoint().is_none());
        assert_eq!(bes_config.buffer_size().unwrap(), 100); // Default
        assert_eq!(bes_config.retry_attempts().unwrap(), 3); // Default
        assert!(bes_config.tls_enabled()); // Default
    }

    #[cfg(not(fbcode_build))]
    #[test]
    fn test_bes_config_values_with_defaults() {
        // Test the get_bes_config_values function with mock BES configuration
        let bes_config = Buck2OssBesConfiguration {
            enabled: true,
            endpoint: Some("https://bes.example.com/stream".to_string()),
            project: Some("test-project".to_string()),
            build_id: None,
            invocation_id: None,
            buffer_size: Some(100),
            retry_attempts: Some(3),
            tls: true,
            http_headers: vec![],
            connection_timeout_ms: None,
            request_timeout_ms: None,
        };
        let wrapper = BesConfiguration(bes_config);

        let (endpoint, project, build_id, invocation_id) =
            wrapper.get_bes_config_values(Some("trace-123")).unwrap();

        assert_eq!(endpoint, "https://bes.example.com/stream");
        assert_eq!(project, "test-project");
        assert!(build_id.starts_with("build-"));
        assert_eq!(invocation_id, "trace-123");
    }

    #[cfg(not(fbcode_build))]
    #[test]
    fn test_bes_config_auto_generated_ids() {
        // Test auto-generation of build/invocation IDs
        let bes_config = Buck2OssBesConfiguration {
            enabled: true,
            endpoint: Some("https://bes.example.com/stream".to_string()),
            project: None,
            build_id: None,
            invocation_id: None,
            buffer_size: Some(100),
            retry_attempts: Some(3),
            tls: true,
            http_headers: vec![],
            connection_timeout_ms: None,
            request_timeout_ms: None,
        };
        let wrapper = BesConfiguration(bes_config);

        let (_, project, build_id, invocation_id) = wrapper.get_bes_config_values(None).unwrap();

        // Test defaults and auto-generation
        assert_eq!(project, "buck2");
        assert!(build_id.starts_with("build-"));
        assert!(invocation_id.starts_with("invocation-"));
    }
}
