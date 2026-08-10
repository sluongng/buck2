/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Typed, command-scoped configuration for Buck2's Bzlmod implementation.
//!
//! This crate deliberately contains no repository transport, cache, evaluator,
//! or server wiring. Credential-helper executable paths are retained only in
//! [`BzlmodRuntimeConfig`]; the DICE-safe configuration contains a provider
//! digest and authorization scopes instead.

mod config;
mod dice;
mod runtime;

pub use config::BzlmodConfig;
pub use config::BzlmodConfigInput;
pub use config::BzlmodLocalStoreConfig;
pub use config::BzlmodRemoteCacheConfig;
pub use config::BzlmodResolutionConfig;
pub use config::BzlmodTransportConfig;
pub use config::CredentialProviderIdentity;
pub use config::DEFAULT_CREDENTIAL_HELPER_MAX_STDERR_BYTES;
pub use config::DEFAULT_CREDENTIAL_HELPER_MAX_STDOUT_BYTES;
pub use config::DEFAULT_CREDENTIAL_HELPER_TIMEOUT_SECONDS;
pub use config::DEFAULT_MAX_REDIRECTS;
pub use config::DEFAULT_MAX_REGISTRY_FILE_BYTES;
pub use config::DEFAULT_MAX_REPOSITORY_DOWNLOAD_BYTES;
pub use config::LockMode;
pub use config::MAX_CREDENTIAL_HELPER_HEADER_NAME_BYTES;
pub use config::MAX_CREDENTIAL_HELPER_HEADER_VALUE_BYTES;
pub use config::MAX_CREDENTIAL_HELPER_OUTPUT_BYTES;
pub use config::MAX_CREDENTIAL_HELPER_RESPONSE_HEADERS;
pub use config::MAX_CREDENTIAL_HELPER_TIMEOUT_SECONDS;
pub use config::MAX_CREDENTIAL_HELPER_TOTAL_HEADER_BYTES;
pub use config::MAX_MAX_REDIRECTS;
pub use config::MAX_MAX_REGISTRY_FILE_BYTES;
pub use config::MAX_MAX_REPOSITORY_DOWNLOAD_BYTES;
pub use config::ParsedBzlmodConfig;
pub use config::RemoteRepositoryCacheMode;
pub use config::RepositoryCacheLocation;
pub use config::RepositoryUrl;
pub use config::WorkspaceRelativePath;
pub use dice::HasBzlmodConfig;
pub use dice::SetBzlmodConfig;
pub use runtime::BzlmodRuntimeConfig;
pub use runtime::CredentialHelpersRuntimeConfig;
pub use runtime::CredentialHelpersSummary;
pub use runtime::HasBzlmodRuntimeConfig;
pub use runtime::SetBzlmodRuntimeConfig;
