/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::BTreeMap;
use std::fmt;
use std::net::IpAddr;
use std::path::Component;
use std::path::Path;

use allocative::Allocative;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use http::Uri;
use pagable::Pagable;

use crate::runtime::CredentialHelpersRuntimeConfig;

const BZLMOD_SECTION: &str = "bzlmod";
const DEFAULT_REGISTRY: &str = "https://bcr.bazel.build";
const DEFAULT_LOCKFILE: &str = "MODULE.buck2.lock";
const DEFAULT_REMOTE_USE_CASE: &str = "buck2-bzlmod";
const BZLMOD_KEYS: &[&str] = &[
    "enabled",
    "registries",
    "lockfile",
    "lock_mode",
    "repository_cache",
    "remote_repository_cache",
    "remote_repository_cache_use_case",
    "remote_repository_cache_trust_domain",
    "module_mirrors",
    "allow_insecure_http",
    "allow_private_network",
    "credential_helpers",
    "max_registry_file_bytes",
    "max_repository_download_bytes",
    "max_redirects",
    "credential_helper_timeout_seconds",
    "credential_helper_max_stdout_bytes",
    "credential_helper_max_stderr_bytes",
];

pub const DEFAULT_MAX_REGISTRY_FILE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_MAX_REGISTRY_FILE_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_REPOSITORY_DOWNLOAD_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const MAX_MAX_REPOSITORY_DOWNLOAD_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_REDIRECTS: u32 = 10;
pub const MAX_MAX_REDIRECTS: u32 = 32;
pub const DEFAULT_CREDENTIAL_HELPER_TIMEOUT_SECONDS: u64 = 10;
pub const MAX_CREDENTIAL_HELPER_TIMEOUT_SECONDS: u64 = 300;
pub const DEFAULT_CREDENTIAL_HELPER_MAX_STDOUT_BYTES: u64 = 65_536;
pub const DEFAULT_CREDENTIAL_HELPER_MAX_STDERR_BYTES: u64 = 65_536;
pub const MAX_CREDENTIAL_HELPER_OUTPUT_BYTES: u64 = 1024 * 1024;
pub const MAX_CREDENTIAL_HELPER_RESPONSE_HEADERS: usize = 64;
pub const MAX_CREDENTIAL_HELPER_HEADER_NAME_BYTES: usize = 8192;
pub const MAX_CREDENTIAL_HELPER_HEADER_VALUE_BYTES: usize = 8192;
pub const MAX_CREDENTIAL_HELPER_TOTAL_HEADER_BYTES: usize = 65_536;

/// Fully parsed configuration safe to inject into command-scoped DICE.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct BzlmodConfig {
    resolution: BzlmodResolutionConfig,
    transport: BzlmodTransportConfig,
    local_store: BzlmodLocalStoreConfig,
    remote_cache: BzlmodRemoteCacheConfig,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct BzlmodResolutionConfig {
    enabled: bool,
    registries: Box<[RepositoryUrl]>,
    lockfile: WorkspaceRelativePath,
    lock_mode: LockMode,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct BzlmodTransportConfig {
    module_mirrors: Box<[RepositoryUrl]>,
    allow_insecure_http: bool,
    allow_private_network: bool,
    credential_provider: CredentialProviderIdentity,
    max_registry_file_bytes: u64,
    max_repository_download_bytes: u64,
    max_redirects: u32,
    credential_helper_timeout_seconds: u64,
    credential_helper_max_stdout_bytes: u64,
    credential_helper_max_stderr_bytes: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct BzlmodLocalStoreConfig {
    repository_cache: RepositoryCacheLocation,
    authorization: CredentialProviderIdentity,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct BzlmodRemoteCacheConfig {
    mode: RemoteRepositoryCacheMode,
    use_case: Box<str>,
    trust_domain: Option<Box<str>>,
    authorization: CredentialProviderIdentity,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct CredentialProviderIdentity {
    provider_digest: Box<str>,
    authorization_domains: Box<[Box<str>]>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub enum RepositoryUrl {
    Network(Box<str>),
    WorkspaceFile(Box<str>),
    MachineFile(Box<str>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub struct WorkspaceRelativePath(Box<str>);

#[derive(Clone, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub enum RepositoryCacheLocation {
    PlatformDefault,
    Explicit(Box<str>),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub enum LockMode {
    Update,
    Refresh,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
pub enum RemoteRepositoryCacheMode {
    Disabled,
    Read,
    Write,
    ReadWrite,
}

/// Transient source values. This type deliberately has no `Debug`, `Eq`, or
/// `Hash` implementation because it may contain credential-helper paths.
pub struct BzlmodConfigInput {
    starlark_dialect: Option<InputValue>,
    bzlmod: BTreeMap<Box<str>, InputValue>,
}

struct InputValue {
    value: Box<str>,
    location: Box<str>,
}

/// The safe DICE value and the separate secret-adjacent command runtime data.
/// This type deliberately has no `Debug` implementation.
pub struct ParsedBzlmodConfig {
    config: BzlmodConfig,
    runtime: crate::BzlmodRuntimeConfig,
}

impl BzlmodConfigInput {
    pub fn from_legacy_config(config: &LegacyBuckConfig) -> Self {
        let starlark_dialect = input_value(config, "buck2", "starlark_dialect");
        let mut bzlmod = BTreeMap::new();
        if let Some(section) = config.get_section(BZLMOD_SECTION) {
            for (name, value) in section.iter() {
                bzlmod.insert(
                    name.into(),
                    InputValue {
                        value: value.as_str().into(),
                        location: value.location().to_string().into(),
                    },
                );
            }
        }
        Self {
            starlark_dialect,
            bzlmod,
        }
    }

    fn get(&self, name: &str) -> Option<&InputValue> {
        self.bzlmod.get(name)
    }
}

impl ParsedBzlmodConfig {
    pub fn from_legacy_config(config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        Self::parse(BzlmodConfigInput::from_legacy_config(config))
    }

    pub fn parse(input: BzlmodConfigInput) -> buck2_error::Result<Self> {
        reject_unknown_keys(&input)?;
        let enabled = parse_bool(&input, "enabled", false)?;
        let allow_insecure_http = parse_bool(&input, "allow_insecure_http", false)?;
        let allow_private_network = parse_bool(&input, "allow_private_network", false)?;
        let lock_mode = parse_lock_mode(input.get("lock_mode"))?;

        let registries = match input.get("registries") {
            Some(value) => parse_url_list(
                "registries",
                value,
                allow_insecure_http,
                allow_private_network,
                lock_mode,
                true,
            )?,
            None => vec![RepositoryUrl::Network(DEFAULT_REGISTRY.into())],
        };
        let module_mirrors = match input.get("module_mirrors") {
            Some(value) => parse_url_list(
                "module_mirrors",
                value,
                allow_insecure_http,
                allow_private_network,
                lock_mode,
                false,
            )?,
            None => Vec::new(),
        };
        let lockfile = parse_lockfile(input.get("lockfile"))?;
        let repository_cache = parse_repository_cache(input.get("repository_cache"))?;
        let remote_mode = parse_remote_mode(input.get("remote_repository_cache"))?;
        let remote_use_case_explicit = input.get("remote_repository_cache_use_case").is_some();
        let remote_use_case = parse_identifier(
            "remote_repository_cache_use_case",
            input.get("remote_repository_cache_use_case"),
            DEFAULT_REMOTE_USE_CASE,
            false,
        )?;
        let trust_domain = parse_optional_identifier(
            "remote_repository_cache_trust_domain",
            input.get("remote_repository_cache_trust_domain"),
        )?;
        let credential_helpers = match input.get("credential_helpers") {
            Some(value) => CredentialHelpersRuntimeConfig::parse(value.value.trim())
                .map_err(|error| invalid_value("credential_helpers", value, error))?,
            None => CredentialHelpersRuntimeConfig::empty(),
        };

        let max_registry_file_bytes = parse_bounded_u64(
            &input,
            "max_registry_file_bytes",
            DEFAULT_MAX_REGISTRY_FILE_BYTES,
            MAX_MAX_REGISTRY_FILE_BYTES,
        )?;
        let max_repository_download_bytes = parse_bounded_u64(
            &input,
            "max_repository_download_bytes",
            DEFAULT_MAX_REPOSITORY_DOWNLOAD_BYTES,
            MAX_MAX_REPOSITORY_DOWNLOAD_BYTES,
        )?;
        let max_redirects = parse_bounded_u64(
            &input,
            "max_redirects",
            DEFAULT_MAX_REDIRECTS.into(),
            MAX_MAX_REDIRECTS.into(),
        )? as u32;
        let credential_helper_timeout_seconds = parse_bounded_u64(
            &input,
            "credential_helper_timeout_seconds",
            DEFAULT_CREDENTIAL_HELPER_TIMEOUT_SECONDS,
            MAX_CREDENTIAL_HELPER_TIMEOUT_SECONDS,
        )?;
        let credential_helper_max_stdout_bytes = parse_bounded_u64(
            &input,
            "credential_helper_max_stdout_bytes",
            DEFAULT_CREDENTIAL_HELPER_MAX_STDOUT_BYTES,
            MAX_CREDENTIAL_HELPER_OUTPUT_BYTES,
        )?;
        let credential_helper_max_stderr_bytes = parse_bounded_u64(
            &input,
            "credential_helper_max_stderr_bytes",
            DEFAULT_CREDENTIAL_HELPER_MAX_STDERR_BYTES,
            MAX_CREDENTIAL_HELPER_OUTPUT_BYTES,
        )?;

        if enabled
            && input
                .starlark_dialect
                .as_ref()
                .map(|value| value.value.trim())
                != Some("bazel")
        {
            let configured = input
                .starlark_dialect
                .as_ref()
                .map(|value| value.value.as_ref())
                .unwrap_or("buck2 (default)");
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "buckconfig `bzlmod.enabled = true` requires `buck2.starlark_dialect = bazel`, got `{}`",
                configured
            ));
        }
        if remote_mode != RemoteRepositoryCacheMode::Disabled && !remote_use_case_explicit {
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "a non-disabled remote_repository_cache requires an explicitly configured remote_repository_cache_use_case"
            ));
        }
        if remote_mode != RemoteRepositoryCacheMode::Disabled
            && (!credential_helpers.is_empty()
                || allow_private_network
                || registries.iter().any(|registry| !registry.is_network()))
            && trust_domain.is_none()
        {
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "authenticated, private-network, and file repository content requires remote_repository_cache_trust_domain before remote repository cache reuse is enabled"
            ));
        }

        let credential_provider = credential_helpers.identity().clone();
        let config = BzlmodConfig {
            resolution: BzlmodResolutionConfig {
                enabled,
                registries: registries.into_boxed_slice(),
                lockfile,
                lock_mode,
            },
            transport: BzlmodTransportConfig {
                module_mirrors: module_mirrors.into_boxed_slice(),
                allow_insecure_http,
                allow_private_network,
                credential_provider: credential_provider.clone(),
                max_registry_file_bytes,
                max_repository_download_bytes,
                max_redirects,
                credential_helper_timeout_seconds,
                credential_helper_max_stdout_bytes,
                credential_helper_max_stderr_bytes,
            },
            local_store: BzlmodLocalStoreConfig {
                repository_cache,
                authorization: credential_provider.clone(),
            },
            remote_cache: BzlmodRemoteCacheConfig {
                mode: remote_mode,
                use_case: remote_use_case,
                trust_domain,
                authorization: credential_provider,
            },
        };
        Ok(Self {
            config,
            runtime: crate::BzlmodRuntimeConfig::new(credential_helpers),
        })
    }

    pub fn config(&self) -> &BzlmodConfig {
        &self.config
    }

    pub fn runtime(&self) -> &crate::BzlmodRuntimeConfig {
        &self.runtime
    }

    pub fn into_parts(self) -> (BzlmodConfig, crate::BzlmodRuntimeConfig) {
        (self.config, self.runtime)
    }
}

impl BzlmodConfig {
    pub fn resolution(&self) -> &BzlmodResolutionConfig {
        &self.resolution
    }

    pub fn transport(&self) -> &BzlmodTransportConfig {
        &self.transport
    }

    pub fn local_store(&self) -> &BzlmodLocalStoreConfig {
        &self.local_store
    }

    pub fn remote_cache(&self) -> &BzlmodRemoteCacheConfig {
        &self.remote_cache
    }
}

impl BzlmodResolutionConfig {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn registries(&self) -> &[RepositoryUrl] {
        &self.registries
    }

    pub fn lockfile(&self) -> &WorkspaceRelativePath {
        &self.lockfile
    }

    pub fn lock_mode(&self) -> LockMode {
        self.lock_mode
    }
}

impl BzlmodTransportConfig {
    pub fn module_mirrors(&self) -> &[RepositoryUrl] {
        &self.module_mirrors
    }

    pub fn allow_insecure_http(&self) -> bool {
        self.allow_insecure_http
    }

    pub fn allow_private_network(&self) -> bool {
        self.allow_private_network
    }

    pub fn credential_provider(&self) -> &CredentialProviderIdentity {
        &self.credential_provider
    }

    pub fn max_registry_file_bytes(&self) -> u64 {
        self.max_registry_file_bytes
    }

    pub fn max_repository_download_bytes(&self) -> u64 {
        self.max_repository_download_bytes
    }

    pub fn max_redirects(&self) -> u32 {
        self.max_redirects
    }

    pub fn credential_helper_timeout_seconds(&self) -> u64 {
        self.credential_helper_timeout_seconds
    }

    pub fn credential_helper_max_stdout_bytes(&self) -> u64 {
        self.credential_helper_max_stdout_bytes
    }

    pub fn credential_helper_max_stderr_bytes(&self) -> u64 {
        self.credential_helper_max_stderr_bytes
    }
}

impl BzlmodLocalStoreConfig {
    pub fn repository_cache(&self) -> &RepositoryCacheLocation {
        &self.repository_cache
    }

    pub fn authorization(&self) -> &CredentialProviderIdentity {
        &self.authorization
    }
}

impl BzlmodRemoteCacheConfig {
    pub fn mode(&self) -> RemoteRepositoryCacheMode {
        self.mode
    }

    pub fn use_case(&self) -> &str {
        &self.use_case
    }

    pub fn trust_domain(&self) -> Option<&str> {
        self.trust_domain.as_deref()
    }

    pub fn authorization(&self) -> &CredentialProviderIdentity {
        &self.authorization
    }
}

impl CredentialProviderIdentity {
    pub(crate) fn new(provider_digest: Box<str>, authorization_domains: Box<[Box<str>]>) -> Self {
        Self {
            provider_digest,
            authorization_domains,
        }
    }

    pub fn provider_digest(&self) -> &str {
        &self.provider_digest
    }

    pub fn authorization_domains(&self) -> &[Box<str>] {
        &self.authorization_domains
    }
}

impl RepositoryUrl {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Network(value) | Self::WorkspaceFile(value) | Self::MachineFile(value) => value,
        }
    }

    pub fn is_network(&self) -> bool {
        matches!(self, Self::Network(_))
    }

    pub fn is_workspace_file(&self) -> bool {
        matches!(self, Self::WorkspaceFile(_))
    }

    pub fn is_machine_file(&self) -> bool {
        matches!(self, Self::MachineFile(_))
    }
}

impl fmt::Display for RepositoryUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl WorkspaceRelativePath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceRelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn input_value(config: &LegacyBuckConfig, section: &str, property: &str) -> Option<InputValue> {
    config
        .get_section(section)
        .and_then(|section| section.get(property))
        .map(|value| InputValue {
            value: value.as_str().into(),
            location: value.location().to_string().into(),
        })
}

fn parse_bool(input: &BzlmodConfigInput, name: &str, default: bool) -> buck2_error::Result<bool> {
    let Some(value) = input.get(name) else {
        return Ok(default);
    };
    match value.value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid_value(name, value, "expected `true` or `false`")),
    }
}

fn parse_lockfile(value: Option<&InputValue>) -> buck2_error::Result<WorkspaceRelativePath> {
    let raw = value.map_or(DEFAULT_LOCKFILE, |value| value.value.trim());
    if raw.is_empty()
        || raw.contains('\\')
        || raw.contains("//")
        || raw.contains("/./")
        || raw.ends_with('/')
        || Path::new(raw).is_absolute()
        || Path::new(raw)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(value.map_or_else(
            || {
                buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "the default Bzlmod lockfile is not workspace-relative"
                )
            },
            |value| {
                invalid_value(
                    "lockfile",
                    value,
                    "expected a normalized workspace-relative path",
                )
            },
        ));
    }
    Ok(WorkspaceRelativePath(raw.into()))
}

fn parse_repository_cache(
    value: Option<&InputValue>,
) -> buck2_error::Result<RepositoryCacheLocation> {
    let Some(value) = value else {
        return Ok(RepositoryCacheLocation::PlatformDefault);
    };
    let path = value.value.trim();
    let mut components = Path::new(path).components();
    let normalized_absolute = matches!(components.next(), Some(Component::RootDir))
        && components.all(|component| matches!(component, Component::Normal(_)));
    if path.is_empty()
        || path == "/"
        || path.contains('\\')
        || path.contains("//")
        || path.contains("/./")
        || path.ends_with('/')
        || !normalized_absolute
    {
        return Err(invalid_value(
            "repository_cache",
            value,
            "expected a normalized non-root absolute path",
        ));
    }
    Ok(RepositoryCacheLocation::Explicit(path.into()))
}

fn parse_lock_mode(value: Option<&InputValue>) -> buck2_error::Result<LockMode> {
    let Some(value) = value else {
        return Ok(LockMode::Update);
    };
    match value.value.trim() {
        "update" => Ok(LockMode::Update),
        "refresh" => Ok(LockMode::Refresh),
        "error" => Ok(LockMode::Error),
        _ => Err(invalid_value(
            "lock_mode",
            value,
            "expected `update`, `refresh`, or `error`",
        )),
    }
}

fn parse_remote_mode(value: Option<&InputValue>) -> buck2_error::Result<RemoteRepositoryCacheMode> {
    let Some(value) = value else {
        return Ok(RemoteRepositoryCacheMode::Disabled);
    };
    match value.value.trim() {
        "disabled" => Ok(RemoteRepositoryCacheMode::Disabled),
        "read" => Ok(RemoteRepositoryCacheMode::Read),
        "write" => Ok(RemoteRepositoryCacheMode::Write),
        "read-write" => Ok(RemoteRepositoryCacheMode::ReadWrite),
        _ => Err(invalid_value(
            "remote_repository_cache",
            value,
            "expected `disabled`, `read`, `write`, or `read-write`",
        )),
    }
}

fn parse_identifier(
    name: &str,
    value: Option<&InputValue>,
    default: &str,
    allow_empty: bool,
) -> buck2_error::Result<Box<str>> {
    let raw = value.map_or(default, |value| value.value.trim());
    let first_and_last_are_alphanumeric = raw
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && raw.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric);
    if (!allow_empty && raw.is_empty())
        || raw.len() > 128
        || !first_and_last_are_alphanumeric
        || raw.split('.').any(str::is_empty)
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(value.map_or_else(
            || {
                buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "invalid compiled default for `{}`",
                    name
                )
            },
            |value| {
                invalid_value(
                    name,
                    value,
                    "expected 1-128 ASCII letters, digits, dots, hyphens, or underscores, starting and ending with an alphanumeric character and without empty dot-separated components",
                )
            },
        ));
    }
    Ok(raw.into())
}

fn parse_optional_identifier(
    name: &str,
    value: Option<&InputValue>,
) -> buck2_error::Result<Option<Box<str>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.value.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_identifier(name, Some(value), "", false)?))
}

fn parse_bounded_u64(
    input: &BzlmodConfigInput,
    name: &str,
    default: u64,
    maximum: u64,
) -> buck2_error::Result<u64> {
    let Some(value) = input.get(name) else {
        return Ok(default);
    };
    let parsed = value
        .value
        .trim()
        .parse::<u64>()
        .map_err(|_| invalid_value(name, value, "expected a positive decimal integer"))?;
    if parsed == 0 || parsed > maximum {
        return Err(invalid_value(
            name,
            value,
            format!("expected a value between 1 and {maximum}"),
        ));
    }
    Ok(parsed)
}

fn parse_url_list(
    name: &str,
    value: &InputValue,
    allow_insecure_http: bool,
    allow_private_network: bool,
    lock_mode: LockMode,
    allow_file: bool,
) -> buck2_error::Result<Vec<RepositoryUrl>> {
    if value.value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .value
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return Err(invalid_value(
                    name,
                    value,
                    "ordered URL lists may not contain empty entries",
                ));
            }
            parse_url(
                name,
                value,
                entry,
                allow_insecure_http,
                allow_private_network,
                lock_mode,
                allow_file,
            )
        })
        .collect()
}

fn parse_url(
    name: &str,
    input: &InputValue,
    value: &str,
    allow_insecure_http: bool,
    allow_private_network: bool,
    lock_mode: LockMode,
    allow_file: bool,
) -> buck2_error::Result<RepositoryUrl> {
    if value.contains('#') {
        return Err(invalid_value(name, input, "URL fragments are not allowed"));
    }
    if let Some(path) = value.strip_prefix("file:") {
        if !allow_file {
            return Err(invalid_value(
                name,
                input,
                "module mirrors must be network URLs; file registries are only valid in registries",
            ));
        }
        return parse_file_registry(name, input, value, path, lock_mode);
    }
    let uri = value
        .parse::<Uri>()
        .map_err(|_| invalid_value(name, input, "invalid URL"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| invalid_value(name, input, "URLs require a scheme"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| invalid_value(name, input, "URLs require an authority"))?;
    if authority.as_str().contains('@') {
        return Err(invalid_value(name, input, "URL userinfo is not allowed"));
    }
    if uri.query().is_some() {
        return Err(invalid_value(
            name,
            input,
            "URL query strings are not allowed",
        ));
    }
    let host = authority.host();
    if host.is_empty() {
        return Err(invalid_value(name, input, "URLs require a host"));
    }
    let loopback_host = is_loopback_host(host);
    match scheme {
        "https" => {}
        "http" if allow_insecure_http && loopback_host => {}
        "http" => {
            return Err(invalid_value(
                name,
                input,
                "plain HTTP requires allow_insecure_http=true and an explicit loopback host",
            ));
        }
        _ => {
            return Err(invalid_value(
                name,
                input,
                "only https URLs and policy-approved loopback http URLs are supported",
            ));
        }
    }
    if !allow_private_network && is_statically_non_public_host(host) {
        return Err(invalid_value(
            name,
            input,
            "private, loopback, link-local, multicast, unspecified, and other non-public literal hosts require allow_private_network=true",
        ));
    }
    Ok(RepositoryUrl::Network(value.into()))
}

fn parse_file_registry(
    name: &str,
    input: &InputValue,
    original: &str,
    value: &str,
    lock_mode: LockMode,
) -> buck2_error::Result<RepositoryUrl> {
    if value.contains('?') || value.contains('#') {
        return Err(invalid_value(
            name,
            input,
            "file registries may not contain query strings or fragments",
        ));
    }
    if value.contains('%') {
        return Err(invalid_value(
            name,
            input,
            "percent escapes are not supported in file registry paths",
        ));
    }
    if value.starts_with('/') && !(value.starts_with("///") && !value.starts_with("////")) {
        return Err(invalid_value(
            name,
            input,
            "machine-absolute file registries must use canonical file:///absolute/path syntax",
        ));
    }
    let path = if let Some(authority_and_path) = value.strip_prefix("//") {
        if !authority_and_path.starts_with('/') {
            return Err(invalid_value(
                name,
                input,
                "file registries may not contain an authority or userinfo",
            ));
        }
        authority_and_path
    } else {
        value
    };
    if path.is_empty() || path.contains('\\') {
        return Err(invalid_value(
            name,
            input,
            "file registries require a normalized path",
        ));
    }
    if Path::new(path).is_absolute() {
        let mut components = Path::new(path).components();
        let normalized = matches!(components.next(), Some(Component::RootDir))
            && components.all(|component| matches!(component, Component::Normal(_)))
            && path != "/"
            && !path.contains("//")
            && !path.contains("/./")
            && !path.ends_with('/');
        if !normalized {
            return Err(invalid_value(
                name,
                input,
                "machine-absolute file registries must use a normalized non-root path",
            ));
        }
        if lock_mode == LockMode::Error {
            return Err(invalid_value(
                name,
                input,
                "machine-absolute file registries are not portable and are rejected in lock_mode=error",
            ));
        }
        return Ok(RepositoryUrl::MachineFile(original.into()));
    }
    if Path::new(path)
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
        || path.contains("//")
        || path.contains("/./")
        || path.ends_with('/')
    {
        return Err(invalid_value(
            name,
            input,
            "workspace file registries must use a normalized workspace-relative path",
        ));
    }
    Ok(RepositoryUrl::WorkspaceFile(original.into()))
}

fn reject_unknown_keys(input: &BzlmodConfigInput) -> buck2_error::Result<()> {
    for (name, value) in &input.bzlmod {
        if !BZLMOD_KEYS.contains(&name.as_ref()) {
            return Err(invalid_value(
                name,
                value,
                "unknown Bzlmod configuration key",
            ));
        }
    }
    Ok(())
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']).trim_end_matches('.');
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| match ip {
            IpAddr::V4(ip) => ip.is_loopback(),
            IpAddr::V6(ip) => {
                ip.is_loopback() || ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback())
            }
        })
}

fn is_statically_non_public_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']).trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") || host.to_ascii_lowercase().ends_with(".localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| match ip {
        IpAddr::V4(ip) => is_non_public_ipv4(ip),
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || ip.to_ipv4_mapped().is_some_and(is_non_public_ipv4)
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    })
}

fn is_non_public_ipv4(ip: std::net::Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_unspecified()
        || ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_documentation()
        || octets[0] == 0
        || octets[0] >= 240
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
}

fn invalid_value(name: &str, value: &InputValue, message: impl fmt::Display) -> buck2_error::Error {
    buck2_error::buck2_error!(
        buck2_error::ErrorTag::Input,
        "invalid value for buckconfig `bzlmod.{}` {}: {}",
        name,
        value.location,
        message
    )
}

#[cfg(test)]
mod tests {
    use buck2_common::legacy_configs::configs::testing::parse;

    use super::*;

    fn parsed(contents: &str) -> buck2_error::Result<ParsedBzlmodConfig> {
        let legacy = parse(&[("config", contents)], "config")?;
        ParsedBzlmodConfig::from_legacy_config(&legacy)
    }

    #[test]
    fn freezes_disabled_defaults() -> buck2_error::Result<()> {
        let parsed = parsed("")?;
        let config = parsed.config();
        assert!(!config.resolution().enabled());
        assert_eq!(
            config.resolution().registries()[0].as_str(),
            DEFAULT_REGISTRY
        );
        assert_eq!(config.resolution().lockfile().as_str(), DEFAULT_LOCKFILE);
        assert_eq!(config.resolution().lock_mode(), LockMode::Update);
        assert_eq!(
            config.local_store().repository_cache(),
            &RepositoryCacheLocation::PlatformDefault
        );
        assert_eq!(
            config.remote_cache().mode(),
            RemoteRepositoryCacheMode::Disabled
        );
        assert_eq!(config.remote_cache().use_case(), DEFAULT_REMOTE_USE_CASE);
        assert!(config.transport().module_mirrors().is_empty());
        assert!(parsed.runtime().credential_helpers().is_empty());
        Ok(())
    }

    #[test]
    fn parses_full_contract() -> buck2_error::Result<()> {
        let parsed = parsed(
            "[buck2]\nstarlark_dialect = bazel\n[bzlmod]\nenabled = true\nregistries = https://one.example/r, https://two.example/r\nlockfile = locks/MODULE.lock\nlock_mode = error\nrepository_cache = /tmp/repositories\nremote_repository_cache = read-write\nremote_repository_cache_use_case = buck2-bzlmod-team\nremote_repository_cache_trust_domain = team\nmodule_mirrors = https://mirror.example/a\ncredential_helpers = *.example=/bin/wild,api.example=/bin/exact\nmax_registry_file_bytes = 1024\nmax_repository_download_bytes = 2048\nmax_redirects = 3\ncredential_helper_timeout_seconds = 4\ncredential_helper_max_stdout_bytes = 512\ncredential_helper_max_stderr_bytes = 256\n",
        )?;
        let config = parsed.config();
        assert!(config.resolution().enabled());
        assert_eq!(config.resolution().registries().len(), 2);
        assert_eq!(
            config.resolution().registries()[0].as_str(),
            "https://one.example/r"
        );
        assert_eq!(
            config.resolution().registries()[1].as_str(),
            "https://two.example/r"
        );
        assert_eq!(config.resolution().lock_mode(), LockMode::Error);
        assert_eq!(
            config.remote_cache().mode(),
            RemoteRepositoryCacheMode::ReadWrite
        );
        assert_eq!(config.remote_cache().trust_domain(), Some("team"));
        assert_eq!(config.transport().max_redirects(), 3);
        assert_eq!(
            parsed
                .runtime()
                .credential_helpers()
                .helper_for_host("api.example"),
            Some(Path::new("/bin/exact"))
        );
        assert!(
            !config
                .transport()
                .credential_provider()
                .provider_digest()
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn explicit_empty_registry_list_replaces_default() -> buck2_error::Result<()> {
        let parsed = parsed("[bzlmod]\nregistries =   \n")?;
        assert!(parsed.config().resolution().registries().is_empty());
        Ok(())
    }

    #[test]
    fn rejects_interior_empty_list_entries() {
        assert!(
            parsed("[bzlmod]\nregistries = https://one.example,,https://two.example\n").is_err()
        );
    }

    #[test]
    fn supports_typed_file_registries_with_lock_portability() -> buck2_error::Result<()> {
        let workspace = parsed("[bzlmod]\nregistries = file:third-party/registry\n")?;
        assert!(workspace.config().resolution().registries()[0].is_workspace_file());

        let machine = parsed("[bzlmod]\nregistries = file:///opt/bzl-registry\n")?;
        assert!(machine.config().resolution().registries()[0].is_machine_file());

        assert!(
            parsed("[bzlmod]\nlock_mode = error\nregistries = file:///opt/bzl-registry\n").is_err()
        );
        assert!(parsed("[bzlmod]\nregistries = file:../registry\n").is_err());
        assert!(parsed("[bzlmod]\nregistries = file:/opt/bzl-registry\n").is_err());
        assert!(parsed("[bzlmod]\nregistries = file:////opt/bzl-registry\n").is_err());
        assert!(parsed("[bzlmod]\nregistries = file://host/registry\n").is_err());
        for encoded in [
            "file:registry%2fsecret",
            "file:registry%2Fsecret",
            "file:registry%5csecret",
            "file:registry%5Csecret",
            "file:%2e%2e/registry",
            "file:registry%20name",
        ] {
            assert!(
                parsed(&format!("[bzlmod]\nregistries = {encoded}\n")).is_err(),
                "accepted {encoded:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn module_mirrors_are_network_only() {
        assert!(parsed("[bzlmod]\nmodule_mirrors = file:registry\n").is_err());
        assert!(parsed("[bzlmod]\nmodule_mirrors = file:///opt/registry\n").is_err());
        assert!(parsed("[bzlmod]\nmodule_mirrors = https://mirror.example\n").is_ok());
    }

    #[test]
    fn rejects_unknown_keys_with_source_context() {
        let error = match parsed("[bzlmod]\nmax_redirect = 2\n") {
            Ok(_) => panic!("accepted unknown key"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("max_redirect"));
        assert!(message.contains("config:2"));
    }

    #[test]
    fn repository_cache_requires_a_normalized_absolute_path() {
        for path in [
            "relative",
            "/",
            "/tmp/../cache",
            "/tmp/./cache",
            "/tmp//cache",
            "/tmp/cache/",
        ] {
            assert!(
                parsed(&format!("[bzlmod]\nrepository_cache = {path}\n")).is_err(),
                "accepted {path:?}"
            );
        }
        assert!(parsed("[bzlmod]\nrepository_cache = /tmp/cache\n").is_ok());
    }

    #[test]
    fn lockfile_requires_a_normalized_workspace_relative_path() {
        for path in [
            "",
            "/tmp/lock",
            "../lock",
            "locks/../lock",
            "locks/./lock",
            "locks//lock",
            "locks/",
            "locks\\lock",
        ] {
            assert!(
                parsed(&format!("[bzlmod]\nlockfile = {path}\n")).is_err(),
                "accepted {path:?}"
            );
        }
        assert!(parsed("[bzlmod]\nlockfile = locks/MODULE.lock\n").is_ok());
    }

    #[test]
    fn parses_malformed_values_even_when_disabled() {
        for setting in [
            "enabled = false\nlock_mode = maybe",
            "enabled = false\nmax_redirects = 0",
            "enabled = false\nrepository_cache = relative",
            "enabled = false\nallow_private_network = maybe",
            "enabled = false\nregistries = https://user@example.com",
            "enabled = false\ncredential_helpers = example.com=relative",
        ] {
            assert!(
                parsed(&format!("[bzlmod]\n{setting}\n")).is_err(),
                "accepted {setting:?}"
            );
        }
    }

    #[test]
    fn enabled_requires_bazel_dialect() {
        assert!(parsed("[bzlmod]\nenabled = true\n").is_err());
        assert!(parsed("[buck2]\nstarlark_dialect = buck2\n[bzlmod]\nenabled = true\n").is_err());
    }

    #[test]
    fn enforces_static_url_policy() {
        for config in [
            "[bzlmod]\nregistries = http://example.com",
            "[bzlmod]\nallow_insecure_http = true\nregistries = http://example.com",
            "[bzlmod]\nregistries = https://example.com/path?q=secret",
            "[bzlmod]\nregistries = https://example.com/path#fragment",
            "[bzlmod]\nregistries = https://127.0.0.1",
            "[bzlmod]\nregistries = https://[::ffff:127.0.0.1]",
        ] {
            assert!(parsed(config).is_err(), "accepted {config:?}");
        }
        assert!(parsed("[bzlmod]\nallow_insecure_http = true\nallow_private_network = true\nregistries = http://127.0.0.1:8080\n").is_ok());
        assert!(
            parsed("[bzlmod]\nallow_private_network = true\nregistries = https://10.0.0.1\n")
                .is_ok()
        );
    }

    #[test]
    fn malformed_urls_do_not_leak_secret_bearing_input() {
        let error = match parsed(
            "[bzlmod]\nregistries = https://registry-user:super-secret token@example.com\n",
        ) {
            Ok(_) => panic!("accepted malformed URL"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("invalid URL"));
        assert!(!message.contains("registry-user"));
        assert!(!message.contains("super-secret"));
    }

    #[test]
    fn remote_cache_requires_explicit_use_case() {
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\n").is_err());
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod\n").is_ok());
    }

    #[test]
    fn authenticated_remote_cache_requires_trust_domain() {
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod\ncredential_helpers = default=/bin/helper\n").is_err());
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod-team\nremote_repository_cache_trust_domain = team\ncredential_helpers = default=/bin/helper\n").is_ok());
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod\nregistries = file:registry\n").is_err());
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod\nallow_private_network = true\n").is_err());
        assert!(parsed("[bzlmod]\nremote_repository_cache = read\nremote_repository_cache_use_case = buck2-bzlmod-team\nremote_repository_cache_trust_domain = team\nregistries = file:registry\n").is_ok());
    }

    #[test]
    fn rejects_unsafe_remote_namespace_identifiers() {
        for value in ["..", ".team", "team.", "team..one", "team/one"] {
            assert!(
                parsed(&format!(
                    "[bzlmod]\nremote_repository_cache_trust_domain = {value}\n"
                ))
                .is_err(),
                "accepted {value:?}"
            );
        }
    }

    #[test]
    fn enforces_positive_compiled_resource_bounds() {
        for (name, maximum) in [
            ("max_registry_file_bytes", MAX_MAX_REGISTRY_FILE_BYTES),
            (
                "max_repository_download_bytes",
                MAX_MAX_REPOSITORY_DOWNLOAD_BYTES,
            ),
            ("max_redirects", MAX_MAX_REDIRECTS.into()),
            (
                "credential_helper_timeout_seconds",
                MAX_CREDENTIAL_HELPER_TIMEOUT_SECONDS,
            ),
            (
                "credential_helper_max_stdout_bytes",
                MAX_CREDENTIAL_HELPER_OUTPUT_BYTES,
            ),
            (
                "credential_helper_max_stderr_bytes",
                MAX_CREDENTIAL_HELPER_OUTPUT_BYTES,
            ),
        ] {
            assert!(parsed(&format!("[bzlmod]\n{name} = {maximum}\n")).is_ok());
            assert!(parsed(&format!("[bzlmod]\n{name} = 0\n")).is_err());
            assert!(parsed(&format!("[bzlmod]\n{name} = {}\n", maximum + 1)).is_err());
        }
    }

    #[test]
    fn credential_paths_do_not_enter_safe_config_debug() -> buck2_error::Result<()> {
        let parsed = parsed("[bzlmod]\ncredential_helpers = example.com=/very/secret/helper\n")?;
        let debug = format!("{:?}", parsed.config());
        assert!(!debug.contains("/very/secret/helper"));
        assert!(debug.contains("provider_digest"));
        Ok(())
    }
}
