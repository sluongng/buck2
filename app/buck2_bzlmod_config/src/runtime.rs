/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::path::Path;
use std::path::PathBuf;

use dice::DiceComputations;
use dice::UserComputationData;
use sha2::Digest;
use sha2::Sha256;

use crate::CredentialProviderIdentity;
use crate::MachineRegistryIdentity;

/// Secret-adjacent command runtime data which must never be injected into DICE.
///
/// This type intentionally implements neither `Debug`, `Eq`, `Hash`,
/// `Allocative`, nor `Pagable`. Callers can inspect only a redacted summary and
/// select the helper for a request host or resolve an opaque machine-registry
/// identity to its normalized path.
pub struct BzlmodRuntimeConfig {
    credential_helpers: CredentialHelpersRuntimeConfig,
    machine_registries: MachineRegistriesRuntimeConfig,
}

impl BzlmodRuntimeConfig {
    pub(crate) fn new(
        credential_helpers: CredentialHelpersRuntimeConfig,
        machine_registries: MachineRegistriesRuntimeConfig,
    ) -> Self {
        Self {
            credential_helpers,
            machine_registries,
        }
    }

    pub fn credential_helpers(&self) -> &CredentialHelpersRuntimeConfig {
        &self.credential_helpers
    }

    /// Resolve a safe machine-registry identity to its command-scoped path.
    pub fn machine_registry_path(&self, identity: &MachineRegistryIdentity) -> Option<&Path> {
        self.machine_registries.path(identity)
    }
}

/// Ordered machine-registry paths retained outside DICE and diagnostics.
pub(crate) struct MachineRegistriesRuntimeConfig {
    entries: Box<[MachineRegistryRuntimeEntry]>,
}

struct MachineRegistryRuntimeEntry {
    identity: MachineRegistryIdentity,
    path: PathBuf,
}

impl MachineRegistriesRuntimeConfig {
    pub(crate) fn from_entries(entries: Vec<(MachineRegistryIdentity, PathBuf)>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(identity, path)| MachineRegistryRuntimeEntry { identity, path })
                .collect(),
        }
    }

    fn path(&self, identity: &MachineRegistryIdentity) -> Option<&Path> {
        self.entries
            .iter()
            .find(|entry| &entry.identity == identity)
            .map(|entry| entry.path.as_path())
    }
}

/// Ordered credential-helper rules retained outside DICE and diagnostics.
pub struct CredentialHelpersRuntimeConfig {
    entries: Box<[CredentialHelperEntry]>,
    identity: CredentialProviderIdentity,
}

struct CredentialHelperEntry {
    scope: CredentialHelperScope,
    authorization_domain: Box<str>,
    executable: PathBuf,
}

enum CredentialHelperScope {
    Exact(Box<str>),
    Wildcard(Box<str>),
    Default,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialHelpersSummary<'a> {
    pub provider_digest: &'a str,
    pub authorization_domains: &'a [Box<str>],
    pub helper_count: usize,
}

/// A selected credential helper and its canonical authorization domain.
///
/// This runtime-only type intentionally does not implement `Debug`, `Eq`,
/// `Hash`, `Allocative`, or `Pagable`, keeping executable paths out of DICE
/// identity, summaries, and diagnostics.
pub struct CredentialHelperMatch<'a> {
    pub executable: &'a Path,
    pub authorization_domain: &'a str,
}

/// Attach runtime-only Bzlmod data to one command transaction.
pub trait SetBzlmodRuntimeConfig {
    fn set_bzlmod_runtime_config(&mut self, config: BzlmodRuntimeConfig);
}

/// Read runtime-only Bzlmod data without making it part of DICE identity.
pub trait HasBzlmodRuntimeConfig {
    fn get_bzlmod_runtime_config(&self) -> buck2_error::Result<&BzlmodRuntimeConfig>;
}

impl SetBzlmodRuntimeConfig for UserComputationData {
    fn set_bzlmod_runtime_config(&mut self, config: BzlmodRuntimeConfig) {
        self.data.set(config);
    }
}

impl HasBzlmodRuntimeConfig for UserComputationData {
    fn get_bzlmod_runtime_config(&self) -> buck2_error::Result<&BzlmodRuntimeConfig> {
        self.data.get::<BzlmodRuntimeConfig>().map_err(|error| {
            buck2_error::buck2_error!(
                buck2_error::ErrorTag::Tier0,
                "Bzlmod runtime configuration should be set for this command: {}",
                error
            )
        })
    }
}

impl HasBzlmodRuntimeConfig for DiceComputations<'_> {
    fn get_bzlmod_runtime_config(&self) -> buck2_error::Result<&BzlmodRuntimeConfig> {
        self.per_transaction_data().get_bzlmod_runtime_config()
    }
}

impl CredentialHelpersRuntimeConfig {
    pub(crate) fn empty() -> Self {
        Self::from_entries(Vec::new())
    }

    pub(crate) fn parse(value: &str) -> buck2_error::Result<Self> {
        if value.trim().is_empty() {
            return Ok(Self::empty());
        }

        let mut entries = Vec::new();
        for raw_entry in value.split(',') {
            let entry = raw_entry.trim();
            if entry.is_empty() {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "credential_helpers contains an empty entry"
                ));
            }
            let (scope, executable) = entry.split_once('=').ok_or_else(|| {
                buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "credential helper entries must use scope=/absolute/helper syntax"
                )
            })?;
            let scope = parse_scope(scope.trim())?;
            let executable = executable.trim();
            if executable.is_empty() || !Path::new(executable).is_absolute() {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "credential helper executable paths must be absolute"
                ));
            }
            let authorization_domain = scope.as_identity_str().into();
            entries.push(CredentialHelperEntry {
                scope,
                authorization_domain,
                executable: PathBuf::from(executable),
            });
        }
        Ok(Self::from_entries(entries))
    }

    fn from_entries(entries: Vec<CredentialHelperEntry>) -> Self {
        let mut hasher = Sha256::new();
        let mut domains = Vec::with_capacity(entries.len());
        for entry in &entries {
            hash_field(&mut hasher, entry.authorization_domain.as_bytes());
            hash_field(&mut hasher, entry.executable.as_os_str().as_encoded_bytes());
            domains.push(entry.authorization_domain.clone());
        }
        let identity = CredentialProviderIdentity::new(
            hex::encode(hasher.finalize()).into(),
            domains.into_boxed_slice(),
        );
        Self {
            entries: entries.into_boxed_slice(),
            identity,
        }
    }

    pub(crate) fn identity(&self) -> &CredentialProviderIdentity {
        &self.identity
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Select a helper using exact, longest-wildcard, then default precedence.
    /// The first entry wins ties.
    pub fn match_for_host(&self, host: &str) -> Option<CredentialHelperMatch<'_>> {
        let host = host
            .trim_matches(['[', ']'])
            .trim_end_matches('.')
            .to_ascii_lowercase();
        let host = host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.to_string())
            .unwrap_or(host);
        let mut best: Option<(u8, usize, &CredentialHelperEntry)> = None;
        for entry in &self.entries {
            let rank = match &entry.scope {
                CredentialHelperScope::Exact(exact) if exact.as_ref() == host => (3, exact.len()),
                CredentialHelperScope::Wildcard(suffix)
                    if host.len() > suffix.len()
                        && host.ends_with(suffix.as_ref())
                        && host.as_bytes()[host.len() - suffix.len() - 1] == b'.' =>
                {
                    (2, suffix.len())
                }
                CredentialHelperScope::Default => (1, 0),
                _ => continue,
            };
            if best.is_none_or(|(best_class, best_len, _)| {
                rank.0 > best_class || (rank.0 == best_class && rank.1 > best_len)
            }) {
                best = Some((rank.0, rank.1, entry));
            }
        }
        best.map(|(_, _, entry)| CredentialHelperMatch {
            executable: entry.executable.as_path(),
            authorization_domain: &entry.authorization_domain,
        })
    }

    /// Select a helper using exact, longest-wildcard, then default precedence.
    /// The first entry wins ties.
    pub fn helper_for_host(&self, host: &str) -> Option<&Path> {
        self.match_for_host(host).map(|matched| matched.executable)
    }

    pub fn redacted_summary(&self) -> CredentialHelpersSummary<'_> {
        CredentialHelpersSummary {
            provider_digest: self.identity.provider_digest(),
            authorization_domains: self.identity.authorization_domains(),
            helper_count: self.entries.len(),
        }
    }
}

impl CredentialHelperScope {
    fn as_identity_str(&self) -> String {
        match self {
            Self::Exact(host) => format!("exact:{host}"),
            Self::Wildcard(suffix) => format!("wildcard:*.{suffix}"),
            Self::Default => "default".to_owned(),
        }
    }
}

fn parse_scope(value: &str) -> buck2_error::Result<CredentialHelperScope> {
    if value == "default" {
        return Ok(CredentialHelperScope::Default);
    }
    if value.is_empty() || value != value.to_ascii_lowercase() {
        return Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "credential helper scopes must be lowercase hosts, *.suffix wildcards, or default"
        ));
    }
    if let Some(suffix) = value.strip_prefix("*.") {
        if suffix.parse::<std::net::IpAddr>().is_ok() || !valid_dns_name(suffix) {
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "invalid credential helper wildcard scope"
            ));
        }
        return Ok(CredentialHelperScope::Wildcard(suffix.into()));
    }
    let ip_value = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(value);
    if let Ok(ip) = ip_value.parse::<std::net::IpAddr>() {
        return Ok(CredentialHelperScope::Exact(ip.to_string().into()));
    }
    if valid_dns_name(value) {
        return Ok(CredentialHelperScope::Exact(value.into()));
    }
    Err(buck2_error::buck2_error!(
        buck2_error::ErrorTag::Input,
        "invalid credential helper host scope"
    ))
}

fn valid_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_match(
        helpers: &CredentialHelpersRuntimeConfig,
        host: &str,
        executable: &str,
        authorization_domain: &str,
    ) {
        let matched = helpers.match_for_host(host).expect("expected a match");
        assert_eq!(matched.executable, Path::new(executable));
        assert_eq!(matched.authorization_domain, authorization_domain);
    }

    #[test]
    fn selects_helpers_by_documented_precedence() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse(
            "default=/bin/default,*.example.com=/bin/wild,*.sub.example.com=/bin/long,api.example.com=/bin/exact,api.example.com=/bin/later",
        )?;
        assert_match(
            &helpers,
            "api.example.com",
            "/bin/exact",
            "exact:api.example.com",
        );
        assert_match(
            &helpers,
            "x.sub.example.com",
            "/bin/long",
            "wildcard:*.sub.example.com",
        );
        assert_match(
            &helpers,
            "x.example.com",
            "/bin/wild",
            "wildcard:*.example.com",
        );
        assert_match(&helpers, "elsewhere.test", "/bin/default", "default");
        Ok(())
    }

    #[test]
    fn wildcard_match_keeps_one_domain_across_hosts() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse("*.example.com=/bin/wild")?;
        assert_match(
            &helpers,
            "one.example.com",
            "/bin/wild",
            "wildcard:*.example.com",
        );
        assert_match(
            &helpers,
            "two.example.com",
            "/bin/wild",
            "wildcard:*.example.com",
        );
        Ok(())
    }

    #[test]
    fn exact_match_has_distinct_domain_from_wildcard() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse(
            "*.example.com=/bin/wild,api.example.com=/bin/exact",
        )?;
        assert_match(
            &helpers,
            "api.example.com",
            "/bin/exact",
            "exact:api.example.com",
        );
        assert_match(
            &helpers,
            "www.example.com",
            "/bin/wild",
            "wildcard:*.example.com",
        );
        Ok(())
    }

    #[test]
    fn canonicalizes_ipv6_helper_scopes() -> buck2_error::Result<()> {
        let helpers =
            CredentialHelpersRuntimeConfig::parse("2001:0db8::1=/bin/ipv6,[::1]=/bin/loopback")?;
        assert_match(&helpers, "[2001:db8::1]", "/bin/ipv6", "exact:2001:db8::1");
        assert_match(&helpers, "0:0:0:0:0:0:0:1", "/bin/loopback", "exact:::1");
        Ok(())
    }

    #[test]
    fn rejects_non_absolute_paths_and_invalid_scopes() {
        for value in [
            "example.com=relative",
            "EXAMPLE.com=/bin/helper",
            "*.127.0.0.1=/bin/helper",
            "example..com=/bin/helper",
            "default=/bin/a,,example.com=/bin/b",
        ] {
            assert!(
                CredentialHelpersRuntimeConfig::parse(value).is_err(),
                "accepted {value:?}"
            );
        }
    }

    #[test]
    fn redacted_summary_never_contains_executable_paths() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse("example.com=/secret/helper")?;
        let debug = format!("{:?}", helpers.redacted_summary());
        assert!(!debug.contains("/secret/helper"));
        assert!(debug.contains("example.com"));
        Ok(())
    }

    #[test]
    fn exact_and_wildcard_scopes_have_distinct_identities() -> buck2_error::Result<()> {
        let exact = CredentialHelpersRuntimeConfig::parse("example.com=/bin/helper")?;
        let wildcard = CredentialHelpersRuntimeConfig::parse("*.example.com=/bin/helper")?;
        assert_ne!(
            exact.identity().provider_digest(),
            wildcard.identity().provider_digest()
        );
        assert_eq!(
            exact.identity().authorization_domains()[0].as_ref(),
            "exact:example.com"
        );
        assert_eq!(
            wildcard.identity().authorization_domains()[0].as_ref(),
            "wildcard:*.example.com"
        );
        Ok(())
    }

    #[test]
    fn matches_agree_with_provider_authorization_domains() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse(
            "default=/bin/default,*.example.com=/bin/wild,api.example.com=/bin/exact",
        )?;
        for (host, domain_index) in [
            ("elsewhere.test", 0),
            ("www.example.com", 1),
            ("api.example.com", 2),
        ] {
            let matched = helpers.match_for_host(host).expect("expected a match");
            assert_eq!(
                matched.authorization_domain,
                helpers.identity().authorization_domains()[domain_index].as_ref()
            );
        }
        Ok(())
    }

    #[test]
    fn stores_runtime_data_outside_dice_identity() -> buck2_error::Result<()> {
        let helpers = CredentialHelpersRuntimeConfig::parse("default=/secret/helper")?;
        let mut data = UserComputationData::default();
        data.set_bzlmod_runtime_config(BzlmodRuntimeConfig::new(
            helpers,
            MachineRegistriesRuntimeConfig::from_entries(Vec::new()),
        ));
        let stored = data.get_bzlmod_runtime_config()?;
        assert_eq!(
            stored.credential_helpers().helper_for_host("example.com"),
            Some(Path::new("/secret/helper"))
        );
        Ok(())
    }
}
