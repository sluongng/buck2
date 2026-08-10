/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

/// A version using Bazel's relaxed SemVer syntax and ordering.
///
/// Unlike SemVer, the release may have any positive number of segments and
/// release segments may contain ASCII letters. Build metadata is accepted but
/// discarded, so it is absent from display, equality, and ordering.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Version(VersionRepr);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum VersionRepr {
    /// Bazel's empty sentinel for the root and non-registry overrides.
    Empty,
    Parsed {
        release: Box<[Identifier]>,
        prerelease: Box<[Identifier]>,
        normalized: Box<str>,
    },
}

impl Version {
    /// The sentinel that sorts after every real version.
    pub const EMPTY: Self = Self(VersionRepr::Empty);

    pub fn parse(input: &str) -> Result<Self, VersionParseError> {
        if input.is_empty() {
            return Ok(Self::EMPTY);
        }

        let (without_build, build) = match input.split_once('+') {
            Some((version, build)) => (version, Some(build)),
            None => (input, None),
        };

        if let Some(build) = build {
            // This deliberately mirrors Version.java's ignored build capture:
            // it validates the character class, but does not split identifiers.
            if build.is_empty() || !build.bytes().all(is_prerelease_or_build_byte) {
                return Err(VersionParseError::bad_version(input));
            }
        }

        let (release, prerelease) = match without_build.split_once('-') {
            Some((release, prerelease)) => (release, Some(prerelease)),
            None => (without_build, None),
        };

        if release.is_empty() || !release.bytes().all(is_release_byte) {
            return Err(VersionParseError::bad_version(input));
        }
        if let Some(prerelease) = prerelease {
            if prerelease.is_empty() || !prerelease.bytes().all(is_prerelease_or_build_byte) {
                return Err(VersionParseError::bad_version(input));
            }
        }

        let release = parse_identifiers(release, input)?;
        let prerelease = prerelease
            .map(|value| parse_identifiers(value, input))
            .transpose()?
            .unwrap_or_default();

        Ok(Self(VersionRepr::Parsed {
            release: release.into_boxed_slice(),
            prerelease: prerelease.into_boxed_slice(),
            normalized: without_build.into(),
        }))
    }

    /// Returns the normalized spelling, with build metadata removed.
    pub fn normalized(&self) -> &str {
        match &self.0 {
            VersionRepr::Empty => "",
            VersionRepr::Parsed { normalized, .. } => normalized,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.0, VersionRepr::Empty)
    }

    pub fn is_prerelease(&self) -> bool {
        matches!(
            &self.0,
            VersionRepr::Parsed { prerelease, .. } if !prerelease.is_empty()
        )
    }
}

impl FromStr for Version {
    type Err = VersionParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.normalized())
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (VersionRepr::Empty, VersionRepr::Empty) => Ordering::Equal,
            (VersionRepr::Empty, VersionRepr::Parsed { .. }) => Ordering::Greater,
            (VersionRepr::Parsed { .. }, VersionRepr::Empty) => Ordering::Less,
            (
                VersionRepr::Parsed {
                    release,
                    prerelease,
                    ..
                },
                VersionRepr::Parsed {
                    release: other_release,
                    prerelease: other_prerelease,
                    ..
                },
            ) => release.cmp(other_release).then_with(|| {
                match (prerelease.is_empty(), other_prerelease.is_empty()) {
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => prerelease.cmp(other_prerelease),
                }
            }),
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Identifier {
    Numeric { value: u64, spelling: Box<str> },
    NonNumeric(Box<str>),
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::Numeric { value, spelling },
                Self::Numeric {
                    value: other_value,
                    spelling: other_spelling,
                },
            ) => value
                .cmp(other_value)
                .then_with(|| spelling.cmp(other_spelling)),
            (Self::Numeric { .. }, Self::NonNumeric(_)) => Ordering::Less,
            (Self::NonNumeric(_), Self::Numeric { .. }) => Ordering::Greater,
            (Self::NonNumeric(value), Self::NonNumeric(other_value)) => value.cmp(other_value),
        }
    }
}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn parse_identifiers(value: &str, input: &str) -> Result<Vec<Identifier>, VersionParseError> {
    value
        .split('.')
        .map(|identifier| {
            if identifier.is_empty() {
                return Err(VersionParseError::bad_version(input));
            }
            if identifier.bytes().all(|byte| byte.is_ascii_digit()) {
                let number = identifier.parse::<u64>().map_err(|_| {
                    VersionParseError::new(format!(
                        "numeric version segment is too large: {identifier}"
                    ))
                })?;
                Ok(Identifier::Numeric {
                    value: number,
                    spelling: identifier.into(),
                })
            } else {
                Ok(Identifier::NonNumeric(identifier.into()))
            }
        })
        .collect()
}

fn is_release_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'.'
}

fn is_prerelease_or_build_byte(byte: u8) -> bool {
    is_release_byte(byte) || byte == b'-'
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionParseError {
    message: Box<str>,
}

impl VersionParseError {
    fn new(message: String) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn bad_version(input: &str) -> Self {
        Self::new(format!("bad version (does not match regex): {input}"))
    }
}

impl fmt::Display for VersionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for VersionParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(value: &str) -> Version {
        Version::parse(value).unwrap()
    }

    #[test]
    fn empty_beats_everything() {
        assert!(version("") > version("1.0"));
        assert!(version("") > version("1.0+build"));
        assert!(version("") > version("1.0-pre"));
        assert!(version("") > version("1.0-pre+build-kek.lol"));
    }

    #[test]
    fn normalized() {
        assert_eq!(version("1.0").normalized(), "1.0");
        assert_eq!(version("1.0+build").normalized(), "1.0");
        assert_eq!(version("1.0-pre").normalized(), "1.0-pre");
        assert_eq!(version("1.0-pre+build-kek.lol").normalized(), "1.0-pre");
        assert_eq!(version("1.0+build-notpre").normalized(), "1.0");
    }

    #[test]
    fn release_version() {
        assert!(version("2.0") > version("1.0"));
        assert!(version("2.0") > version("1.9"));
        assert!(version("11.0") > version("3.0"));
        assert!(version("1.0.1") > version("1.0"));
        assert!(version("1.0.0") > version("1.0"));
        assert_eq!(version("1.0+build2"), version("1.0+build3"));
        assert!(version("1.0") > version("1.0-pre"));
        assert_eq!(version("1.0"), version("1.0+build-notpre"));
    }

    #[test]
    fn release_version_with_letters() {
        assert!(version("1.0.patch.3") > version("1.0"));
        assert!(version("1.0.patch.3") > version("1.0.patch.2"));
        assert!(version("1.0.patch.3") < version("1.0.patch.10"));
        assert!(version("1.0.patch3") > version("1.0.patch10"));
        assert!(version("4") < version("a"));
        assert!(version("abc") < version("abd"));
    }

    #[test]
    fn prerelease_version() {
        assert!(version("1.0-pre") > version("1.0-are"));
        assert!(version("1.0-3") > version("1.0-2"));
        assert!(version("1.0-pre") < version("1.0-pre.foo"));
        assert!(version("1.0-pre.3") > version("1.0-pre.2"));
        assert!(version("1.0-pre.10") > version("1.0-pre.2"));
        assert!(version("1.0-pre.10a") < version("1.0-pre.2a"));
        assert!(version("1.0-pre.99") < version("1.0-pre.2a"));
        assert!(version("1.0-pre.patch.3") < version("1.0-pre.patch.4"));
        assert!(version("1.0--") < version("1.0----"));
        assert!(
            version("2.1.1-develop.bcr.20250113215904")
                > version("2.1.1-develop.bcr.20250113215903")
        );
    }

    #[test]
    fn parse_exception() {
        for invalid in [
            "-abc",
            "1_2",
            "ßážëł",
            "1.0-pre?",
            "1.0-11111111111111111111111111111111111111111",
            "1.0-pre///",
            "1..0",
            "1.0-pre..erp",
        ] {
            assert!(Version::parse(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn numeric_spelling_breaks_ties_deterministically() {
        assert!(version("1.01") < version("1.1"));
        assert_ne!(version("1.01"), version("1.1"));
    }

    #[test]
    fn display_is_normalized() {
        assert_eq!(version("1.2-pre+ignored.1").to_string(), "1.2-pre");
        assert_eq!(version("").to_string(), "");
    }
}
