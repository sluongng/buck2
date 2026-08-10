/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::error::Error;
use std::fmt;

/// An opaque absolute target-pattern spelling from a module registration.
///
/// This type deliberately checks only the leading absolute marker. Wildcards,
/// inferred targets, repository names, and malformed suffixes remain opaque so
/// later loading and target-pattern phases can interpret them with the proper
/// repository mapping and package context.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawAbsoluteTargetPattern(Box<str>);

impl RawAbsoluteTargetPattern {
    pub fn parse(value: &str) -> Result<Self, RawAbsoluteTargetPatternParseError> {
        if value.starts_with("//") || value.starts_with('@') {
            Ok(Self(value.into()))
        } else {
            Err(RawAbsoluteTargetPatternParseError(value.into()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RawAbsoluteTargetPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawAbsoluteTargetPatternParseError(Box<str>);

impl fmt::Display for RawAbsoluteTargetPatternParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target pattern '{}' must be absolute, starting with '//' or '@'",
            self.0
        )
    }
}

impl Error for RawAbsoluteTargetPatternParseError {}

/// Source-ordered target patterns registered by one evaluated module file.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ModuleRegistrations {
    execution_platforms: Box<[RawAbsoluteTargetPattern]>,
    toolchains: Box<[RawAbsoluteTargetPattern]>,
}

impl ModuleRegistrations {
    pub fn new(
        execution_platforms: Box<[RawAbsoluteTargetPattern]>,
        toolchains: Box<[RawAbsoluteTargetPattern]>,
    ) -> Self {
        Self {
            execution_platforms,
            toolchains,
        }
    }

    pub fn execution_platforms(&self) -> &[RawAbsoluteTargetPattern] {
        &self.execution_platforms
    }

    pub fn toolchains(&self) -> &[RawAbsoluteTargetPattern] {
        &self.toolchains
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_prefix_check_preserves_raw_opaque_spellings() {
        for value in [
            "//pkg:target",
            "//pkg",
            "//...",
            "//pkg/...",
            "//malformed::suffix",
            "@repo//pkg:target",
            "@repo//...",
            "@",
        ] {
            let parsed = RawAbsoluteTargetPattern::parse(value).unwrap();
            assert_eq!(parsed.as_str(), value);
        }

        for value in [
            "",
            "relative:target",
            ":target",
            "-//pkg:target",
            "-@repo//:all",
        ] {
            assert!(RawAbsoluteTargetPattern::parse(value).is_err(), "{value}");
        }
    }

    #[test]
    fn registrations_preserve_categories_order_and_duplicates() {
        let platform = RawAbsoluteTargetPattern::parse("//:platform").unwrap();
        let toolchain = RawAbsoluteTargetPattern::parse("@repo//:all").unwrap();
        let registrations = ModuleRegistrations::new(
            vec![platform.clone(), platform].into_boxed_slice(),
            vec![toolchain.clone(), toolchain].into_boxed_slice(),
        );

        assert_eq!(
            registrations
                .execution_platforms()
                .iter()
                .map(RawAbsoluteTargetPattern::as_str)
                .collect::<Vec<_>>(),
            ["//:platform", "//:platform"]
        );
        assert_eq!(
            registrations
                .toolchains()
                .iter()
                .map(RawAbsoluteTargetPattern::as_str)
                .collect::<Vec<_>>(),
            ["@repo//:all", "@repo//:all"]
        );
    }
}
