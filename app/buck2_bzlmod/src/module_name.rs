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
use std::str::FromStr;

/// A module name validated according to Bazel's module-name rules.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModuleName(Box<str>);

impl ModuleName {
    pub fn parse(value: &str) -> Result<Self, ModuleNameParseError> {
        let mut bytes = value.bytes();
        let first = bytes.next();
        let last = value.bytes().next_back();
        let valid = first.is_some_and(|byte| byte.is_ascii_lowercase())
            && last.is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'-' | b'_')
            });
        if !valid {
            return Err(ModuleNameParseError {
                value: value.into(),
            });
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ModuleName {
    type Err = ModuleNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl fmt::Display for ModuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleNameParseError {
    value: Box<str>,
}

impl fmt::Display for ModuleNameParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid module name '{}': names must start with a lowercase letter, end with a lowercase letter or digit, and contain only lowercase letters, digits, dots, hyphens, and underscores",
            self.value
        )
    }
}

impl Error for ModuleNameParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bazel_module_names() {
        for name in ["a", "rules_go", "rules-java.2", "a-b_c.d9"] {
            assert_eq!(ModuleName::parse(name).unwrap().as_str(), name);
        }
    }

    #[test]
    fn rejects_invalid_module_names() {
        for name in ["", "A", "2rules", "rules_", "rules.", "rules+go", "rüles"] {
            assert!(ModuleName::parse(name).is_err(), "accepted {name:?}");
        }
    }
}
