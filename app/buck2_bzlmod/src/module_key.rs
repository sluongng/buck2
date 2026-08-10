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

use crate::ModuleName;
use crate::ModuleNameParseError;
use crate::Version;
use crate::VersionParseError;

/// A module identity with a structurally distinct root sentinel.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ModuleKey {
    Root,
    Module { name: ModuleName, version: Version },
}

impl ModuleKey {
    pub const ROOT: Self = Self::Root;

    pub fn new(name: ModuleName, version: Version) -> Self {
        Self::Module { name, version }
    }

    pub fn registry(name: ModuleName, version: Version) -> Result<Self, ModuleKeyParseError> {
        if version.is_empty() {
            return Err(ModuleKeyParseError::EmptyRegistryVersion);
        }
        Ok(Self::new(name, version))
    }

    pub fn non_registry(name: ModuleName) -> Self {
        Self::new(name, Version::EMPTY)
    }

    pub fn is_root(&self) -> bool {
        matches!(self, Self::Root)
    }

    pub fn is_non_registry(&self) -> bool {
        matches!(self, Self::Module { version, .. } if version.is_empty())
    }

    pub fn name(&self) -> Option<&ModuleName> {
        match self {
            Self::Root => None,
            Self::Module { name, .. } => Some(name),
        }
    }

    pub fn version(&self) -> &Version {
        match self {
            Self::Root => &Version::EMPTY,
            Self::Module { version, .. } => version,
        }
    }

    pub fn to_display_string(&self) -> String {
        if self.is_root() {
            "root module".to_owned()
        } else {
            format!("module '{self}'")
        }
    }

    fn name_for_ordering(&self) -> &str {
        self.name().map_or("", ModuleName::as_str)
    }
}

impl fmt::Display for ModuleKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => f.write_str("<root>"),
            Self::Module { name, version } if version.is_empty() => write!(f, "{name}@_"),
            Self::Module { name, version } => write!(f, "{name}@{version}"),
        }
    }
}

impl Ord for ModuleKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name_for_ordering()
            .cmp(other.name_for_ordering())
            .then_with(|| self.version().cmp(other.version()))
    }
}

impl PartialOrd for ModuleKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for ModuleKey {
    type Err = ModuleKeyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "<root>" {
            return Ok(Self::ROOT);
        }
        let (name, version) = value
            .split_once('@')
            .ok_or_else(|| ModuleKeyParseError::BadKey(value.into()))?;
        let name = ModuleName::parse(name).map_err(ModuleKeyParseError::Name)?;
        if version == "_" {
            return Ok(Self::non_registry(name));
        }
        let version = Version::parse(version).map_err(ModuleKeyParseError::Version)?;
        Self::registry(name, version)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleKeyParseError {
    BadKey(Box<str>),
    Name(ModuleNameParseError),
    Version(VersionParseError),
    EmptyRegistryVersion,
}

impl fmt::Display for ModuleKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadKey(value) => write!(f, "bad module key: {value}"),
            Self::Name(error) => error.fmt(f),
            Self::Version(error) => error.fmt(f),
            Self::EmptyRegistryVersion => f.write_str("a registry module version may not be empty"),
        }
    }
}

impl Error for ModuleKeyParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Name(error) => Some(error),
            Self::Version(error) => Some(error),
            Self::BadKey(_) | Self::EmptyRegistryVersion => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(value: &str) -> ModuleName {
        ModuleName::parse(value).unwrap()
    }

    #[test]
    fn root_and_non_registry_sentinels_are_distinct() {
        let root = ModuleKey::ROOT;
        let override_key = ModuleKey::non_registry(name("rules_go"));

        assert!(root.is_root());
        assert!(!root.is_non_registry());
        assert!(!override_key.is_root());
        assert!(override_key.is_non_registry());
        assert_eq!(root.version(), override_key.version());
        assert_ne!(root, override_key);
    }

    #[test]
    fn display_matches_bazel_module_keys() {
        let registry =
            ModuleKey::registry(name("rules_go"), Version::parse("0.50+build").unwrap()).unwrap();
        let overridden = ModuleKey::non_registry(name("rules_go"));

        assert_eq!(ModuleKey::ROOT.to_string(), "<root>");
        assert_eq!(registry.to_string(), "rules_go@0.50");
        assert_eq!(overridden.to_string(), "rules_go@_");
        assert_eq!(ModuleKey::ROOT.to_display_string(), "root module");
        assert_eq!(registry.to_display_string(), "module 'rules_go@0.50'");
    }

    #[test]
    fn parses_display_spelling_deterministically() {
        for value in ["<root>", "rules_go@0.50", "rules_go@_"] {
            let key = ModuleKey::from_str(value).unwrap();
            assert_eq!(key.to_string(), value);
        }
    }

    #[test]
    fn registry_keys_require_real_versions() {
        assert_eq!(
            ModuleKey::registry(name("rules_go"), Version::EMPTY),
            Err(ModuleKeyParseError::EmptyRegistryVersion)
        );
        assert!(ModuleKey::from_str("rules_go@").is_err());
        assert!(ModuleKey::from_str("@1.0").is_err());
        assert!(ModuleKey::from_str("rules_go").is_err());
    }

    #[test]
    fn ordering_is_name_then_bazel_version() {
        let root = ModuleKey::ROOT;
        let v1 = ModuleKey::registry(name("rules_go"), Version::parse("1").unwrap()).unwrap();
        let v2 = ModuleKey::registry(name("rules_go"), Version::parse("2").unwrap()).unwrap();
        let overridden = ModuleKey::non_registry(name("rules_go"));
        let other = ModuleKey::registry(name("zlib"), Version::parse("1").unwrap()).unwrap();

        assert!(root < v1);
        assert!(v1 < v2);
        assert!(v2 < overridden);
        assert!(overridden < other);
    }
}
