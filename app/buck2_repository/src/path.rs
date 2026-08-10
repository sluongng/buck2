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

use buck2_bzlmod::ModuleKey;
use buck2_bzlmod::ModuleName;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RegistryFileKind {
    BazelRegistryJson,
    MetadataJson,
    ModuleFile,
    SourceJson,
    Patch,
    Overlay,
}

/// A canonical, relative registry path constructed without filesystem joins.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RegistryPath {
    kind: RegistryFileKind,
    value: Box<str>,
}

impl RegistryPath {
    pub fn bazel_registry_json() -> Self {
        Self::new(RegistryFileKind::BazelRegistryJson, "bazel_registry.json")
    }

    pub fn metadata(module: &ModuleName) -> Self {
        Self::new(
            RegistryFileKind::MetadataJson,
            format!("modules/{module}/metadata.json"),
        )
    }

    pub fn module_file(module: &ModuleKey) -> Result<Self, RegistryPathError> {
        Self::module_version_file(module, RegistryFileKind::ModuleFile, "MODULE.bazel")
    }

    pub fn source_json(module: &ModuleKey) -> Result<Self, RegistryPathError> {
        Self::module_version_file(module, RegistryFileKind::SourceJson, "source.json")
    }

    pub fn patch(module: &ModuleKey, path: &str) -> Result<Self, RegistryPathError> {
        Self::module_artifact(module, RegistryFileKind::Patch, "patches", path)
    }

    pub fn overlay(module: &ModuleKey, path: &str) -> Result<Self, RegistryPathError> {
        Self::module_artifact(module, RegistryFileKind::Overlay, "overlay", path)
    }

    pub fn kind(&self) -> RegistryFileKind {
        self.kind
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    fn module_version_file(
        module: &ModuleKey,
        kind: RegistryFileKind,
        file: &str,
    ) -> Result<Self, RegistryPathError> {
        let (name, version) = registry_module_parts(module)?;
        Ok(Self::new(kind, format!("modules/{name}/{version}/{file}")))
    }

    fn module_artifact(
        module: &ModuleKey,
        kind: RegistryFileKind,
        directory: &str,
        path: &str,
    ) -> Result<Self, RegistryPathError> {
        validate_registry_subpath(path)?;
        let (name, version) = registry_module_parts(module)?;
        Ok(Self::new(
            kind,
            format!("modules/{name}/{version}/{directory}/{path}"),
        ))
    }

    fn new(kind: RegistryFileKind, value: impl Into<Box<str>>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }
}

impl fmt::Display for RegistryPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

fn registry_module_parts(module: &ModuleKey) -> Result<(&ModuleName, &str), RegistryPathError> {
    let name = module.name().ok_or(RegistryPathError::RootModule)?;
    if module.version().is_empty() {
        return Err(RegistryPathError::NonRegistryModule);
    }
    Ok((name, module.version().normalized()))
}

pub(crate) fn validate_registry_subpath(path: &str) -> Result<(), RegistryPathError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains('%')
        || path.contains('?')
        || path.contains('#')
        // Colons admit Windows drive paths and NTFS alternate data streams.
        || path.contains(':')
        || path.bytes().any(|byte| byte.is_ascii_control())
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(RegistryPathError::UnsafeSubpath);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryPathError {
    RootModule,
    NonRegistryModule,
    UnsafeSubpath,
}

impl fmt::Display for RegistryPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootModule => f.write_str("the root module has no registry path"),
            Self::NonRegistryModule => f.write_str("a non-registry module has no registry path"),
            Self::UnsafeSubpath => {
                f.write_str("unsafe registry subpath: expected a canonical relative path")
            }
        }
    }
}

impl Error for RegistryPathError {}

#[cfg(test)]
mod tests {
    use buck2_bzlmod::Version;

    use super::*;

    fn module() -> ModuleKey {
        ModuleKey::registry(
            ModuleName::parse("rules_go").unwrap(),
            Version::parse("0.50.1").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn constructs_every_registry_path() {
        let module = module();
        assert_eq!(
            RegistryPath::bazel_registry_json().as_str(),
            "bazel_registry.json"
        );
        assert_eq!(
            RegistryPath::metadata(module.name().unwrap()).as_str(),
            "modules/rules_go/metadata.json"
        );
        assert_eq!(
            RegistryPath::module_file(&module).unwrap().as_str(),
            "modules/rules_go/0.50.1/MODULE.bazel"
        );
        assert_eq!(
            RegistryPath::source_json(&module).unwrap().as_str(),
            "modules/rules_go/0.50.1/source.json"
        );
        assert_eq!(
            RegistryPath::patch(&module, "fixes/one.patch")
                .unwrap()
                .as_str(),
            "modules/rules_go/0.50.1/patches/fixes/one.patch"
        );
        assert_eq!(
            RegistryPath::overlay(&module, "pkg/BUILD.bazel")
                .unwrap()
                .as_str(),
            "modules/rules_go/0.50.1/overlay/pkg/BUILD.bazel"
        );
    }

    #[test]
    fn rejects_modules_without_registry_coordinates() {
        assert_eq!(
            RegistryPath::source_json(&ModuleKey::ROOT).unwrap_err(),
            RegistryPathError::RootModule
        );
        assert_eq!(
            RegistryPath::source_json(&ModuleKey::non_registry(
                ModuleName::parse("rules_go").unwrap()
            ))
            .unwrap_err(),
            RegistryPathError::NonRegistryModule
        );
    }

    #[test]
    fn rejects_path_injection_and_traversal() {
        for path in [
            "",
            "/absolute",
            "//server/share",
            "trailing/",
            "two//parts",
            ".",
            "../escape",
            "a/../b",
            "a\\b",
            r"\\server\share",
            r"\\?\C:\outside",
            "C:/outside",
            r"C:\outside",
            "c:relative",
            "file:stream",
            "a?query",
            "a#fragment",
            "%2e%2e/escape",
            "line\nfeed",
        ] {
            assert!(
                RegistryPath::patch(&module(), path).is_err(),
                "accepted {path:?}"
            );
            assert!(
                RegistryPath::overlay(&module(), path).is_err(),
                "accepted {path:?}"
            );
        }
    }

    #[test]
    fn accepts_canonical_colon_free_artifact_paths() {
        for path in [
            "fix.patch",
            "patches/fix.patch",
            "pkg/BUILD.bazel",
            ".hidden",
            "directory/file name",
        ] {
            assert_eq!(
                RegistryPath::patch(&module(), path).unwrap().kind(),
                RegistryFileKind::Patch
            );
            assert_eq!(
                RegistryPath::overlay(&module(), path).unwrap().kind(),
                RegistryFileKind::Overlay
            );
        }
    }

    #[test]
    fn rejected_paths_do_not_echo_input() {
        let error = RegistryPath::patch(&module(), "../secret-path").unwrap_err();
        assert!(!error.to_string().contains("secret-path"));
        assert!(!format!("{error:?}").contains("secret-path"));
    }
}
