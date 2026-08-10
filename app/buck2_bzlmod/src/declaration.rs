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

use crate::ModuleKey;
use crate::ModuleName;
use crate::Version;

/// A patch label normalized during `MODULE.bazel` evaluation.
///
/// The evaluator resolves root-relative labels and the root module's apparent
/// repository alias before constructing this value. Canonical repository
/// labels remain canonical so a later materialization layer can resolve them
/// without consulting the evaluator again.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PatchLabel(Box<str>);

impl PatchLabel {
    /// Parses the canonical spelling produced after Bazel repository mapping.
    ///
    /// Only main-repository (`//pkg:target`) and canonical-repository
    /// (`@@repo//pkg:target`) labels are accepted. Apparent repository names,
    /// relative labels, traversal, and legacy unnormalized `/.` target
    /// suffixes are rejected.
    pub fn parse_normalized(value: &str) -> Result<Self, PatchLabelParseError> {
        let remainder = if let Some(remainder) = value.strip_prefix("//") {
            remainder
        } else if let Some(remainder) = value.strip_prefix("@@") {
            let (repository, remainder) = remainder
                .split_once("//")
                .ok_or_else(|| PatchLabelParseError::new(value))?;
            if !valid_canonical_repository(repository) {
                return Err(PatchLabelParseError::new(value));
            }
            remainder
        } else {
            return Err(PatchLabelParseError::new(value));
        };
        let (package, target) = remainder
            .split_once(':')
            .ok_or_else(|| PatchLabelParseError::new(value))?;
        if !valid_normalized_package(package) || !valid_normalized_target(target) {
            return Err(PatchLabelParseError::new(value));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatchLabelParseError {
    value: Box<str>,
}

impl PatchLabelParseError {
    fn new(value: &str) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl fmt::Display for PatchLabelParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid normalized patch label '{}'", self.value)
    }
}

impl Error for PatchLabelParseError {}

fn valid_canonical_repository(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+'))
}

fn valid_normalized_package(value: &str) -> bool {
    value.is_empty()
        || (!value.starts_with('/')
            && !value.ends_with('/')
            && !value.contains("//")
            && value.chars().all(|character| {
                character.is_ascii()
                    && !character.is_ascii_control()
                    && !matches!(character, ':' | '\\')
            })
            && value
                .split('/')
                .all(|segment| segment.chars().any(|character| character != '.')))
}

fn valid_normalized_target(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value != ".."
        && !value.starts_with("../")
        && !value.ends_with("/..")
        && !value.contains("/../")
        && !value.starts_with("./")
        && !value.contains("/./")
        && !value.ends_with("/.")
        && !value.contains("//")
        && value.chars().all(|character| {
            (!character.is_ascii() || !character.is_ascii_control())
                && !matches!(character, ':' | '\\')
        })
}

impl fmt::Display for PatchLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A root-owned registry override for one module.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SingleVersionOverride {
    module_name: ModuleName,
    version: Version,
    registry: Box<str>,
    patches: Box<[PatchLabel]>,
    patch_cmds: Box<[Box<str>]>,
    patch_strip: i32,
}

impl SingleVersionOverride {
    pub fn new(
        module_name: ModuleName,
        version: Version,
        registry: Box<str>,
        patches: Box<[PatchLabel]>,
        patch_cmds: Box<[Box<str>]>,
        patch_strip: i32,
    ) -> Self {
        Self {
            module_name,
            version,
            registry,
            patches,
            patch_cmds,
            patch_strip,
        }
    }

    pub fn module_name(&self) -> &ModuleName {
        &self.module_name
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn registry(&self) -> &str {
        &self.registry
    }

    pub fn patches(&self) -> &[PatchLabel] {
        &self.patches
    }

    pub fn patch_cmds(&self) -> &[Box<str>] {
        &self.patch_cmds
    }

    pub fn patch_strip(&self) -> i32 {
        self.patch_strip
    }
}

/// A root-owned non-registry override backed by an opaque local path.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LocalPathOverride {
    module_name: ModuleName,
    path: Box<str>,
}

impl LocalPathOverride {
    pub fn new(module_name: ModuleName, path: Box<str>) -> Self {
        Self { module_name, path }
    }

    pub fn module_name(&self) -> &ModuleName {
        &self.module_name
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

/// One supported root module override, retained in declaration order.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ModuleOverride {
    SingleVersion(SingleVersionOverride),
    LocalPath(LocalPathOverride),
}

impl ModuleOverride {
    pub fn module_name(&self) -> &ModuleName {
        match self {
            Self::SingleVersion(value) => value.module_name(),
            Self::LocalPath(value) => value.module_name(),
        }
    }

    pub fn as_single_version(&self) -> Option<&SingleVersionOverride> {
        match self {
            Self::SingleVersion(value) => Some(value),
            Self::LocalPath(_) => None,
        }
    }

    pub fn as_local_path(&self) -> Option<&LocalPathOverride> {
        match self {
            Self::SingleVersion(_) => None,
            Self::LocalPath(value) => Some(value),
        }
    }
}

/// The immutable result of a `module()` directive.
///
/// The name is optional because only the root file may omit it. Contextual
/// root/dependency validation belongs to the restricted evaluator.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ModuleDeclaration {
    name: Option<ModuleName>,
    version: Version,
    repo_name: Option<Box<str>>,
    bazel_compatibility: Box<[Box<str>]>,
}

impl ModuleDeclaration {
    pub fn new(
        name: Option<ModuleName>,
        version: Version,
        repo_name: Option<Box<str>>,
        bazel_compatibility: Box<[Box<str>]>,
    ) -> Self {
        Self {
            name,
            version,
            repo_name,
            bazel_compatibility,
        }
    }

    pub fn name(&self) -> Option<&ModuleName> {
        self.name.as_ref()
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn repo_name(&self) -> Option<&str> {
        self.repo_name.as_deref()
    }

    pub fn bazel_compatibility(&self) -> &[Box<str>] {
        &self.bazel_compatibility
    }
}

/// The apparent repository edge created by `bazel_dep`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DependencyRepoName {
    /// A normal dependency visible through this apparent repository name.
    Apparent(Box<str>),
    /// `repo_name = None`: constrain an already-present module without adding
    /// a graph edge.
    Nodep,
}

/// The immutable result of a `bazel_dep()` directive.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DependencyRequest {
    module: ModuleKey,
    repo_name: DependencyRepoName,
    dev_dependency: bool,
}

impl DependencyRequest {
    pub fn new(
        module: ModuleKey,
        repo_name: DependencyRepoName,
        dev_dependency: bool,
    ) -> Result<Self, DependencyRequestError> {
        if module.is_root() {
            return Err(DependencyRequestError);
        }
        Ok(Self {
            module,
            repo_name,
            dev_dependency,
        })
    }

    pub fn module(&self) -> &ModuleKey {
        &self.module
    }

    pub fn repo_name(&self) -> &DependencyRepoName {
        &self.repo_name
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }

    pub fn is_nodep(&self) -> bool {
        matches!(self.repo_name, DependencyRepoName::Nodep)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRequestError;

impl fmt::Display for DependencyRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bazel_dep cannot name the root module")
    }
}

impl Error for DependencyRequestError {}

/// The supported immutable output of one `MODULE.bazel` evaluation.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ModuleFile {
    declaration: Option<ModuleDeclaration>,
    dependencies: Box<[DependencyRequest]>,
    overrides: Box<[ModuleOverride]>,
}

impl ModuleFile {
    pub fn new(
        declaration: Option<ModuleDeclaration>,
        dependencies: Box<[DependencyRequest]>,
        overrides: Box<[ModuleOverride]>,
    ) -> Self {
        Self {
            declaration,
            dependencies,
            overrides,
        }
    }

    pub fn declaration(&self) -> Option<&ModuleDeclaration> {
        self.declaration.as_ref()
    }

    pub fn dependencies(&self) -> &[DependencyRequest] {
        &self.dependencies
    }

    pub fn overrides(&self) -> &[ModuleOverride] {
        &self.overrides
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_patch_labels_are_constructible_only_from_canonical_forms() {
        for value in [
            "//:root.patch",
            "//pkg:patch.diff",
            "//:.",
            "@@rules_cc+//patches:unicode-π.patch",
        ] {
            assert_eq!(PatchLabel::parse_normalized(value).unwrap().as_str(), value);
        }

        for value in [
            "",
            ":relative.patch",
            "relative.patch",
            "@repo//:patch.diff",
            "@@//:patch.diff",
            "@@bad repo//:patch.diff",
            "@@bad~repo//:patch.diff",
            "//pkg",
            "///pkg:patch.diff",
            "//pkg/:patch.diff",
            "//pkg//nested:patch.diff",
            "//...:patch.diff",
            "//pkg:/patch.diff",
            "//pkg:../patch.diff",
            "//pkg:patch/../bad.diff",
            "//pkg:patch/.",
            "//pkg:bad:target",
            "//pkg:bad\\target",
            "//pkg:\u{7f}",
        ] {
            assert_eq!(
                PatchLabel::parse_normalized(value),
                Err(PatchLabelParseError::new(value)),
                "accepted {value:?}"
            );
        }
    }

    #[test]
    fn preserves_regular_dev_and_nodep_edges() {
        let name = ModuleName::parse("rules_go").unwrap();
        let key = ModuleKey::registry(name, Version::parse("0.50.0").unwrap()).unwrap();
        let regular = DependencyRequest::new(
            key.clone(),
            DependencyRepoName::Apparent("go_rules".into()),
            false,
        )
        .unwrap();
        let nodep_dev = DependencyRequest::new(key, DependencyRepoName::Nodep, true).unwrap();

        let file = ModuleFile::new(
            None,
            vec![regular, nodep_dev].into_boxed_slice(),
            Box::new([]),
        );
        assert!(file.declaration().is_none());
        assert!(!file.dependencies()[0].is_nodep());
        assert!(file.dependencies()[1].is_nodep());
        assert!(file.dependencies()[1].is_dev_dependency());
    }

    #[test]
    fn rejects_root_dependency_without_panicking() {
        assert_eq!(
            DependencyRequest::new(
                ModuleKey::ROOT,
                DependencyRepoName::Apparent("root".into()),
                false,
            ),
            Err(DependencyRequestError)
        );
    }
}
