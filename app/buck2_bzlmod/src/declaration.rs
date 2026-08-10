/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use crate::ModuleKey;
use crate::ModuleName;
use crate::ModuleRegistrations;
use crate::RepoRuleUse;
use crate::Version;
use crate::module_extension::ExtensionUse;

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
    extension_uses: Box<[ExtensionUse]>,
    repo_rule_uses: Box<[RepoRuleUse]>,
    registrations: ModuleRegistrations,
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
            extension_uses: Box::new([]),
            repo_rule_uses: Box::new([]),
            registrations: ModuleRegistrations::default(),
        }
    }

    /// Attaches the source-ordered registration records from this module file.
    pub fn with_registrations(mut self, registrations: ModuleRegistrations) -> Self {
        self.registrations = registrations;
        self
    }

    /// Attaches source-ordered module extension uses to the evaluated file.
    pub fn with_extension_uses(
        mut self,
        extension_uses: Box<[ExtensionUse]>,
    ) -> Result<Self, ModuleFileExtensionUseError> {
        validate_uses(&self, &extension_uses, &self.repo_rule_uses).map_err(
            |error| match error {
                ModuleFileRepoRuleUseError::ExtensionUsesNotSourceOrdered { previous, next } => {
                    ModuleFileExtensionUseError::UsesNotSourceOrdered { previous, next }
                }
                ModuleFileRepoRuleUseError::DuplicateExtensionIdentity { first_use } => {
                    ModuleFileExtensionUseError::DuplicateExtensionIdentity { first_use }
                }
                ModuleFileRepoRuleUseError::DuplicateEventOrdinal(ordinal) => {
                    ModuleFileExtensionUseError::DuplicateEventOrdinal(ordinal)
                }
                ModuleFileRepoRuleUseError::DuplicateLocalRepoName(name) => {
                    ModuleFileExtensionUseError::DuplicateLocalRepoName(name)
                }
                ModuleFileRepoRuleUseError::RepoRuleUsesNotSourceOrdered { .. }
                | ModuleFileRepoRuleUseError::DuplicateRepoRuleIdentity { .. }
                | ModuleFileRepoRuleUseError::MixedRepoRuleOwners => {
                    unreachable!("attached repository rule uses were previously validated")
                }
            },
        )?;
        self.extension_uses = extension_uses;
        Ok(self)
    }

    /// Attaches source-ordered owner-scoped repository rule uses.
    pub fn with_repo_rule_uses(
        mut self,
        repo_rule_uses: Box<[RepoRuleUse]>,
    ) -> Result<Self, ModuleFileRepoRuleUseError> {
        validate_uses(&self, &self.extension_uses, &repo_rule_uses)?;
        self.repo_rule_uses = repo_rule_uses;
        Ok(self)
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

    pub fn extension_uses(&self) -> &[ExtensionUse] {
        &self.extension_uses
    }

    pub fn repo_rule_uses(&self) -> &[RepoRuleUse] {
        &self.repo_rule_uses
    }

    pub fn registrations(&self) -> &ModuleRegistrations {
        &self.registrations
    }
}

fn validate_uses(
    module_file: &ModuleFile,
    extension_uses: &[ExtensionUse],
    repo_rule_uses: &[RepoRuleUse],
) -> Result<(), ModuleFileRepoRuleUseError> {
    for uses in extension_uses.windows(2) {
        if uses[0].first_use_ordinal() >= uses[1].first_use_ordinal() {
            return Err(ModuleFileRepoRuleUseError::ExtensionUsesNotSourceOrdered {
                previous: uses[0].first_use_ordinal(),
                next: uses[1].first_use_ordinal(),
            });
        }
    }
    for uses in repo_rule_uses.windows(2) {
        if uses[0].first_use_ordinal() >= uses[1].first_use_ordinal() {
            return Err(ModuleFileRepoRuleUseError::RepoRuleUsesNotSourceOrdered {
                previous: uses[0].first_use_ordinal(),
                next: uses[1].first_use_ordinal(),
            });
        }
    }

    let mut identities = BTreeSet::new();
    let mut repo_rule_identities = BTreeSet::new();
    let mut repo_rule_owner = None;
    let mut event_ordinals = BTreeSet::new();
    for extension_use in extension_uses.iter() {
        if !identities.insert((
            extension_use.kind().clone(),
            extension_use.isolation().clone(),
        )) {
            return Err(ModuleFileRepoRuleUseError::DuplicateExtensionIdentity {
                first_use: extension_use.first_use_ordinal(),
            });
        }
        if !event_ordinals.insert(extension_use.first_use_ordinal()) {
            return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                extension_use.first_use_ordinal(),
            ));
        }
        for proxy in extension_use.proxies() {
            if !event_ordinals.insert(proxy.ordinal()) {
                return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                    proxy.ordinal(),
                ));
            }
            for import in proxy.imports() {
                if !event_ordinals.insert(import.ordinal()) {
                    return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                        import.ordinal(),
                    ));
                }
            }
        }
        for tag in extension_use.tags() {
            if !event_ordinals.insert(tag.ordinal()) {
                return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                    tag.ordinal(),
                ));
            }
        }
    }
    for repo_rule_use in repo_rule_uses {
        if let Some(owner) = repo_rule_owner {
            if owner != repo_rule_use.id().owner() {
                return Err(ModuleFileRepoRuleUseError::MixedRepoRuleOwners);
            }
        } else {
            repo_rule_owner = Some(repo_rule_use.id().owner());
        }
        if !repo_rule_identities.insert(repo_rule_use.id().clone()) {
            return Err(ModuleFileRepoRuleUseError::DuplicateRepoRuleIdentity {
                first_use: repo_rule_use.first_use_ordinal(),
            });
        }
        if !event_ordinals.insert(repo_rule_use.first_use_ordinal()) {
            return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                repo_rule_use.first_use_ordinal(),
            ));
        }
        for invocation in repo_rule_use.invocations() {
            if !event_ordinals.insert(invocation.ordinal()) {
                return Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(
                    invocation.ordinal(),
                ));
            }
        }
    }

    let mut local_repo_names = BTreeSet::new();
    if let Some(declaration) = module_file.declaration.as_ref() {
        let own_apparent_repo = declaration
            .repo_name()
            .filter(|repo_name| !repo_name.is_empty())
            .or_else(|| declaration.name().map(ModuleName::as_str));
        if let Some(repo_name) = own_apparent_repo {
            local_repo_names.insert(repo_name.to_owned());
        }
    }
    for dependency in module_file.dependencies.iter() {
        if let DependencyRepoName::Apparent(repo_name) = dependency.repo_name() {
            if !local_repo_names.insert(repo_name.to_string()) {
                return Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                    repo_name.clone(),
                ));
            }
        }
    }
    for extension_use in extension_uses.iter() {
        for proxy in extension_use.proxies() {
            for import in proxy.imports() {
                if !local_repo_names.insert(import.local_name().as_str().to_owned()) {
                    return Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                        import.local_name().as_str().into(),
                    ));
                }
            }
        }
    }
    for repo_rule_use in repo_rule_uses {
        for invocation in repo_rule_use.invocations() {
            if !local_repo_names.insert(invocation.repository_name().as_str().to_owned()) {
                return Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                    invocation.repository_name().as_str().into(),
                ));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleFileExtensionUseError {
    UsesNotSourceOrdered { previous: u32, next: u32 },
    DuplicateExtensionIdentity { first_use: u32 },
    DuplicateEventOrdinal(u32),
    DuplicateLocalRepoName(Box<str>),
}

impl fmt::Display for ModuleFileExtensionUseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UsesNotSourceOrdered { previous, next } => write!(
                f,
                "module extension uses must be source ordered, got {previous} then {next}"
            ),
            Self::DuplicateExtensionIdentity { first_use } => write!(
                f,
                "duplicate module extension identity at first-use ordinal {first_use}"
            ),
            Self::DuplicateEventOrdinal(ordinal) => {
                write!(f, "duplicate module extension event ordinal {ordinal}")
            }
            Self::DuplicateLocalRepoName(name) => {
                write!(f, "duplicate module-local repository name '{name}'")
            }
        }
    }
}

impl Error for ModuleFileExtensionUseError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleFileRepoRuleUseError {
    ExtensionUsesNotSourceOrdered { previous: u32, next: u32 },
    RepoRuleUsesNotSourceOrdered { previous: u32, next: u32 },
    DuplicateExtensionIdentity { first_use: u32 },
    DuplicateRepoRuleIdentity { first_use: u32 },
    MixedRepoRuleOwners,
    DuplicateEventOrdinal(u32),
    DuplicateLocalRepoName(Box<str>),
}

impl fmt::Display for ModuleFileRepoRuleUseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExtensionUsesNotSourceOrdered { previous, next } => write!(
                f,
                "module extension uses must be source ordered, got {previous} then {next}"
            ),
            Self::RepoRuleUsesNotSourceOrdered { previous, next } => write!(
                f,
                "repository rule uses must be source ordered, got {previous} then {next}"
            ),
            Self::DuplicateExtensionIdentity { first_use } => write!(
                f,
                "duplicate module extension identity at first-use ordinal {first_use}"
            ),
            Self::DuplicateRepoRuleIdentity { first_use } => write!(
                f,
                "duplicate repository rule identity at first-use ordinal {first_use}"
            ),
            Self::MixedRepoRuleOwners => {
                f.write_str("repository rule uses in one module file must have one owner")
            }
            Self::DuplicateEventOrdinal(ordinal) => {
                write!(f, "duplicate module event ordinal {ordinal}")
            }
            Self::DuplicateLocalRepoName(name) => {
                write!(f, "duplicate module-local repository name '{name}'")
            }
        }
    }
}

impl Error for ModuleFileRepoRuleUseError {}

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
        assert!(file.extension_uses().is_empty());
        assert_eq!(file.registrations(), &ModuleRegistrations::default());

        let registrations = ModuleRegistrations::new(
            vec![crate::RawAbsoluteTargetPattern::parse("//:host").unwrap()].into_boxed_slice(),
            vec![crate::RawAbsoluteTargetPattern::parse("@repo//:all").unwrap()].into_boxed_slice(),
        );
        let file = file.with_registrations(registrations.clone());
        assert_eq!(file.registrations(), &registrations);
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
