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
use crate::ModuleSourceLocation;
use crate::RawAttribute;
use crate::RawAttributeValue;
use crate::RepositoryName;

/// An opaque, unevaluated bzl-file spelling supplied to `use_repo_rule`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoRuleBzlFile(Box<str>);

impl RepoRuleBzlFile {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<Box<str>>> From<T> for RepoRuleBzlFile {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for RepoRuleBzlFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An opaque, unevaluated repository-rule symbol spelling.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoRuleName(Box<str>);

impl RepoRuleName {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<Box<str>>> From<T> for RepoRuleName {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for RepoRuleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The owner-scoped identity of one repository rule exposed by `use_repo_rule`.
///
/// The bzl file and rule name deliberately retain their raw source spellings.
/// Loading, label normalization, symbol validation, and repository-rule schema
/// checks belong to later phases. Equality keeps those components structurally
/// distinct instead of reproducing Bazel's unchecked space-concatenation
/// collision for invalid spellings; valid module inputs are unaffected.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoRuleUseId {
    owner: ModuleKey,
    bzl_file: RepoRuleBzlFile,
    rule_name: RepoRuleName,
}

impl RepoRuleUseId {
    pub fn new(
        owner: ModuleKey,
        bzl_file: impl Into<RepoRuleBzlFile>,
        rule_name: impl Into<RepoRuleName>,
    ) -> Self {
        Self {
            owner,
            bzl_file: bzl_file.into(),
            rule_name: rule_name.into(),
        }
    }

    pub fn owner(&self) -> &ModuleKey {
        &self.owner
    }

    pub fn bzl_file(&self) -> &RepoRuleBzlFile {
        &self.bzl_file
    }

    pub fn rule_name(&self) -> &RepoRuleName {
        &self.rule_name
    }

    /// Bazel's derived innate-extension name. This is not stored separately,
    /// so raw identity components cannot disagree with it.
    pub fn innate_extension_name(&self) -> String {
        format!("{} {}", self.bzl_file, self.rule_name)
    }
}

/// One retained invocation of a proxy returned by `use_repo_rule`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoRuleInvocation {
    ordinal: u32,
    repository_name: RepositoryName,
    attributes: Box<[RawAttribute]>,
    dev_dependency: bool,
    location: ModuleSourceLocation,
}

impl RepoRuleInvocation {
    pub fn new(
        ordinal: u32,
        repository_name: RepositoryName,
        attributes: Box<[RawAttribute]>,
        dev_dependency: bool,
        location: ModuleSourceLocation,
    ) -> Result<Self, RepoRuleInvocationError> {
        for (index, attribute) in attributes.iter().enumerate() {
            if attribute.name() == "name" || attribute.name() == "dev_dependency" {
                return Err(RepoRuleInvocationError::ReservedAttribute(
                    attribute.name().into(),
                ));
            }
            if attributes[..index]
                .iter()
                .any(|existing| existing.name() == attribute.name())
            {
                return Err(RepoRuleInvocationError::DuplicateAttribute(
                    attribute.name().into(),
                ));
            }
        }
        Ok(Self {
            ordinal,
            repository_name,
            attributes,
            dev_dependency,
            location,
        })
    }

    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn repository_name(&self) -> &RepositoryName {
        &self.repository_name
    }

    /// Raw keyword arguments, excluding `name` and `dev_dependency`.
    pub fn attributes(&self) -> &[RawAttribute] {
        &self.attributes
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }

    pub fn location(&self) -> &ModuleSourceLocation {
        &self.location
    }

    /// Derives the fixed innate-extension `repo` tag without duplicating it in
    /// the stored record.
    pub fn evaluation_projection(&self) -> RepoRuleInvocationEvaluationProjection {
        let mut attributes = self.attributes.to_vec();
        attributes.push(RawAttribute::new(
            "name",
            RawAttributeValue::String(self.repository_name.as_str().into()),
        ));
        RepoRuleInvocationEvaluationProjection {
            repository_name: self.repository_name.clone(),
            attributes: attributes.into_boxed_slice(),
            dev_dependency: self.dev_dependency,
        }
    }

    /// Derives the fixed local-to-exported identity import.
    pub fn repository_mapping_projection(&self) -> RepoRuleImportProjection {
        RepoRuleImportProjection {
            local_name: self.repository_name.clone(),
            exported_name: self.repository_name.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepoRuleInvocationError {
    DuplicateAttribute(Box<str>),
    ReservedAttribute(Box<str>),
}

impl fmt::Display for RepoRuleInvocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateAttribute(name) => {
                write!(f, "duplicate repository rule argument '{name}'")
            }
            Self::ReservedAttribute(name) => {
                write!(
                    f,
                    "repository rule argument '{name}' is stored structurally"
                )
            }
        }
    }
}

impl Error for RepoRuleInvocationError {}

/// All retained calls associated with one owner-scoped raw repository rule.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoRuleUse {
    first_use_ordinal: u32,
    id: RepoRuleUseId,
    invocations: Box<[RepoRuleInvocation]>,
}

impl RepoRuleUse {
    pub fn new(
        first_use_ordinal: u32,
        id: RepoRuleUseId,
        invocations: Box<[RepoRuleInvocation]>,
    ) -> Result<Self, RepoRuleUseError> {
        let Some(first) = invocations.first() else {
            return Err(RepoRuleUseError::NoInvocations);
        };
        if first.ordinal <= first_use_ordinal {
            return Err(RepoRuleUseError::FirstInvocationNotAfterFirstUse {
                first_use: first_use_ordinal,
                first_invocation: first.ordinal,
            });
        }
        for invocations in invocations.windows(2) {
            if invocations[0].ordinal >= invocations[1].ordinal {
                return Err(RepoRuleUseError::InvocationsNotSourceOrdered {
                    previous: invocations[0].ordinal,
                    next: invocations[1].ordinal,
                });
            }
        }
        let mut names = BTreeSet::new();
        for invocation in invocations.iter() {
            if !names.insert(invocation.repository_name.clone()) {
                return Err(RepoRuleUseError::DuplicateRepositoryName(
                    invocation.repository_name.clone(),
                ));
            }
        }
        Ok(Self {
            first_use_ordinal,
            id,
            invocations,
        })
    }

    pub fn first_use_ordinal(&self) -> u32 {
        self.first_use_ordinal
    }

    pub fn id(&self) -> &RepoRuleUseId {
        &self.id
    }

    pub fn invocations(&self) -> &[RepoRuleInvocation] {
        &self.invocations
    }

    /// Semantic repository-rule inputs in invocation order. Source locations
    /// and absolute event ordinals are intentionally omitted.
    pub fn evaluation_projection(&self) -> RepoRuleEvaluationProjection {
        RepoRuleEvaluationProjection {
            id: self.id.clone(),
            invocations: self
                .invocations
                .iter()
                .map(RepoRuleInvocation::evaluation_projection)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Apparent repository imports partitioned by development policy and
    /// sorted so source regrouping does not perturb repository mapping input.
    pub fn repository_mapping_projection(&self) -> RepoRuleRepositoryMappingProjection {
        let mut regular_imports = Vec::new();
        let mut dev_imports = Vec::new();
        for invocation in self.invocations.iter() {
            let destination = if invocation.dev_dependency {
                &mut dev_imports
            } else {
                &mut regular_imports
            };
            destination.push(invocation.repository_mapping_projection());
        }
        regular_imports.sort();
        dev_imports.sort();
        RepoRuleRepositoryMappingProjection {
            id: self.id.clone(),
            regular_imports: regular_imports.into_boxed_slice(),
            dev_imports: dev_imports.into_boxed_slice(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepoRuleUseError {
    NoInvocations,
    FirstInvocationNotAfterFirstUse {
        first_use: u32,
        first_invocation: u32,
    },
    InvocationsNotSourceOrdered {
        previous: u32,
        next: u32,
    },
    DuplicateRepositoryName(RepositoryName),
}

impl fmt::Display for RepoRuleUseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoInvocations => f.write_str("a repository rule use requires an invocation"),
            Self::FirstInvocationNotAfterFirstUse {
                first_use,
                first_invocation,
            } => write!(
                f,
                "first repository rule invocation ordinal {first_invocation} must follow first-use ordinal {first_use}"
            ),
            Self::InvocationsNotSourceOrdered { previous, next } => write!(
                f,
                "repository rule invocation ordinals must be source ordered, got {previous} then {next}"
            ),
            Self::DuplicateRepositoryName(name) => {
                write!(f, "duplicate repository rule invocation name '{name}'")
            }
        }
    }
}

impl Error for RepoRuleUseError {}

/// One fixed innate-extension `repo` tag. The tag name is structural and thus
/// intentionally absent from this projection.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoRuleInvocationEvaluationProjection {
    repository_name: RepositoryName,
    attributes: Box<[RawAttribute]>,
    dev_dependency: bool,
}

impl RepoRuleInvocationEvaluationProjection {
    pub const TAG_NAME: &'static str = "repo";

    pub fn repository_name(&self) -> &RepositoryName {
        &self.repository_name
    }

    pub fn attributes(&self) -> &[RawAttribute] {
        &self.attributes
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoRuleEvaluationProjection {
    id: RepoRuleUseId,
    invocations: Box<[RepoRuleInvocationEvaluationProjection]>,
}

impl RepoRuleEvaluationProjection {
    pub fn id(&self) -> &RepoRuleUseId {
        &self.id
    }

    pub fn invocations(&self) -> &[RepoRuleInvocationEvaluationProjection] {
        &self.invocations
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoRuleImportProjection {
    local_name: RepositoryName,
    exported_name: RepositoryName,
}

impl RepoRuleImportProjection {
    pub fn local_name(&self) -> &RepositoryName {
        &self.local_name
    }

    pub fn exported_name(&self) -> &RepositoryName {
        &self.exported_name
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoRuleRepositoryMappingProjection {
    id: RepoRuleUseId,
    regular_imports: Box<[RepoRuleImportProjection]>,
    dev_imports: Box<[RepoRuleImportProjection]>,
}

impl RepoRuleRepositoryMappingProjection {
    pub fn id(&self) -> &RepoRuleUseId {
        &self.id
    }

    pub fn regular_imports(&self) -> &[RepoRuleImportProjection] {
        &self.regular_imports
    }

    pub fn dev_imports(&self) -> &[RepoRuleImportProjection] {
        &self.dev_imports
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApparentLabel;
    use crate::DependencyRepoName;
    use crate::DependencyRequest;
    use crate::ExtensionIsolation;
    use crate::ExtensionName;
    use crate::ExtensionUse;
    use crate::ExtensionUseKind;
    use crate::ModuleDeclaration;
    use crate::ModuleFile;
    use crate::ModuleFileRepoRuleUseError;
    use crate::ModuleName;
    use crate::ProxyUse;
    use crate::RawInteger;
    use crate::RepoImport;
    use crate::Version;

    fn repo(value: &str) -> RepositoryName {
        RepositoryName::parse(value).unwrap()
    }

    fn invocation(ordinal: u32, name: &str, dev_dependency: bool) -> RepoRuleInvocation {
        RepoRuleInvocation::new(
            ordinal,
            repo(name),
            vec![RawAttribute::new(
                "value",
                RawAttributeValue::Integer(RawInteger::from(ordinal as i64)),
            )]
            .into_boxed_slice(),
            dev_dependency,
            ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
        )
        .unwrap()
    }

    fn use_value(
        first_use: u32,
        owner: ModuleKey,
        bzl_file: &str,
        rule_name: &str,
        invocation_ordinal: u32,
        repository_name: &str,
    ) -> RepoRuleUse {
        RepoRuleUse::new(
            first_use,
            RepoRuleUseId::new(owner, bzl_file, rule_name),
            vec![invocation(invocation_ordinal, repository_name, false)].into(),
        )
        .unwrap()
    }

    fn import_extension(first_use: u32, local: &str) -> ExtensionUse {
        ExtensionUse::new(
            first_use,
            ExtensionUseKind::Regular {
                extension_file: ApparentLabel::parse_normalized("//:ext.bzl").unwrap(),
                extension_name: ExtensionName::parse("ext").unwrap(),
            },
            ExtensionIsolation::None,
            vec![
                ProxyUse::new(
                    first_use + 1,
                    None,
                    false,
                    ModuleSourceLocation::new("MODULE.bazel:1"),
                    vec![RepoImport::new(
                        first_use + 2,
                        repo(local),
                        repo("exported"),
                        ModuleSourceLocation::new("MODULE.bazel:2"),
                    )]
                    .into(),
                )
                .unwrap(),
            ]
            .into(),
            Box::new([]),
        )
        .unwrap()
    }

    #[test]
    fn identities_are_owner_scoped_and_raw_spellings_are_opaque() {
        let root = RepoRuleUseId::new(ModuleKey::ROOT, "bad label", "_private-name");
        let dependency = RepoRuleUseId::new(
            ModuleKey::new(
                ModuleName::parse("dep").unwrap(),
                Version::parse("1.0").unwrap(),
            ),
            "bad label",
            "_private-name",
        );
        assert_ne!(root, dependency);
        assert_eq!(root.bzl_file().as_str(), "bad label");
        assert_eq!(root.rule_name().as_str(), "_private-name");
        assert_eq!(root.innate_extension_name(), "bad label _private-name");
    }

    #[test]
    fn validates_nonempty_ordered_unique_invocations_and_arguments() {
        let id = RepoRuleUseId::new(ModuleKey::ROOT, "//:repo.bzl", "repo");
        assert_eq!(
            RepoRuleUse::new(0, id.clone(), Box::new([])),
            Err(RepoRuleUseError::NoInvocations)
        );
        assert!(matches!(
            RepoRuleUse::new(2, id.clone(), vec![invocation(2, "one", false)].into()),
            Err(RepoRuleUseError::FirstInvocationNotAfterFirstUse { .. })
        ));
        assert!(matches!(
            RepoRuleUse::new(
                0,
                id.clone(),
                vec![invocation(2, "one", false), invocation(1, "two", false)].into()
            ),
            Err(RepoRuleUseError::InvocationsNotSourceOrdered { .. })
        ));
        assert_eq!(
            RepoRuleUse::new(
                0,
                id,
                vec![invocation(1, "same", false), invocation(2, "same", true)].into()
            ),
            Err(RepoRuleUseError::DuplicateRepositoryName(repo("same")))
        );

        assert_eq!(
            RepoRuleInvocation::new(
                1,
                repo("one"),
                vec![
                    RawAttribute::new("same", RawAttributeValue::Bool(true)),
                    RawAttribute::new("same", RawAttributeValue::Bool(false)),
                ]
                .into(),
                false,
                ModuleSourceLocation::new("MODULE.bazel:1"),
            ),
            Err(RepoRuleInvocationError::DuplicateAttribute("same".into()))
        );
    }

    #[test]
    fn projections_derive_fixed_fields_preserve_evaluation_order_and_sort_mapping() {
        let use_value = RepoRuleUse::new(
            0,
            RepoRuleUseId::new(ModuleKey::ROOT, "//:repo.bzl", "rule"),
            vec![
                invocation(1, "z_dev", true),
                invocation(2, "z", false),
                invocation(3, "a_dev", true),
                invocation(4, "a", false),
            ]
            .into(),
        )
        .unwrap();

        let evaluation = use_value.evaluation_projection();
        assert_eq!(
            evaluation
                .invocations()
                .iter()
                .map(|value| value.attributes().last().unwrap().value())
                .collect::<Vec<_>>(),
            ["z_dev", "z", "a_dev", "a"]
                .map(|value| RawAttributeValue::String(value.into()))
                .iter()
                .collect::<Vec<_>>()
        );
        assert_eq!(RepoRuleInvocationEvaluationProjection::TAG_NAME, "repo");

        let mapping = use_value.repository_mapping_projection();
        assert_eq!(
            mapping
                .regular_imports()
                .iter()
                .map(|value| value.local_name().as_str())
                .collect::<Vec<_>>(),
            ["a", "z"]
        );
        assert_eq!(
            mapping
                .dev_imports()
                .iter()
                .map(|value| value.local_name().as_str())
                .collect::<Vec<_>>(),
            ["a_dev", "z_dev"]
        );
    }

    #[test]
    fn module_file_validates_repo_rule_owners_identities_and_cross_family_ordinals() {
        let dependency_owner = ModuleKey::new(
            ModuleName::parse("dep").unwrap(),
            Version::parse("1.0").unwrap(),
        );
        assert_eq!(
            ModuleFile::new(None, Box::new([]), Box::new([])).with_repo_rule_uses(
                vec![
                    use_value(0, ModuleKey::ROOT, "x", "r", 1, "one"),
                    use_value(2, dependency_owner, "y", "r", 3, "two"),
                ]
                .into()
            ),
            Err(ModuleFileRepoRuleUseError::MixedRepoRuleOwners)
        );
        assert!(matches!(
            ModuleFile::new(None, Box::new([]), Box::new([])).with_repo_rule_uses(
                vec![
                    use_value(0, ModuleKey::ROOT, "x", "r", 1, "one"),
                    use_value(2, ModuleKey::ROOT, "x", "r", 3, "two"),
                ]
                .into()
            ),
            Err(ModuleFileRepoRuleUseError::DuplicateRepoRuleIdentity { .. })
        ));

        let extension = import_extension(0, "extension_repo");
        assert_eq!(
            ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![extension].into())
                .unwrap()
                .with_repo_rule_uses(
                    vec![use_value(2, ModuleKey::ROOT, "x", "r", 3, "rule_repo")].into()
                ),
            Err(ModuleFileRepoRuleUseError::DuplicateEventOrdinal(2))
        );
    }

    #[test]
    fn module_file_rejects_repo_rule_names_colliding_with_every_local_repo_family() {
        let declaration = ModuleDeclaration::new(
            Some(ModuleName::parse("root").unwrap()),
            Version::EMPTY,
            Some("same".into()),
            Box::new([]),
        );
        assert_eq!(
            ModuleFile::new(Some(declaration), Box::new([]), Box::new([])).with_repo_rule_uses(
                vec![use_value(0, ModuleKey::ROOT, "x", "r", 1, "same")].into()
            ),
            Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                "same".into()
            ))
        );

        let dependency = DependencyRequest::new(
            ModuleKey::new(
                ModuleName::parse("dep").unwrap(),
                Version::parse("1.0").unwrap(),
            ),
            DependencyRepoName::Apparent("same".into()),
            false,
        )
        .unwrap();
        assert_eq!(
            ModuleFile::new(None, vec![dependency].into(), Box::new([])).with_repo_rule_uses(
                vec![use_value(0, ModuleKey::ROOT, "x", "r", 1, "same")].into()
            ),
            Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                "same".into()
            ))
        );

        assert_eq!(
            ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![import_extension(0, "same")].into())
                .unwrap()
                .with_repo_rule_uses(
                    vec![use_value(3, ModuleKey::ROOT, "x", "r", 4, "same")].into()
                ),
            Err(ModuleFileRepoRuleUseError::DuplicateLocalRepoName(
                "same".into()
            ))
        );
    }
}
