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
}

impl ModuleFile {
    pub fn new(
        declaration: Option<ModuleDeclaration>,
        dependencies: Box<[DependencyRequest]>,
    ) -> Self {
        Self {
            declaration,
            dependencies,
        }
    }

    pub fn declaration(&self) -> Option<&ModuleDeclaration> {
        self.declaration.as_ref()
    }

    pub fn dependencies(&self) -> &[DependencyRequest] {
        &self.dependencies
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let file = ModuleFile::new(None, vec![regular, nodep_dev].into_boxed_slice());
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
