/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Pure data types used by Bazel module resolution.
//!
//! This crate intentionally has no evaluator, filesystem, network, or server
//! dependencies. It defines the immutable values exchanged by those layers.

mod declaration;
mod module_extension;
mod module_key;
mod module_name;
mod repo_rule;
mod version;

pub use declaration::DependencyRepoName;
pub use declaration::DependencyRequest;
pub use declaration::DependencyRequestError;
pub use declaration::LocalPathOverride;
pub use declaration::ModuleDeclaration;
pub use declaration::ModuleFile;
pub use declaration::ModuleFileExtensionUseError;
pub use declaration::ModuleFileRepoRuleUseError;
pub use declaration::ModuleOverride;
pub use declaration::PatchLabel;
pub use declaration::PatchLabelParseError;
pub use declaration::SingleVersionOverride;
pub use module_extension::ApparentLabel;
pub use module_extension::ApparentLabelParseError;
pub use module_extension::ExtensionEvaluationProjection;
pub use module_extension::ExtensionIsolation;
pub use module_extension::ExtensionName;
pub use module_extension::ExtensionNameParseError;
pub use module_extension::ExtensionRepositoryMappingProjection;
pub use module_extension::ExtensionUse;
pub use module_extension::ExtensionUseError;
pub use module_extension::ExtensionUseKind;
pub use module_extension::ModuleSourceLocation;
pub use module_extension::OrderedStringDict;
pub use module_extension::OrderedStringDictError;
pub use module_extension::ProxyUse;
pub use module_extension::ProxyUseError;
pub use module_extension::RawAttribute;
pub use module_extension::RawAttributeValue;
pub use module_extension::RawInteger;
pub use module_extension::RawIntegerParseError;
pub use module_extension::RawTag;
pub use module_extension::RawTagError;
pub use module_extension::RepoImport;
pub use module_extension::RepoImportProjection;
pub use module_extension::RepositoryName;
pub use module_extension::RepositoryNameParseError;
pub use module_extension::StarlarkBindingName;
pub use module_extension::StarlarkBindingNameParseError;
pub use module_extension::TagEvaluationProjection;
pub use module_key::ModuleKey;
pub use module_key::ModuleKeyParseError;
pub use module_name::ModuleName;
pub use module_name::ModuleNameParseError;
pub use repo_rule::RepoRuleBzlFile;
pub use repo_rule::RepoRuleEvaluationProjection;
pub use repo_rule::RepoRuleImportProjection;
pub use repo_rule::RepoRuleInvocation;
pub use repo_rule::RepoRuleInvocationError;
pub use repo_rule::RepoRuleInvocationEvaluationProjection;
pub use repo_rule::RepoRuleName;
pub use repo_rule::RepoRuleRepositoryMappingProjection;
pub use repo_rule::RepoRuleUse;
pub use repo_rule::RepoRuleUseError;
pub use repo_rule::RepoRuleUseId;
pub use version::Version;
pub use version::VersionParseError;
