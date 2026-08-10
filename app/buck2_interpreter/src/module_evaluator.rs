/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Isolated evaluation of the supported Bazel `MODULE.bazel` directives.
//!
//! This evaluator deliberately has no loader, filesystem, DICE, action, or
//! network context. Its globals contain standard Starlark helpers, isolated
//! `print`, the supported module/dependency/override directives, and explicit
//! fail-closed stubs for later override kinds. A successful call publishes one
//! immutable [`ModuleFile`] and its warnings.

use std::cell::RefCell;
use std::collections::BTreeMap;

use buck2_bzlmod::DependencyRepoName;
use buck2_bzlmod::DependencyRequest;
use buck2_bzlmod::LocalPathOverride;
use buck2_bzlmod::ModuleDeclaration;
use buck2_bzlmod::ModuleFile;
use buck2_bzlmod::ModuleKey;
use buck2_bzlmod::ModuleName;
use buck2_bzlmod::ModuleOverride;
use buck2_bzlmod::PatchLabel;
use buck2_bzlmod::SingleVersionOverride;
use buck2_bzlmod::Version;
use starlark::PrintHandler;
use starlark::any::ProvidesStaticType;
use starlark::environment::Globals;
use starlark::environment::GlobalsBuilder;
use starlark::environment::LibraryExtension;
use starlark::environment::Module;
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::values::UnpackValue;
use starlark::values::Value;
use starlark::values::list_or_tuple::UnpackListOrTuple;
use starlark::values::none::NoneOr;
use starlark::values::none::NoneType;
use starlark::values::tuple::UnpackTuple;

use crate::dialect::StarlarkDialect;
use crate::module_file::parse_module_file;

/// Context that affects Bazel's interpretation of a `MODULE.bazel` file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModuleFileEvalKind {
    /// The workspace root. Root development dependencies are retained unless
    /// the caller applies Bazel's `--ignore_dev_dependency` policy.
    Root { ignore_dev_dependencies: bool },
    /// A dependency module. Development dependencies are always ignored and
    /// the declaration is checked against the requested module key.
    Dependency { expected: ModuleKey },
}

impl ModuleFileEvalKind {
    fn ignores_dev_dependencies(&self) -> bool {
        match self {
            Self::Root {
                ignore_dev_dependencies,
            } => *ignore_dev_dependencies,
            Self::Dependency { .. } => true,
        }
    }

    fn is_root(&self) -> bool {
        matches!(self, Self::Root { .. })
    }
}

/// One warning produced while evaluating a root `MODULE.bazel` file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleFileWarning {
    location: Box<str>,
    message: Box<str>,
}

impl ModuleFileWarning {
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Successful, side-effect-free output of one `MODULE.bazel` evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModuleFileEvaluation {
    module_file: ModuleFile,
    warnings: Box<[ModuleFileWarning]>,
}

impl ModuleFileEvaluation {
    pub fn module_file(&self) -> &ModuleFile {
        &self.module_file
    }

    pub fn warnings(&self) -> &[ModuleFileWarning] {
        &self.warnings
    }

    pub fn into_parts(self) -> (ModuleFile, Box<[ModuleFileWarning]>) {
        (self.module_file, self.warnings)
    }
}

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
enum ModuleFileEvaluationError {
    #[error("the module() directive can only be called once")]
    RepeatedModule,
    #[error("if module() is called, it must be called before any other functions")]
    LateModule,
    #[error(
        "invalid module name '{0}': valid names must 1) only contain lowercase letters (a-z), digits (0-9), dots (.), hyphens (-), and underscores (_); 2) begin with a lowercase letter; 3) end with a lowercase letter or digit."
    )]
    InvalidModuleName(String),
    #[error("Invalid version in {directive}(): {message}")]
    InvalidVersion {
        directive: &'static str,
        message: String,
    },
    #[error(
        "invalid user-provided repo name '{0}': valid names may contain only A-Z, a-z, 0-9, '-', '_', '.', and must start with a letter or a number"
    )]
    InvalidRepoName(String),
    #[error(
        "invalid version argument '{0}': valid argument must 1) start with (<,<=,>,>=,-); 2) contain a version number in form of X.X.X where X is a number"
    )]
    InvalidBazelCompatibility(String),
    #[error(
        "The repo name '{name}' cannot be defined {incoming_how} at {incoming_location} as it is already defined {existing_how} at {existing_location}"
    )]
    DuplicateRepoName {
        name: String,
        incoming_how: &'static str,
        incoming_location: Box<str>,
        existing_how: &'static str,
        existing_location: Box<str>,
    },
    #[error("dependency MODULE.bazel evaluation requires a non-root expected module key")]
    RootDependencyKey,
    #[error("the MODULE.bazel file of {expected} declares a different name ({actual})")]
    DependencyNameMismatch { expected: ModuleKey, actual: String },
    #[error("the MODULE.bazel file of {expected} declares a different version ({actual})")]
    DependencyVersionMismatch {
        expected: ModuleKey,
        actual: Version,
    },
    #[error("internal error: MODULE.bazel builtin called without its isolated evaluation context")]
    MissingContext,
    #[error("{0}")]
    InvalidDependency(String),
    #[error("parameter '{parameter}' got value of type '{actual}', want 'int'")]
    CompatibilityLevelType {
        parameter: &'static str,
        actual: &'static str,
    },
    #[error("got {value} for {parameter}, want value in signed 32-bit range")]
    CompatibilityLevelOverflow {
        parameter: &'static str,
        value: String,
    },
    #[error("for bazel_compatibility, got {actual}, want sequence")]
    BazelCompatibilitySequenceType { actual: &'static str },
    #[error("at index {index} of bazel_compatibility, got element of type {actual}, want string")]
    BazelCompatibilityElementType { index: usize, actual: &'static str },
    #[error("for {parameter}, got {actual}, want sequence")]
    StringSequenceType {
        parameter: &'static str,
        actual: &'static str,
    },
    #[error("at index {index} of {parameter}, got element of type {actual}, want string")]
    StringSequenceElementType {
        parameter: &'static str,
        index: usize,
        actual: &'static str,
    },
    #[error("invalid label \"{label}\" in 'patches': {message}")]
    InvalidPatchLabel { label: String, message: String },
    #[error(
        "invalid label in 'patches': only patches in the main repository can be applied, not from '@{0}'"
    )]
    InvalidPatchRepository(String),
    #[error("multiple overrides for dep {0} found")]
    DuplicateOverride(String),
    #[error(
        "module '{module_name}' is overridden to use version '{override_version}', which is lower than the version '{requested_version}' requested by the root module"
    )]
    OverrideVersionTooLow {
        module_name: ModuleName,
        override_version: Version,
        requested_version: Version,
    },
    #[error("invalid override for the root module found: {0}")]
    RootSelfOverride(ModuleName),
    #[error("{0}() is not supported by the Buck2 Bazel MODULE dialect yet")]
    UnsupportedDirective(&'static str),
}

#[derive(Debug)]
struct RepoNameUsage {
    how: &'static str,
    location: Box<str>,
}

#[derive(Debug)]
struct WorkingModuleFile {
    declaration: Option<ModuleDeclaration>,
    dependencies: Vec<DependencyRequest>,
    overrides: Vec<ModuleOverride>,
    override_names: BTreeMap<ModuleName, ()>,
    repo_names: BTreeMap<Box<str>, RepoNameUsage>,
    warnings: Vec<ModuleFileWarning>,
    had_non_module_call: bool,
}

impl WorkingModuleFile {
    fn new() -> Self {
        Self {
            declaration: None,
            dependencies: Vec::new(),
            overrides: Vec::new(),
            override_names: BTreeMap::new(),
            repo_names: BTreeMap::new(),
            warnings: Vec::new(),
            had_non_module_call: false,
        }
    }

    fn reserve_repo_name(
        &mut self,
        name: &str,
        incoming_how: &'static str,
        incoming_location: &str,
    ) -> buck2_error::Result<()> {
        if let Some(existing) = self.repo_names.get(name) {
            return Err(ModuleFileEvaluationError::DuplicateRepoName {
                name: name.to_owned(),
                incoming_how,
                incoming_location: incoming_location.into(),
                existing_how: existing.how,
                existing_location: existing.location.clone(),
            }
            .into());
        }
        self.repo_names.insert(
            name.into(),
            RepoNameUsage {
                how: incoming_how,
                location: incoming_location.into(),
            },
        );
        Ok(())
    }

    fn warn(&mut self, location: &str, message: &'static str) {
        self.warnings.push(ModuleFileWarning {
            location: location.into(),
            message: message.into(),
        });
    }

    fn root_repo_alias(&self) -> &str {
        self.declaration
            .as_ref()
            .and_then(ModuleDeclaration::repo_name)
            .unwrap_or("")
    }

    fn add_override(&mut self, value: ModuleOverride) -> buck2_error::Result<()> {
        let name = value.module_name().clone();
        if self.override_names.insert(name.clone(), ()).is_some() {
            return Err(ModuleFileEvaluationError::DuplicateOverride(name.to_string()).into());
        }
        self.overrides.push(value);
        Ok(())
    }
}

#[derive(Debug, ProvidesStaticType)]
struct ModuleFileEvalState {
    kind: ModuleFileEvalKind,
    working: RefCell<WorkingModuleFile>,
}

impl ModuleFileEvalState {
    fn new(kind: ModuleFileEvalKind) -> buck2_error::Result<Self> {
        if matches!(
            &kind,
            ModuleFileEvalKind::Dependency { expected } if expected.is_root()
        ) {
            return Err(ModuleFileEvaluationError::RootDependencyKey.into());
        }
        Ok(Self {
            kind,
            working: RefCell::new(WorkingModuleFile::new()),
        })
    }

    fn declare_module(
        &self,
        name: &str,
        version: &str,
        repo_name: &str,
        compatibility_level: Option<Value<'_>>,
        bazel_compatibility: Option<Value<'_>>,
        location: &str,
    ) -> buck2_error::Result<()> {
        let mut working = self.working.borrow_mut();
        if working.declaration.is_some() {
            return Err(ModuleFileEvaluationError::RepeatedModule.into());
        }
        let compatibility_level =
            unpack_compatibility_level(compatibility_level, "compatibility_level")?;
        if compatibility_level != -1 && self.kind.is_root() {
            working.warn(
                location,
                "The attribute 'compatibility_level' in module() is a no-op and will be removed in a future Bazel release. Please remove it from your MODULE.bazel file.",
            );
        }
        if working.had_non_module_call {
            return Err(ModuleFileEvaluationError::LateModule.into());
        }

        let name = if name.is_empty() {
            None
        } else {
            Some(parse_module_name(name)?)
        };

        let (repo_name, repo_name_source) = match repo_name {
            "" => (
                name.as_ref()
                    .map(|name| Box::<str>::from(name.as_str()))
                    .unwrap_or_else(|| "".into()),
                "as the current module name",
            ),
            repo_name => {
                validate_repo_name(repo_name)?;
                (repo_name.into(), "as the module's own repo name")
            }
        };
        working.reserve_repo_name(&repo_name, repo_name_source, location)?;

        let version = parse_version("module", version)?;
        let bazel_compatibility = parse_bazel_compatibility(bazel_compatibility)?;

        let declaration = ModuleDeclaration::new(
            name,
            version,
            Some(repo_name),
            bazel_compatibility.into_boxed_slice(),
        );
        working.declaration = Some(declaration);
        Ok(())
    }

    fn add_dependency(
        &self,
        name: &str,
        version: &str,
        max_compatibility_level: Option<Value<'_>>,
        repo_name: Option<NoneOr<&str>>,
        dev_dependency: bool,
        location: &str,
    ) -> buck2_error::Result<()> {
        self.working.borrow_mut().had_non_module_call = true;
        let name = parse_module_name(name)?;
        let version = parse_version("bazel_dep", version)?;
        let max_compatibility_level =
            unpack_compatibility_level(max_compatibility_level, "max_compatibility_level")?;
        if max_compatibility_level != -1 && self.kind.is_root() {
            self.working.borrow_mut().warn(
                location,
                "The attribute 'max_compatibility_level' in bazel_dep() is a no-op and will be removed in a future Bazel release. Please remove it from your MODULE.bazel file.",
            );
        }
        let (repo_name, apparent_name) = match repo_name {
            None | Some(NoneOr::Other("")) => {
                let apparent_name: Box<str> = name.as_str().into();
                (
                    DependencyRepoName::Apparent(apparent_name.clone()),
                    Some(apparent_name),
                )
            }
            Some(NoneOr::Other(repo_name)) => {
                validate_repo_name(repo_name)?;
                let apparent_name: Box<str> = repo_name.into();
                (
                    DependencyRepoName::Apparent(apparent_name.clone()),
                    Some(apparent_name),
                )
            }
            Some(NoneOr::None) => (DependencyRepoName::Nodep, None),
        };
        let dependency =
            DependencyRequest::new(ModuleKey::new(name, version), repo_name, dev_dependency)
                .map_err(|error| ModuleFileEvaluationError::InvalidDependency(error.to_string()))?;

        let mut working = self.working.borrow_mut();
        if !(dev_dependency && self.kind.ignores_dev_dependencies()) {
            working.dependencies.push(dependency);
        }
        if let Some(apparent_name) = apparent_name {
            // Bazel reserves an ignored development dependency's apparent
            // name even though it omits the dependency edge.
            working.reserve_repo_name(&apparent_name, "by a bazel_dep", location)?;
        }
        Ok(())
    }

    fn add_single_version_override(
        &self,
        module_name: &str,
        version: &str,
        registry: &str,
        patches: Option<Value<'_>>,
        patch_cmds: Option<Value<'_>>,
        patch_strip: Option<Value<'_>>,
    ) -> buck2_error::Result<()> {
        self.working.borrow_mut().had_non_module_call = true;

        // Bazel validates the complete directive before deciding whether root
        // override declarations are ignored in this evaluation context.
        let module_name = parse_module_name(module_name)?;
        let version = parse_version("single_version_override", version)?;
        let patches = parse_string_sequence(patches, "patches")?;
        let root_repo_alias = self.working.borrow().root_repo_alias().to_owned();
        let patches = patches
            .into_iter()
            .map(|raw| parse_patch_label(&raw, &root_repo_alias))
            .collect::<buck2_error::Result<Vec<_>>>()?;
        let patch_cmds = parse_string_sequence(patch_cmds, "patch_cmds")?;
        let patch_strip = unpack_i32(patch_strip, "patch_strip", 0)?;

        if self.kind.ignores_dev_dependencies() {
            return Ok(());
        }
        self.working
            .borrow_mut()
            .add_override(ModuleOverride::SingleVersion(SingleVersionOverride::new(
                module_name,
                version,
                registry.into(),
                patches.into_boxed_slice(),
                patch_cmds.into_boxed_slice(),
                patch_strip,
            )))
    }

    fn add_local_path_override(&self, module_name: &str, path: &str) -> buck2_error::Result<()> {
        self.working.borrow_mut().had_non_module_call = true;
        let module_name = parse_module_name(module_name)?;
        if self.kind.ignores_dev_dependencies() {
            return Ok(());
        }
        self.working
            .borrow_mut()
            .add_override(ModuleOverride::LocalPath(LocalPathOverride::new(
                module_name,
                path.into(),
            )))
    }

    fn finish(self) -> buck2_error::Result<ModuleFileEvaluation> {
        let working = self.working.into_inner();
        let declaration = working.declaration.unwrap_or_else(|| {
            ModuleDeclaration::new(None, Version::EMPTY, Some("".into()), Box::new([]))
        });
        if let ModuleFileEvalKind::Dependency { expected } = &self.kind {
            let actual_name = declaration.name().map(ModuleName::as_str).unwrap_or("");
            let expected_name = expected
                .name()
                .expect("dependency key was checked as non-root above");
            if actual_name != expected_name.as_str() {
                return Err(ModuleFileEvaluationError::DependencyNameMismatch {
                    expected: expected.clone(),
                    actual: actual_name.to_owned(),
                }
                .into());
            }
            if !expected.is_non_registry() && declaration.version() != expected.version() {
                return Err(ModuleFileEvaluationError::DependencyVersionMismatch {
                    expected: expected.clone(),
                    actual: declaration.version().clone(),
                }
                .into());
            }
        }
        if self.kind.is_root() {
            for override_value in &working.overrides {
                let Some(single) = override_value.as_single_version() else {
                    continue;
                };
                let Some(requested) = working.dependencies.iter().find(|dependency| {
                    matches!(
                        dependency.repo_name(),
                        DependencyRepoName::Apparent(alias)
                            if alias.as_ref() == single.module_name().as_str()
                    )
                }) else {
                    continue;
                };
                if !single.version().is_empty()
                    && !requested.module().version().is_empty()
                    && single.version() < requested.module().version()
                {
                    return Err(ModuleFileEvaluationError::OverrideVersionTooLow {
                        module_name: single.module_name().clone(),
                        override_version: single.version().clone(),
                        requested_version: requested.module().version().clone(),
                    }
                    .into());
                }
            }
            if let Some(root_name) = declaration.name()
                && working.override_names.contains_key(root_name)
            {
                return Err(ModuleFileEvaluationError::RootSelfOverride(root_name.clone()).into());
            }
        }
        Ok(ModuleFileEvaluation {
            module_file: ModuleFile::new(
                Some(declaration),
                working.dependencies.into_boxed_slice(),
                working.overrides.into_boxed_slice(),
            ),
            warnings: working.warnings.into_boxed_slice(),
        })
    }
}

fn state<'a>(eval: &'a Evaluator<'_, '_, '_>) -> buck2_error::Result<&'a ModuleFileEvalState> {
    eval.extra
        .and_then(|extra| extra.downcast_ref::<ModuleFileEvalState>())
        .ok_or_else(|| ModuleFileEvaluationError::MissingContext.into())
}

fn parse_module_name(value: &str) -> buck2_error::Result<ModuleName> {
    ModuleName::parse(value)
        .map_err(|_| ModuleFileEvaluationError::InvalidModuleName(value.to_owned()).into())
}

fn parse_version(directive: &'static str, value: &str) -> buck2_error::Result<Version> {
    Version::parse(value).map_err(|error| {
        ModuleFileEvaluationError::InvalidVersion {
            directive,
            message: error.to_string(),
        }
        .into()
    })
}

fn validate_repo_name(value: &str) -> buck2_error::Result<()> {
    let mut bytes = value.bytes();
    let valid = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(ModuleFileEvaluationError::InvalidRepoName(value.to_owned()).into())
    }
}

fn parse_bazel_compatibility(value: Option<Value<'_>>) -> buck2_error::Result<Vec<Box<str>>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let value = UnpackListOrTuple::<Value>::unpack_value(value)?.ok_or(
        ModuleFileEvaluationError::BazelCompatibilitySequenceType {
            actual: value.get_type(),
        },
    )?;
    let mut result = Vec::new();
    for (index, item) in value.items.into_iter().enumerate() {
        let item = String::unpack_value(item)?.ok_or(
            ModuleFileEvaluationError::BazelCompatibilityElementType {
                index,
                actual: item.get_type(),
            },
        )?;
        if !valid_bazel_compatibility(&item) {
            return Err(ModuleFileEvaluationError::InvalidBazelCompatibility(item).into());
        }
        result.push(item.into());
    }
    Ok(result)
}

fn parse_string_sequence(
    value: Option<Value<'_>>,
    parameter: &'static str,
) -> buck2_error::Result<Vec<Box<str>>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let value = UnpackListOrTuple::<Value>::unpack_value(value)?.ok_or(
        ModuleFileEvaluationError::StringSequenceType {
            parameter,
            actual: value.get_type(),
        },
    )?;
    value
        .items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            String::unpack_value(item)?
                .map(Box::<str>::from)
                .ok_or_else(|| {
                    ModuleFileEvaluationError::StringSequenceElementType {
                        parameter,
                        index,
                        actual: item.get_type(),
                    }
                    .into()
                })
        })
        .collect()
}

fn parse_patch_label(raw: &str, root_repo_alias: &str) -> buck2_error::Result<PatchLabel> {
    let (canonical_repo, package_start, package_is_absolute) = if raw.starts_with('@') {
        let canonical = raw.starts_with("@@");
        let repo_start = if canonical { 2 } else { 1 };
        if let Some(double_slash) = raw.find("//") {
            let repo = &raw[repo_start..double_slash];
            validate_canonical_repo_name(repo, raw)?;
            if !canonical && !repo.is_empty() && repo != root_repo_alias {
                return Err(
                    ModuleFileEvaluationError::InvalidPatchRepository(repo.to_owned()).into(),
                );
            }
            (
                if canonical && !repo.is_empty() {
                    Some(repo)
                } else {
                    None
                },
                double_slash + 2,
                true,
            )
        } else {
            let repo = &raw[repo_start..];
            validate_canonical_repo_name(repo, raw)?;
            if !canonical && !repo.is_empty() && repo != root_repo_alias {
                return Err(
                    ModuleFileEvaluationError::InvalidPatchRepository(repo.to_owned()).into(),
                );
            }
            (
                if canonical && !repo.is_empty() {
                    Some(repo)
                } else {
                    None
                },
                raw.len(),
                true,
            )
        }
    } else if raw.starts_with("//") {
        (None, 2, true)
    } else {
        (None, 0, false)
    };

    let remainder = &raw[package_start..];
    let (package, target) = if package_start == raw.len() && raw.starts_with('@') {
        let repo_start = if raw.starts_with("@@") { 2 } else { 1 };
        ("", &raw[repo_start..])
    } else if let Some(colon) = remainder.find(':') {
        (&remainder[..colon], &remainder[colon + 1..])
    } else if !package_is_absolute {
        ("", remainder)
    } else {
        (remainder, remainder.rsplit('/').next().unwrap_or(""))
    };
    if !package_is_absolute && !package.is_empty() {
        return Err(ModuleFileEvaluationError::InvalidPatchLabel {
            label: raw.to_owned(),
            message: "package-relative labels must use an absolute package".to_owned(),
        }
        .into());
    }
    validate_label_parts(raw, package, target)?;
    let target = target.strip_suffix("/.").unwrap_or(target);

    let repository = canonical_repo
        .map(|repo| format!("@@{repo}"))
        .unwrap_or_default();
    let normalized = format!("{repository}//{package}:{target}");
    PatchLabel::parse_normalized(&normalized).map_err(|error| {
        ModuleFileEvaluationError::InvalidPatchLabel {
            label: raw.to_owned(),
            message: error.to_string(),
        }
        .into()
    })
}

fn validate_canonical_repo_name(repo: &str, raw: &str) -> buck2_error::Result<()> {
    if !matches!(repo, "." | "..")
        && repo
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+'))
    {
        Ok(())
    } else {
        Err(ModuleFileEvaluationError::InvalidPatchLabel {
            label: raw.to_owned(),
            message: "invalid canonical repository name".to_owned(),
        }
        .into())
    }
}

fn validate_label_parts(raw: &str, package: &str, target: &str) -> buck2_error::Result<()> {
    let valid_printable = |character: char, allow_non_ascii: bool| {
        (character.is_ascii() && !character.is_ascii_control() && !matches!(character, ':' | '\\'))
            || (allow_non_ascii && !character.is_ascii())
    };
    let valid_package = package.is_empty()
        || (!package.starts_with('/')
            && !package.ends_with('/')
            && !package.contains("//")
            && package
                .chars()
                .all(|character| valid_printable(character, false))
            && package
                .split('/')
                .all(|segment| segment.chars().any(|character| character != '.')));
    let invalid_target_path = target.starts_with('/')
        || target.ends_with('/')
        || target == ".."
        || target.starts_with("../")
        || target.ends_with("/..")
        || target.contains("/../")
        || target.starts_with("./")
        || target.contains("/./")
        || target.contains("//");
    let valid_target = !target.is_empty()
        && !invalid_target_path
        && target
            .chars()
            .all(|character| valid_printable(character, true));
    if !valid_package || !valid_target {
        return Err(ModuleFileEvaluationError::InvalidPatchLabel {
            label: raw.to_owned(),
            message: "invalid package or target name".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn unpack_compatibility_level(
    value: Option<Value<'_>>,
    parameter: &'static str,
) -> buck2_error::Result<i32> {
    unpack_i32(value, parameter, -1)
}

fn unpack_i32(
    value: Option<Value<'_>>,
    parameter: &'static str,
    default: i32,
) -> buck2_error::Result<i32> {
    let Some(value) = value else {
        return Ok(default);
    };
    if value.get_type() != "int" {
        return Err(ModuleFileEvaluationError::CompatibilityLevelType {
            parameter,
            actual: value.get_type(),
        }
        .into());
    }
    value.unpack_i32().ok_or_else(|| {
        ModuleFileEvaluationError::CompatibilityLevelOverflow {
            parameter,
            value: value.to_repr(),
        }
        .into()
    })
}

fn call_site(eval: &Evaluator<'_, '_, '_>) -> Box<str> {
    eval.call_stack_top_location()
        .map(|location| location.to_string().into())
        .unwrap_or_else(|| "<builtin>".into())
}

fn valid_bazel_compatibility(value: &str) -> bool {
    let version = value
        .strip_prefix("<=")
        .or_else(|| value.strip_prefix(">="))
        .or_else(|| value.strip_prefix('<'))
        .or_else(|| value.strip_prefix('>'))
        .or_else(|| value.strip_prefix('-'));
    let Some(version) = version else {
        return false;
    };
    let mut parts = version.split('.');
    let valid_part =
        |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None)
            if valid_part(a) && valid_part(b) && valid_part(c)
    )
}

#[starlark_module]
fn register_module_file_globals(builder: &mut GlobalsBuilder) {
    fn module<'v>(
        #[starlark(require = named, default = "")] name: &str,
        #[starlark(require = named, default = "")] version: &str,
        #[starlark(require = named)] compatibility_level: Option<Value<'v>>,
        #[starlark(require = named, default = "")] repo_name: &str,
        #[starlark(require = named)] bazel_compatibility: Option<Value<'v>>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<NoneType> {
        let location = call_site(eval);
        state(eval)?.declare_module(
            name,
            version,
            repo_name,
            compatibility_level,
            bazel_compatibility,
            &location,
        )?;
        Ok(NoneType)
    }

    fn bazel_dep<'v>(
        #[starlark(require = named)] name: &str,
        #[starlark(require = named, default = "")] version: &str,
        #[starlark(require = named)] max_compatibility_level: Option<Value<'v>>,
        #[starlark(require = named)] repo_name: Option<NoneOr<&str>>,
        #[starlark(require = named, default = false)] dev_dependency: bool,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<NoneType> {
        let location = call_site(eval);
        state(eval)?.add_dependency(
            name,
            version,
            max_compatibility_level,
            repo_name,
            dev_dependency,
            &location,
        )?;
        Ok(NoneType)
    }

    fn single_version_override<'v>(
        #[starlark(require = named)] module_name: &str,
        #[starlark(require = named, default = "")] version: &str,
        #[starlark(require = named, default = "")] registry: &str,
        #[starlark(require = named)] patches: Option<Value<'v>>,
        #[starlark(require = named)] patch_cmds: Option<Value<'v>>,
        #[starlark(require = named)] patch_strip: Option<Value<'v>>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<NoneType> {
        state(eval)?.add_single_version_override(
            module_name,
            version,
            registry,
            patches,
            patch_cmds,
            patch_strip,
        )?;
        Ok(NoneType)
    }

    fn local_path_override<'v>(
        #[starlark(require = named)] module_name: &str,
        #[starlark(require = named)] path: &str,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> starlark::Result<NoneType> {
        state(eval)?.add_local_path_override(module_name, path)?;
        Ok(NoneType)
    }

    fn multiple_version_override<'v>(
        #[starlark(args)] _args: UnpackTuple<Value<'v>>,
        #[starlark(kwargs)] _kwargs: Value<'v>,
    ) -> starlark::Result<NoneType> {
        let error: buck2_error::Error =
            ModuleFileEvaluationError::UnsupportedDirective("multiple_version_override").into();
        Err(error.into())
    }

    fn archive_override<'v>(
        #[starlark(args)] _args: UnpackTuple<Value<'v>>,
        #[starlark(kwargs)] _kwargs: Value<'v>,
    ) -> starlark::Result<NoneType> {
        let error: buck2_error::Error =
            ModuleFileEvaluationError::UnsupportedDirective("archive_override").into();
        Err(error.into())
    }

    fn git_override<'v>(
        #[starlark(args)] _args: UnpackTuple<Value<'v>>,
        #[starlark(kwargs)] _kwargs: Value<'v>,
    ) -> starlark::Result<NoneType> {
        let error: buck2_error::Error =
            ModuleFileEvaluationError::UnsupportedDirective("git_override").into();
        Err(error.into())
    }
}

fn module_file_globals() -> Globals {
    GlobalsBuilder::extended_by(&[LibraryExtension::Print])
        .with(register_module_file_globals)
        .build()
}

/// The exact isolated global surface, exposed for integration audit tests.
#[doc(hidden)]
pub fn module_file_global_names_for_audit() -> Vec<String> {
    module_file_globals()
        .names()
        .map(|name| name.as_str().to_owned())
        .collect()
}

struct NoopModulePrintHandler;

impl PrintHandler for NoopModulePrintHandler {
    fn println(&self, _text: &str) -> starlark::Result<()> {
        Ok(())
    }
}

static NOOP_MODULE_PRINT_HANDLER: NoopModulePrintHandler = NoopModulePrintHandler;

/// Parse, validate, and evaluate one in-memory Bazel `MODULE.bazel` source.
///
/// The evaluator has no loader and adds isolated `print`, `module`, and
/// `bazel_dep`, `single_version_override`, and `local_path_override` to the
/// standard Starlark globals. The accumulator is private to this call and is
/// converted to [`ModuleFile`] only after evaluation and contextual checks
/// both succeed. Print output is discarded rather than emitted to an ambient
/// process stream, and root warnings are returned with the module file rather
/// than emitted ambiently.
pub fn evaluate_module_file(
    filename: &str,
    content: String,
    kind: ModuleFileEvalKind,
) -> buck2_error::Result<ModuleFileEvaluation> {
    let ast = parse_module_file(filename, content, StarlarkDialect::Bazel)?;
    let state = ModuleFileEvalState::new(kind)?;
    let globals = module_file_globals();
    // patternlint-disable-next-line buck2-no-starlark-module: MODULE.bazel evaluation is isolated, in-memory, and deliberately small.
    Module::with_temp_heap(|module| {
        let mut eval = Evaluator::new(&module);
        eval.extra = Some(&state);
        eval.set_print_handler(&NOOP_MODULE_PRINT_HANDLER);
        eval.eval_module(ast, &globals)?;
        starlark::Result::Ok(())
    })?;
    state.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globals_match_the_isolated_surface() {
        let names = module_file_global_names_for_audit();
        assert_eq!(
            names,
            [
                "False",
                "None",
                "True",
                "abs",
                "all",
                "any",
                "archive_override",
                "bazel_dep",
                "bool",
                "bytes",
                "chr",
                "dict",
                "dir",
                "enumerate",
                "fail",
                "float",
                "getattr",
                "git_override",
                "hasattr",
                "hash",
                "int",
                "len",
                "list",
                "local_path_override",
                "max",
                "min",
                "module",
                "multiple_version_override",
                "ord",
                "print",
                "range",
                "repr",
                "reversed",
                "single_version_override",
                "sorted",
                "str",
                "tuple",
                "type",
                "zip",
            ]
        );
    }
}
