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

/// A validated top-level Starlark binding name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StarlarkBindingName(Box<str>);

impl StarlarkBindingName {
    pub fn parse(value: &str) -> Result<Self, StarlarkBindingNameParseError> {
        if !valid_identifier_spelling(value) || is_starlark_keyword(value) {
            return Err(StarlarkBindingNameParseError(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn valid_identifier_spelling(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn is_starlark_keyword(value: &str) -> bool {
    matches!(
        value,
        "False"
            | "None"
            | "True"
            | "and"
            | "as"
            | "assert"
            | "async"
            | "await"
            | "break"
            | "class"
            | "continue"
            | "def"
            | "del"
            | "elif"
            | "else"
            | "except"
            | "finally"
            | "for"
            | "from"
            | "global"
            | "if"
            | "import"
            | "in"
            | "is"
            | "lambda"
            | "load"
            | "nonlocal"
            | "not"
            | "or"
            | "pass"
            | "raise"
            | "return"
            | "try"
            | "while"
            | "with"
            | "yield"
    )
}

impl fmt::Display for StarlarkBindingName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarlarkBindingNameParseError(Box<str>);

impl fmt::Display for StarlarkBindingNameParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid Starlark binding name '{}'", self.0)
    }
}

impl Error for StarlarkBindingNameParseError {}

/// A lexical identifier spelling supplied as an extension-name string.
///
/// Keyword spellings are accepted because this value is looked up by string;
/// it is not parsed as a source binding.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExtensionName(Box<str>);

impl ExtensionName {
    pub fn parse(value: &str) -> Result<Self, ExtensionNameParseError> {
        if !valid_identifier_spelling(value) {
            return Err(ExtensionNameParseError(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExtensionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionNameParseError(Box<str>);

impl fmt::Display for ExtensionNameParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid extension name '{}'", self.0)
    }
}

impl Error for ExtensionNameParseError {}

/// A validated apparent repository name used by `use_repo`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepositoryName(Box<str>);

impl RepositoryName {
    pub fn parse(value: &str) -> Result<Self, RepositoryNameParseError> {
        let mut bytes = value.bytes();
        let valid = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid {
            return Err(RepositoryNameParseError(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepositoryName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryNameParseError(Box<str>);

impl fmt::Display for RepositoryNameParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid apparent repository name '{}'", self.0)
    }
}

impl Error for RepositoryNameParseError {}

/// A normalized main- or apparent-repository label.
///
/// Canonical repository labels are intentionally excluded: repository mapping
/// has not run when the pure MODULE frontend creates this value. Whether the
/// target is a loadable `.bzl` file is checked later by extension loading.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApparentLabel(Box<str>);

impl ApparentLabel {
    pub fn parse_normalized(value: &str) -> Result<Self, ApparentLabelParseError> {
        let remainder = if let Some(remainder) = value.strip_prefix("//") {
            remainder
        } else if let Some(remainder) = value.strip_prefix('@') {
            if remainder.starts_with('@') {
                return Err(ApparentLabelParseError(value.into()));
            }
            let (repository, remainder) = remainder
                .split_once("//")
                .ok_or_else(|| ApparentLabelParseError(value.into()))?;
            RepositoryName::parse(repository).map_err(|_| ApparentLabelParseError(value.into()))?;
            remainder
        } else {
            return Err(ApparentLabelParseError(value.into()));
        };

        let (package, target) = remainder
            .split_once(':')
            .ok_or_else(|| ApparentLabelParseError(value.into()))?;
        if !valid_normalized_package(package) || !valid_normalized_target(target) {
            return Err(ApparentLabelParseError(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApparentLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApparentLabelParseError(Box<str>);

impl fmt::Display for ApparentLabelParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid normalized apparent label '{}'", self.0)
    }
}

impl Error for ApparentLabelParseError {}

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

/// A canonical arbitrary-precision Starlark integer spelling.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RawInteger(Box<str>);

impl RawInteger {
    pub fn parse_decimal(value: &str) -> Result<Self, RawIntegerParseError> {
        let digits = value.strip_prefix('-').unwrap_or(value);
        let valid = !digits.is_empty()
            && digits.bytes().all(|byte| byte.is_ascii_digit())
            && (digits == "0" || !digits.starts_with('0'))
            && !(value.starts_with('-') && digits == "0");
        if !valid {
            return Err(RawIntegerParseError(value.into()));
        }
        Ok(Self(value.into()))
    }

    pub fn as_decimal(&self) -> &str {
        &self.0
    }
}

impl From<i64> for RawInteger {
    fn from(value: i64) -> Self {
        Self(value.to_string().into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIntegerParseError(Box<str>);

impl fmt::Display for RawIntegerParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid canonical decimal integer '{}'", self.0)
    }
}

impl Error for RawIntegerParseError {}

/// An evaluator-independent value retained until an extension tag schema is
/// available.
///
/// Lists and tuples intentionally share the `Sequence` representation because
/// Bazel attribute schemas consume both as sequences. Unsupported Starlark
/// values must fail before constructing this type rather than being frozen or
/// stringified. Dictionary insertion order is retained by the pair slice.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum RawAttributeValue {
    String(Box<str>),
    Bool(bool),
    Integer(RawInteger),
    Sequence(Box<[RawAttributeValue]>),
    Dict(OrderedStringDict),
}

/// A source-ordered Starlark dictionary with unique string keys.
///
/// The initial extension frontend intentionally rejects non-string dictionary
/// keys. This covers the pinned Bazel module shape without allowing impossible
/// duplicate or unhashable keys into semantic identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OrderedStringDict(Box<[(Box<str>, RawAttributeValue)]>);

impl OrderedStringDict {
    pub fn new(
        entries: Box<[(Box<str>, RawAttributeValue)]>,
    ) -> Result<Self, OrderedStringDictError> {
        for (index, (key, _)) in entries.iter().enumerate() {
            if entries[..index].iter().any(|(existing, _)| existing == key) {
                return Err(OrderedStringDictError::DuplicateKey(key.clone()));
            }
        }
        Ok(Self(entries))
    }

    pub fn entries(&self) -> &[(Box<str>, RawAttributeValue)] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderedStringDictError {
    DuplicateKey(Box<str>),
}

impl fmt::Display for OrderedStringDictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey(key) => write!(f, "duplicate raw dictionary key '{key}'"),
        }
    }
}

impl Error for OrderedStringDictError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RawAttribute {
    name: Box<str>,
    value: RawAttributeValue,
}

impl RawAttribute {
    pub fn new(name: impl Into<Box<str>>, value: RawAttributeValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &RawAttributeValue {
        &self.value
    }
}

/// Diagnostic source text kept outside semantic extension projections.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ModuleSourceLocation(Box<str>);

impl ModuleSourceLocation {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One regular module-extension tag call in source order.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RawTag {
    ordinal: u32,
    class_name: Box<str>,
    attributes: Box<[RawAttribute]>,
    dev_dependency: bool,
    location: ModuleSourceLocation,
}

impl RawTag {
    pub fn new(
        ordinal: u32,
        class_name: impl Into<Box<str>>,
        attributes: Box<[RawAttribute]>,
        dev_dependency: bool,
        location: ModuleSourceLocation,
    ) -> Result<Self, RawTagError> {
        for (index, attribute) in attributes.iter().enumerate() {
            if attributes[..index]
                .iter()
                .any(|existing| existing.name == attribute.name)
            {
                return Err(RawTagError::DuplicateAttribute(attribute.name.clone()));
            }
        }
        Ok(Self {
            ordinal,
            class_name: class_name.into(),
            attributes,
            dev_dependency,
            location,
        })
    }

    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    pub fn attributes(&self) -> &[RawAttribute] {
        &self.attributes
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }

    pub fn location(&self) -> &ModuleSourceLocation {
        &self.location
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawTagError {
    DuplicateAttribute(Box<str>),
}

impl fmt::Display for RawTagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateAttribute(name) => write!(f, "duplicate raw tag attribute '{name}'"),
        }
    }
}

impl Error for RawTagError {}

/// One local-to-exported repository mapping requested through `use_repo`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RepoImport {
    ordinal: u32,
    local_name: RepositoryName,
    exported_name: RepositoryName,
    location: ModuleSourceLocation,
}

impl RepoImport {
    pub fn new(
        ordinal: u32,
        local_name: RepositoryName,
        exported_name: RepositoryName,
        location: ModuleSourceLocation,
    ) -> Self {
        Self {
            ordinal,
            local_name,
            exported_name,
            location,
        }
    }

    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn local_name(&self) -> &RepositoryName {
        &self.local_name
    }

    pub fn exported_name(&self) -> &RepositoryName {
        &self.exported_name
    }

    pub fn location(&self) -> &ModuleSourceLocation {
        &self.location
    }
}

/// One proxy returned by `use_extension`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProxyUse {
    ordinal: u32,
    exported_name: Option<StarlarkBindingName>,
    dev_dependency: bool,
    location: ModuleSourceLocation,
    imports: Box<[RepoImport]>,
}

impl ProxyUse {
    pub fn new(
        ordinal: u32,
        exported_name: Option<StarlarkBindingName>,
        dev_dependency: bool,
        location: ModuleSourceLocation,
        imports: Box<[RepoImport]>,
    ) -> Result<Self, ProxyUseError> {
        for import in imports.iter() {
            if import.ordinal <= ordinal {
                return Err(ProxyUseError::ImportNotAfterProxy {
                    proxy: ordinal,
                    import: import.ordinal,
                });
            }
        }
        for imports in imports.windows(2) {
            if imports[0].ordinal >= imports[1].ordinal {
                return Err(ProxyUseError::ImportsNotSourceOrdered {
                    previous: imports[0].ordinal,
                    next: imports[1].ordinal,
                });
            }
        }
        Ok(Self {
            ordinal,
            exported_name,
            dev_dependency,
            location,
            imports,
        })
    }

    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn exported_name(&self) -> Option<&StarlarkBindingName> {
        self.exported_name.as_ref()
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }

    pub fn location(&self) -> &ModuleSourceLocation {
        &self.location
    }

    pub fn imports(&self) -> &[RepoImport] {
        &self.imports
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProxyUseError {
    ImportNotAfterProxy { proxy: u32, import: u32 },
    ImportsNotSourceOrdered { previous: u32, next: u32 },
}

impl fmt::Display for ProxyUseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ImportNotAfterProxy { proxy, import } => write!(
                f,
                "repository import ordinal {import} must follow proxy ordinal {proxy}"
            ),
            Self::ImportsNotSourceOrdered { previous, next } => write!(
                f,
                "repository import ordinals must be source ordered, got {previous} then {next}"
            ),
        }
    }
}

impl Error for ProxyUseError {}

/// The source of an extension proxy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExtensionUseKind {
    Regular {
        extension_file: ApparentLabel,
        extension_name: ExtensionName,
    },
}

/// Whether an extension's evaluation identity is shared or proxy-isolated.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExtensionIsolation {
    None,
    Isolated {
        owner: ModuleKey,
        exported_proxy_name: StarlarkBindingName,
    },
}

/// All source-ordered proxies associated with one extension identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtensionUse {
    first_use_ordinal: u32,
    kind: ExtensionUseKind,
    isolation: ExtensionIsolation,
    proxies: Box<[ProxyUse]>,
    tags: Box<[RawTag]>,
}

impl ExtensionUse {
    pub fn new(
        first_use_ordinal: u32,
        kind: ExtensionUseKind,
        isolation: ExtensionIsolation,
        proxies: Box<[ProxyUse]>,
        tags: Box<[RawTag]>,
    ) -> Result<Self, ExtensionUseError> {
        let Some(first_proxy) = proxies.first() else {
            return Err(ExtensionUseError::NoProxies);
        };
        if first_use_ordinal >= first_proxy.ordinal {
            return Err(ExtensionUseError::FirstProxyNotAfterFirstUse {
                first_use: first_use_ordinal,
                first_proxy: first_proxy.ordinal,
            });
        }
        for proxies in proxies.windows(2) {
            if proxies[0].ordinal >= proxies[1].ordinal {
                return Err(ExtensionUseError::ProxiesNotSourceOrdered {
                    previous: proxies[0].ordinal,
                    next: proxies[1].ordinal,
                });
            }
        }
        for tags in tags.windows(2) {
            if tags[0].ordinal >= tags[1].ordinal {
                return Err(ExtensionUseError::TagsNotSourceOrdered {
                    previous: tags[0].ordinal,
                    next: tags[1].ordinal,
                });
            }
        }

        let mut event_ordinals = BTreeSet::new();
        let mut local_imports = BTreeSet::new();
        let mut exported_imports = BTreeSet::new();
        for proxy in proxies.iter() {
            if !event_ordinals.insert(proxy.ordinal) {
                return Err(ExtensionUseError::DuplicateEventOrdinal(proxy.ordinal));
            }
            for import in proxy.imports.iter() {
                if !event_ordinals.insert(import.ordinal) {
                    return Err(ExtensionUseError::DuplicateEventOrdinal(import.ordinal));
                }
                if !local_imports.insert(import.local_name.clone()) {
                    return Err(ExtensionUseError::DuplicateLocalImport(
                        import.local_name.clone(),
                    ));
                }
                if !exported_imports.insert(import.exported_name.clone()) {
                    return Err(ExtensionUseError::DuplicateExportedImport(
                        import.exported_name.clone(),
                    ));
                }
            }
        }
        for tag in tags.iter() {
            if tag.ordinal <= first_use_ordinal {
                return Err(ExtensionUseError::TagNotAfterFirstUse {
                    first_use: first_use_ordinal,
                    tag: tag.ordinal,
                });
            }
            if !event_ordinals.insert(tag.ordinal) {
                return Err(ExtensionUseError::DuplicateEventOrdinal(tag.ordinal));
            }
            if !proxies.iter().any(|proxy| {
                proxy.ordinal < tag.ordinal && proxy.dev_dependency == tag.dev_dependency
            }) {
                return Err(ExtensionUseError::TagWithoutEligibleProxy {
                    tag: tag.ordinal,
                    dev_dependency: tag.dev_dependency,
                });
            }
        }

        if let ExtensionIsolation::Isolated {
            owner,
            exported_proxy_name,
        } = &isolation
        {
            let [proxy] = proxies.as_ref() else {
                return Err(ExtensionUseError::IsolatedProxyCount(proxies.len()));
            };
            let Some(proxy_name) = proxy.exported_name() else {
                return Err(ExtensionUseError::UnnamedIsolatedProxy);
            };
            if proxy_name != exported_proxy_name {
                return Err(ExtensionUseError::IsolatedProxyNameMismatch {
                    identity: exported_proxy_name.clone(),
                    proxy: proxy_name.clone(),
                });
            }
            if owner.is_root() && proxy.imports.is_empty() {
                return Err(ExtensionUseError::RootIsolationWithoutImports);
            }
        }
        Ok(Self {
            first_use_ordinal,
            kind,
            isolation,
            proxies,
            tags,
        })
    }

    pub fn first_use_ordinal(&self) -> u32 {
        self.first_use_ordinal
    }

    pub fn kind(&self) -> &ExtensionUseKind {
        &self.kind
    }

    pub fn isolation(&self) -> &ExtensionIsolation {
        &self.isolation
    }

    pub fn proxies(&self) -> &[ProxyUse] {
        &self.proxies
    }

    pub fn tags(&self) -> &[RawTag] {
        &self.tags
    }

    /// Returns the semantic input for extension evaluation.
    ///
    /// Diagnostics, absolute ordinals, and repository imports are omitted so
    /// edits to them cannot invalidate extension evaluation. The flat tag
    /// slice still preserves tag order across all proxies.
    pub fn evaluation_projection(&self) -> ExtensionEvaluationProjection {
        ExtensionEvaluationProjection {
            kind: self.kind.clone(),
            isolation: self.isolation.clone(),
            tags: self
                .tags
                .iter()
                .map(|tag| TagEvaluationProjection {
                    class_name: tag.class_name.clone(),
                    attributes: tag.attributes.clone(),
                    dev_dependency: tag.dev_dependency,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Returns only the input needed to project generated repositories into a
    /// module's apparent repository mapping.
    pub fn repository_mapping_projection(&self) -> ExtensionRepositoryMappingProjection {
        let mut regular_imports = Vec::new();
        let mut dev_imports = Vec::new();
        for proxy in self.proxies.iter() {
            let destination = if proxy.dev_dependency {
                &mut dev_imports
            } else {
                &mut regular_imports
            };
            destination.extend(proxy.imports.iter().map(|import| RepoImportProjection {
                local_name: import.local_name.clone(),
                exported_name: import.exported_name.clone(),
            }));
        }
        regular_imports.sort();
        dev_imports.sort();
        ExtensionRepositoryMappingProjection {
            kind: self.kind.clone(),
            isolation: self.isolation.clone(),
            regular_imports: regular_imports.into_boxed_slice(),
            dev_imports: dev_imports.into_boxed_slice(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtensionUseError {
    NoProxies,
    FirstProxyNotAfterFirstUse {
        first_use: u32,
        first_proxy: u32,
    },
    ProxiesNotSourceOrdered {
        previous: u32,
        next: u32,
    },
    TagsNotSourceOrdered {
        previous: u32,
        next: u32,
    },
    TagNotAfterFirstUse {
        first_use: u32,
        tag: u32,
    },
    DuplicateEventOrdinal(u32),
    TagWithoutEligibleProxy {
        tag: u32,
        dev_dependency: bool,
    },
    DuplicateLocalImport(RepositoryName),
    DuplicateExportedImport(RepositoryName),
    IsolatedProxyCount(usize),
    UnnamedIsolatedProxy,
    IsolatedProxyNameMismatch {
        identity: StarlarkBindingName,
        proxy: StarlarkBindingName,
    },
    RootIsolationWithoutImports,
}

impl fmt::Display for ExtensionUseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoProxies => f.write_str("an extension use requires at least one proxy"),
            Self::FirstProxyNotAfterFirstUse {
                first_use,
                first_proxy,
            } => write!(
                f,
                "first proxy ordinal {first_proxy} must follow first-use ordinal {first_use}"
            ),
            Self::ProxiesNotSourceOrdered { previous, next } => write!(
                f,
                "proxy ordinals must be source ordered, got {previous} then {next}"
            ),
            Self::TagsNotSourceOrdered { previous, next } => write!(
                f,
                "tag ordinals must be source ordered, got {previous} then {next}"
            ),
            Self::TagNotAfterFirstUse { first_use, tag } => write!(
                f,
                "tag ordinal {tag} must follow first-use ordinal {first_use}"
            ),
            Self::DuplicateEventOrdinal(ordinal) => {
                write!(f, "duplicate extension event ordinal {ordinal}")
            }
            Self::TagWithoutEligibleProxy {
                tag,
                dev_dependency,
            } => write!(
                f,
                "tag ordinal {tag} has no earlier proxy with dev_dependency={dev_dependency}"
            ),
            Self::DuplicateLocalImport(name) => {
                write!(f, "duplicate local repository import name '{name}'")
            }
            Self::DuplicateExportedImport(name) => {
                write!(f, "duplicate exported repository import name '{name}'")
            }
            Self::IsolatedProxyCount(count) => {
                write!(
                    f,
                    "an isolated extension requires exactly one proxy, got {count}"
                )
            }
            Self::UnnamedIsolatedProxy => {
                f.write_str("an isolated extension proxy must have an exported name")
            }
            Self::IsolatedProxyNameMismatch { identity, proxy } => write!(
                f,
                "isolated extension identity name '{identity}' does not match proxy name '{proxy}'"
            ),
            Self::RootIsolationWithoutImports => {
                f.write_str("an isolated root extension must import at least one repository")
            }
        }
    }
}

impl Error for ExtensionUseError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtensionEvaluationProjection {
    kind: ExtensionUseKind,
    isolation: ExtensionIsolation,
    tags: Box<[TagEvaluationProjection]>,
}

impl ExtensionEvaluationProjection {
    pub fn kind(&self) -> &ExtensionUseKind {
        &self.kind
    }

    pub fn isolation(&self) -> &ExtensionIsolation {
        &self.isolation
    }

    pub fn tags(&self) -> &[TagEvaluationProjection] {
        &self.tags
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TagEvaluationProjection {
    class_name: Box<str>,
    attributes: Box<[RawAttribute]>,
    dev_dependency: bool,
}

impl TagEvaluationProjection {
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    pub fn attributes(&self) -> &[RawAttribute] {
        &self.attributes
    }

    pub fn is_dev_dependency(&self) -> bool {
        self.dev_dependency
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtensionRepositoryMappingProjection {
    kind: ExtensionUseKind,
    isolation: ExtensionIsolation,
    regular_imports: Box<[RepoImportProjection]>,
    dev_imports: Box<[RepoImportProjection]>,
}

impl ExtensionRepositoryMappingProjection {
    pub fn kind(&self) -> &ExtensionUseKind {
        &self.kind
    }

    pub fn isolation(&self) -> &ExtensionIsolation {
        &self.isolation
    }

    pub fn regular_imports(&self) -> &[RepoImportProjection] {
        &self.regular_imports
    }

    pub fn dev_imports(&self) -> &[RepoImportProjection] {
        &self.dev_imports
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepoImportProjection {
    local_name: RepositoryName,
    exported_name: RepositoryName,
}

impl RepoImportProjection {
    pub fn local_name(&self) -> &RepositoryName {
        &self.local_name
    }

    pub fn exported_name(&self) -> &RepositoryName {
        &self.exported_name
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;

    use super::*;

    fn binding(value: &str) -> StarlarkBindingName {
        StarlarkBindingName::parse(value).unwrap()
    }

    fn extension_name(value: &str) -> ExtensionName {
        ExtensionName::parse(value).unwrap()
    }

    fn repo(value: &str) -> RepositoryName {
        RepositoryName::parse(value).unwrap()
    }

    fn attr(name: &str, value: RawAttributeValue) -> RawAttribute {
        RawAttribute::new(name, value)
    }

    fn string(value: &str) -> RawAttributeValue {
        RawAttributeValue::String(value.into())
    }

    fn hash(value: &impl Hash) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn extension(first_use: u32, location: &str, imported: &str) -> ExtensionUse {
        ExtensionUse::new(
            first_use,
            ExtensionUseKind::Regular {
                extension_file: ApparentLabel::parse_normalized(
                    "@rules_python//python/extensions:pip.bzl",
                )
                .unwrap(),
                extension_name: extension_name("pip"),
            },
            ExtensionIsolation::None,
            vec![ProxyUse::new(
                first_use + 1,
                Some(binding("pip")),
                false,
                ModuleSourceLocation::new(location),
                vec![RepoImport::new(
                    first_use + 3,
                    repo("pip_deps"),
                    repo(imported),
                    ModuleSourceLocation::new(location),
                )]
                .into_boxed_slice(),
            )]
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_boxed_slice(),
            vec![
                RawTag::new(
                    first_use + 2,
                    "parse",
                    vec![attr("hub_name", string("pip_deps"))].into_boxed_slice(),
                    false,
                    ModuleSourceLocation::new(location),
                )
                .unwrap(),
            ]
            .into_boxed_slice(),
        )
        .unwrap()
    }

    fn import_only_extension(
        first_use: u32,
        file: &str,
        name: &str,
        local: &str,
        exported: &str,
    ) -> ExtensionUse {
        ExtensionUse::new(
            first_use,
            ExtensionUseKind::Regular {
                extension_file: ApparentLabel::parse_normalized(file).unwrap(),
                extension_name: extension_name(name),
            },
            ExtensionIsolation::None,
            vec![
                ProxyUse::new(
                    first_use + 1,
                    Some(binding(name)),
                    false,
                    ModuleSourceLocation::new(format!("MODULE.bazel:{}", first_use + 1)),
                    vec![RepoImport::new(
                        first_use + 2,
                        repo(local),
                        repo(exported),
                        ModuleSourceLocation::new(format!("MODULE.bazel:{}", first_use + 2)),
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
    fn rejects_invalid_public_names_and_labels() {
        for value in [
            "", "1proxy", "bad-name", "naïve", "False", "None", "True", "and", "as", "assert",
            "async", "await", "break", "class", "continue", "def", "del", "elif", "else", "except",
            "finally", "for", "from", "global", "if", "import", "in", "is", "lambda", "load",
            "nonlocal", "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
        ] {
            assert!(
                StarlarkBindingName::parse(value).is_err(),
                "accepted {value:?}"
            );
        }
        for value in ["for", "True", "_private", "extension"] {
            assert_eq!(ExtensionName::parse(value).unwrap().as_str(), value);
        }
        for value in ["", "1extension", "bad-name", "naïve"] {
            assert!(ExtensionName::parse(value).is_err(), "accepted {value:?}");
        }
        for value in ["", "@repo", "bad/name", "bad+name", "naïve"] {
            assert!(RepositoryName::parse(value).is_err(), "accepted {value:?}");
        }
        for value in [
            "extensions.bzl",
            ":extensions.bzl",
            "@@canonical//:extensions.bzl",
            "@bad+repo//:extensions.bzl",
            "@repo//pkg",
            "@repo//pkg/../bad:extension.bzl",
            "//pkg//bad:extension.bzl",
            "//pkg:../extension.bzl",
        ] {
            assert!(
                ApparentLabel::parse_normalized(value).is_err(),
                "accepted {value:?}"
            );
        }
        assert_eq!(
            ApparentLabel::parse_normalized("@repo//pkg:extension.star")
                .unwrap()
                .as_str(),
            "@repo//pkg:extension.star"
        );
        for value in ["0", "1", "-1", "123456789012345678901234567890"] {
            assert_eq!(
                RawInteger::parse_decimal(value).unwrap().as_decimal(),
                value
            );
        }
        for value in ["", "+1", "00", "01", "-0", "-01", "1.0", " 1"] {
            assert!(
                RawInteger::parse_decimal(value).is_err(),
                "accepted {value:?}"
            );
        }
    }

    #[test]
    fn retains_pinned_maven_and_python_raw_shapes() {
        let maven_install = RawTag::new(
            18,
            "install",
            vec![
                attr(
                    "artifacts",
                    RawAttributeValue::Sequence(
                        [
                            string("com.google.guava:guava:33.5.0-jre"),
                            string("junit:junit:4.13.2"),
                        ]
                        .into(),
                    ),
                ),
                attr(
                    "repositories",
                    RawAttributeValue::Sequence([string("https://repo1.maven.org/maven2")].into()),
                ),
                attr("fetch_sources", RawAttributeValue::Bool(true)),
            ]
            .into(),
            false,
            ModuleSourceLocation::new("MODULE.bazel:18"),
        )
        .unwrap();
        assert_eq!(maven_install.ordinal(), 18);
        assert_eq!(maven_install.attributes()[0].name(), "artifacts");

        let first_artifacts = ["asm", "asm-analysis", "asm-commons", "asm-tree", "asm-util"];
        let test_artifacts = [
            "guava-testlib",
            "jimfs",
            "compile-testing",
            "test-parameter-injector",
            "truth",
            "truth-java8-extension",
            "truth-liteproto-extension",
            "truth-proto-extension",
            "mockito-core",
        ];
        let expanded_artifacts = first_artifacts
            .iter()
            .chain(test_artifacts.iter())
            .enumerate()
            .map(|(index, artifact)| {
                RawTag::new(
                    283 + index as u32,
                    "artifact",
                    [attr("artifact", string(artifact))].into(),
                    false,
                    ModuleSourceLocation::new(format!("MODULE.bazel:{}", 283 + index)),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(expanded_artifacts.len(), 5 + 9);
        assert_eq!(
            expanded_artifacts
                .iter()
                .map(|tag| match tag.attributes()[0].value() {
                    RawAttributeValue::String(value) => value.as_ref(),
                    other => panic!("unexpected artifact shape: {other:?}"),
                })
                .collect::<Vec<_>>(),
            first_artifacts
                .iter()
                .chain(test_artifacts.iter())
                .copied()
                .collect::<Vec<_>>()
        );

        let python_parse = RawTag::new(
            31,
            "parse",
            vec![
                attr("hub_name", string("pip")),
                attr("python_version", string("3.11")),
                attr("requirements_lock", string("//:requirements.txt")),
            ]
            .into(),
            false,
            ModuleSourceLocation::new("MODULE.bazel:31"),
        )
        .unwrap();
        assert_eq!(
            python_parse.attributes()[2].value(),
            &string("//:requirements.txt")
        );
    }

    #[test]
    fn represents_staged_recursive_dict_and_integer_values() {
        let value = RawAttributeValue::Dict(
            OrderedStringDict::new(
                [
                    ("enabled".into(), RawAttributeValue::Bool(true)),
                    (
                        "limits".into(),
                        RawAttributeValue::Sequence(
                            [RawAttributeValue::Integer(
                                RawInteger::parse_decimal("123456789012345678901234567890")
                                    .unwrap(),
                            )]
                            .into(),
                        ),
                    ),
                ]
                .into(),
            )
            .unwrap(),
        );
        let RawAttributeValue::Dict(dict) = value else {
            unreachable!()
        };
        assert_eq!(dict.entries()[0].0.as_ref(), "enabled");
        assert_eq!(dict.entries()[1].0.as_ref(), "limits");
    }

    #[test]
    fn equality_hashing_and_nested_order_are_deterministic() {
        let first = extension(4, "MODULE.bazel:1", "pip_generated");
        let same = extension(4, "MODULE.bazel:1", "pip_generated");
        assert_eq!(first, same);
        assert_eq!(hash(&first), hash(&same));

        let ordered = RawAttributeValue::Sequence([string("first"), string("second")].into());
        let reversed = RawAttributeValue::Sequence([string("second"), string("first")].into());
        assert_ne!(ordered, reversed);
        assert_ne!(hash(&ordered), hash(&reversed));
    }

    #[test]
    fn ordinals_preserve_cross_record_source_order() {
        let value = extension(4, "MODULE.bazel:1", "pip_generated");
        let proxy = &value.proxies()[0];
        assert!(value.first_use_ordinal() < proxy.ordinal());
        assert!(proxy.ordinal() < value.tags()[0].ordinal());
        assert!(value.tags()[0].ordinal() < proxy.imports()[0].ordinal());
    }

    #[test]
    fn flat_tags_preserve_interleaved_proxy_calls() {
        let base = extension(4, "MODULE.bazel:1", "pip_generated");
        let kind = base.kind().clone();
        let tags = ["a", "b", "c"]
            .into_iter()
            .enumerate()
            .map(|(index, class_name)| {
                RawTag::new(
                    10 + index as u32,
                    class_name,
                    Box::new([]),
                    false,
                    ModuleSourceLocation::new(format!("MODULE.bazel:{}", 10 + index)),
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let interleaved = ExtensionUse::new(
            1,
            kind.clone(),
            ExtensionIsolation::None,
            vec![
                ProxyUse::new(
                    2,
                    Some(binding("first")),
                    false,
                    ModuleSourceLocation::new("MODULE.bazel:2"),
                    Box::new([]),
                )
                .unwrap(),
                ProxyUse::new(
                    3,
                    Some(binding("second")),
                    false,
                    ModuleSourceLocation::new("MODULE.bazel:3"),
                    Box::new([]),
                )
                .unwrap(),
            ]
            .into(),
            tags.clone(),
        )
        .unwrap();
        assert_eq!(
            interleaved
                .tags()
                .iter()
                .map(RawTag::class_name)
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );

        let unnamed_proxy = ExtensionUse::new(
            0,
            kind,
            ExtensionIsolation::None,
            vec![
                ProxyUse::new(
                    1,
                    None,
                    false,
                    ModuleSourceLocation::new("MODULE.bazel:101"),
                    Box::new([]),
                )
                .unwrap(),
            ]
            .into(),
            tags,
        )
        .unwrap();
        assert_eq!(
            interleaved.evaluation_projection(),
            unnamed_proxy.evaluation_projection()
        );
        assert!(unnamed_proxy.proxies()[0].exported_name().is_none());
    }

    #[test]
    fn semantic_and_mapping_projections_are_separate() {
        let original = extension(4, "MODULE.bazel:1", "pip_generated");
        let moved_and_reimported = extension(200, "MODULE.bazel:200", "other_generated");

        assert_eq!(
            original.evaluation_projection(),
            moved_and_reimported.evaluation_projection()
        );
        assert_ne!(
            original.repository_mapping_projection(),
            moved_and_reimported.repository_mapping_projection()
        );
        assert_ne!(original, moved_and_reimported);

        let only_moved = extension(300, "MODULE.bazel:300", "pip_generated");
        assert_eq!(
            original.repository_mapping_projection(),
            only_moved.repository_mapping_projection()
        );
    }

    #[test]
    fn mapping_projection_is_canonical_across_proxy_grouping_and_order() {
        let kind = ExtensionUseKind::Regular {
            extension_file: ApparentLabel::parse_normalized("//:extension.star").unwrap(),
            extension_name: extension_name("extension"),
        };
        let import = |ordinal, local: &str, exported: &str| {
            RepoImport::new(
                ordinal,
                repo(local),
                repo(exported),
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
            )
        };
        let proxy = |ordinal, name: &str, dev_dependency, imports: Box<[RepoImport]>| {
            ProxyUse::new(
                ordinal,
                Some(binding(name)),
                dev_dependency,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
                imports,
            )
            .unwrap()
        };

        let first = ExtensionUse::new(
            1,
            kind.clone(),
            ExtensionIsolation::None,
            vec![
                proxy(
                    2,
                    "first",
                    false,
                    vec![
                        import(6, "z_local", "z_generated"),
                        import(7, "a_local", "a_generated"),
                    ]
                    .into(),
                ),
                proxy(
                    3,
                    "second",
                    true,
                    vec![import(5, "dev_local", "dev_generated")].into(),
                ),
            ]
            .into(),
            Box::new([]),
        )
        .unwrap();
        let regrouped = ExtensionUse::new(
            100,
            kind,
            ExtensionIsolation::None,
            vec![
                proxy(
                    101,
                    "dev_first",
                    true,
                    vec![import(103, "dev_local", "dev_generated")].into(),
                ),
                proxy(
                    102,
                    "regular_second",
                    false,
                    vec![
                        import(104, "a_local", "a_generated"),
                        import(105, "z_local", "z_generated"),
                    ]
                    .into(),
                ),
            ]
            .into(),
            Box::new([]),
        )
        .unwrap();

        let projection = first.repository_mapping_projection();
        assert_eq!(projection, regrouped.repository_mapping_projection());
        assert_eq!(
            projection
                .regular_imports()
                .iter()
                .map(|import| import.local_name().as_str())
                .collect::<Vec<_>>(),
            ["a_local", "z_local"]
        );
        assert_eq!(
            projection.dev_imports()[0].local_name().as_str(),
            "dev_local"
        );
        assert_ne!(first, regrouped);
    }

    #[test]
    fn rejects_duplicate_local_and_exported_imports_across_proxies() {
        let base = extension(1, "MODULE.bazel:1", "generated");
        let kind = base.kind().clone();
        let import = |ordinal, local: &str, exported: &str| {
            RepoImport::new(
                ordinal,
                repo(local),
                repo(exported),
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
            )
        };
        let proxy = |ordinal, name: &str, import| {
            ProxyUse::new(
                ordinal,
                Some(binding(name)),
                false,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
                vec![import].into(),
            )
            .unwrap()
        };

        assert_eq!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![
                    proxy(2, "first", import(4, "same", "one")),
                    proxy(3, "second", import(5, "same", "two")),
                ]
                .into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::DuplicateLocalImport(repo("same")))
        );
        assert_eq!(
            ExtensionUse::new(
                1,
                kind,
                ExtensionIsolation::None,
                vec![
                    proxy(2, "first", import(4, "one", "same")),
                    proxy(3, "second", import(5, "two", "same")),
                ]
                .into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::DuplicateExportedImport(repo("same")))
        );
    }

    #[test]
    fn rejects_impossible_event_ordinals() {
        let base = extension(1, "MODULE.bazel:1", "generated");
        let kind = base.kind().clone();
        let proxy = |ordinal| {
            ProxyUse::new(
                ordinal,
                None,
                false,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
                Box::new([]),
            )
            .unwrap()
        };
        let tag = |ordinal| {
            RawTag::new(
                ordinal,
                "tag",
                Box::new([]),
                false,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
            )
            .unwrap()
        };

        assert!(matches!(
            ExtensionUse::new(
                2,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(2)].into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::FirstProxyNotAfterFirstUse { .. })
        ));
        assert!(matches!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(3), proxy(2)].into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::ProxiesNotSourceOrdered { .. })
        ));
        assert!(matches!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(2)].into(),
                vec![tag(5), tag(4)].into(),
            ),
            Err(ExtensionUseError::TagsNotSourceOrdered { .. })
        ));
        assert!(matches!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(2)].into(),
                vec![tag(1)].into(),
            ),
            Err(ExtensionUseError::TagNotAfterFirstUse { .. })
        ));
        assert_eq!(
            ExtensionUse::new(
                1,
                kind,
                ExtensionIsolation::None,
                vec![proxy(2)].into(),
                vec![tag(2)].into(),
            ),
            Err(ExtensionUseError::DuplicateEventOrdinal(2))
        );

        let import = |ordinal| {
            RepoImport::new(
                ordinal,
                repo(&format!("local{ordinal}")),
                repo(&format!("generated{ordinal}")),
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
            )
        };
        assert!(matches!(
            ProxyUse::new(
                3,
                None,
                false,
                ModuleSourceLocation::new("MODULE.bazel:3"),
                vec![import(3)].into(),
            ),
            Err(ProxyUseError::ImportNotAfterProxy { .. })
        ));
        assert!(matches!(
            ProxyUse::new(
                2,
                None,
                false,
                ModuleSourceLocation::new("MODULE.bazel:2"),
                vec![import(4), import(3)].into(),
            ),
            Err(ProxyUseError::ImportsNotSourceOrdered { .. })
        ));
    }

    #[test]
    fn tags_require_an_earlier_proxy_with_matching_dev_provenance() {
        let kind = extension(1, "MODULE.bazel:1", "generated").kind().clone();
        let proxy = |ordinal, dev_dependency| {
            ProxyUse::new(
                ordinal,
                None,
                dev_dependency,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
                Box::new([]),
            )
            .unwrap()
        };
        let tag = |ordinal, dev_dependency| {
            RawTag::new(
                ordinal,
                "tag",
                Box::new([]),
                dev_dependency,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
            )
            .unwrap()
        };

        assert!(matches!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(2, false)].into(),
                vec![tag(3, true)].into(),
            ),
            Err(ExtensionUseError::TagWithoutEligibleProxy {
                dev_dependency: true,
                ..
            })
        ));
        assert!(matches!(
            ExtensionUse::new(
                1,
                kind.clone(),
                ExtensionIsolation::None,
                vec![proxy(3, false)].into(),
                vec![tag(2, false)].into(),
            ),
            Err(ExtensionUseError::TagWithoutEligibleProxy {
                dev_dependency: false,
                ..
            })
        ));
        assert!(
            ExtensionUse::new(
                1,
                kind,
                ExtensionIsolation::None,
                vec![proxy(2, true)].into(),
                vec![tag(3, true)].into(),
            )
            .is_ok()
        );
    }

    #[test]
    fn tag_and_attribute_names_remain_raw_strings() {
        let tag = RawTag::new(
            1,
            "not-an-identifier",
            [RawAttribute::new("also not an identifier", string("value"))].into(),
            false,
            ModuleSourceLocation::new("MODULE.bazel:1"),
        )
        .unwrap();

        assert_eq!(tag.class_name(), "not-an-identifier");
        assert_eq!(tag.attributes()[0].name(), "also not an identifier");
    }

    #[test]
    fn isolation_owns_the_module_and_exported_proxy_name() {
        let isolation = ExtensionIsolation::Isolated {
            owner: ModuleKey::ROOT,
            exported_proxy_name: binding("isolated_pip"),
        };
        assert!(matches!(
            isolation,
            ExtensionIsolation::Isolated {
                owner: ModuleKey::Root,
                ..
            }
        ));
    }

    #[test]
    fn rejects_duplicate_dictionary_keys_and_tag_attributes() {
        let duplicate_dict = OrderedStringDict::new(
            [
                ("key".into(), string("first")),
                ("key".into(), string("second")),
            ]
            .into(),
        );
        assert_eq!(
            duplicate_dict,
            Err(OrderedStringDictError::DuplicateKey("key".into()))
        );

        let duplicate_tag = RawTag::new(
            1,
            "tag",
            [
                attr("name", string("first")),
                attr("name", string("second")),
            ]
            .into(),
            false,
            ModuleSourceLocation::new("MODULE.bazel:1"),
        );
        assert_eq!(
            duplicate_tag,
            Err(RawTagError::DuplicateAttribute("name".into()))
        );
    }

    #[test]
    fn rejects_impossible_isolation_states() {
        let regular = ExtensionUseKind::Regular {
            extension_file: ApparentLabel::parse_normalized("//:extensions.bzl").unwrap(),
            extension_name: extension_name("extension"),
        };
        let isolated = ExtensionIsolation::Isolated {
            owner: ModuleKey::ROOT,
            exported_proxy_name: binding("isolated"),
        };
        let proxy = |ordinal: u32, name: Option<&str>, imports: Box<[RepoImport]>| {
            ProxyUse::new(
                ordinal,
                name.map(binding),
                false,
                ModuleSourceLocation::new(format!("MODULE.bazel:{ordinal}")),
                imports,
            )
            .unwrap()
        };

        assert_eq!(
            ExtensionUse::new(
                1,
                regular.clone(),
                isolated.clone(),
                Box::new([]),
                Box::new([]),
            ),
            Err(ExtensionUseError::NoProxies)
        );
        assert_eq!(
            ExtensionUse::new(
                1,
                regular.clone(),
                isolated.clone(),
                vec![proxy(2, None, Box::new([]))].into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::UnnamedIsolatedProxy)
        );
        assert!(matches!(
            ExtensionUse::new(
                1,
                regular.clone(),
                isolated.clone(),
                vec![proxy(2, Some("other"), Box::new([]))].into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::IsolatedProxyNameMismatch { .. })
        ));
        assert_eq!(
            ExtensionUse::new(
                1,
                regular.clone(),
                isolated.clone(),
                vec![proxy(2, Some("isolated"), Box::new([]))].into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::RootIsolationWithoutImports)
        );
        assert_eq!(
            ExtensionUse::new(
                1,
                regular.clone(),
                isolated.clone(),
                vec![
                    proxy(2, Some("isolated"), Box::new([])),
                    proxy(3, Some("other"), Box::new([])),
                ]
                .into(),
                Box::new([]),
            ),
            Err(ExtensionUseError::IsolatedProxyCount(2))
        );

        let import = RepoImport::new(
            3,
            repo("local"),
            repo("generated"),
            ModuleSourceLocation::new("MODULE.bazel:3"),
        );
        assert!(
            ExtensionUse::new(
                1,
                regular,
                isolated,
                vec![proxy(2, Some("isolated"), vec![import].into())].into(),
                Box::new([]),
            )
            .is_ok()
        );
    }

    #[test]
    fn module_file_retains_extension_first_use_order() {
        let first = extension(4, "MODULE.bazel:1", "pip_generated");
        let second = ExtensionUse::new(
            20,
            ExtensionUseKind::Regular {
                extension_file: ApparentLabel::parse_normalized("//:other.star").unwrap(),
                extension_name: extension_name("other"),
            },
            ExtensionIsolation::None,
            vec![
                ProxyUse::new(
                    21,
                    Some(binding("other")),
                    false,
                    ModuleSourceLocation::new("MODULE.bazel:21"),
                    Box::new([]),
                )
                .unwrap(),
            ]
            .into(),
            Box::new([]),
        )
        .unwrap();
        let file = crate::ModuleFile::new(None, Box::new([]), Box::new([]))
            .with_extension_uses(vec![first, second].into())
            .unwrap();

        assert_eq!(file.extension_uses().len(), 2);
        assert_eq!(file.extension_uses()[0].first_use_ordinal(), 4);
        assert_eq!(file.extension_uses()[1].first_use_ordinal(), 20);
    }

    #[test]
    fn module_file_rejects_unsorted_uses_and_local_repo_collisions() {
        let first = extension(4, "MODULE.bazel:4", "first_generated");
        let second =
            import_only_extension(20, "//:other.star", "other", "pip_deps", "second_generated");
        assert!(matches!(
            crate::ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![second.clone(), first.clone()].into()),
            Err(crate::ModuleFileExtensionUseError::UsesNotSourceOrdered { .. })
        ));
        assert_eq!(
            crate::ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![first.clone(), second].into()),
            Err(crate::ModuleFileExtensionUseError::DuplicateLocalRepoName(
                "pip_deps".into()
            ))
        );

        let dependency = crate::DependencyRequest::new(
            ModuleKey::new(
                crate::ModuleName::parse("rules_python").unwrap(),
                crate::Version::parse("1.0").unwrap(),
            ),
            crate::DependencyRepoName::Apparent("pip_deps".into()),
            false,
        )
        .unwrap();
        assert_eq!(
            crate::ModuleFile::new(None, vec![dependency].into(), Box::new([]))
                .with_extension_uses(vec![first].into()),
            Err(crate::ModuleFileExtensionUseError::DuplicateLocalRepoName(
                "pip_deps".into()
            ))
        );
    }

    #[test]
    fn module_file_rejects_duplicate_identities_and_absolute_event_ordinals() {
        let first = extension(4, "MODULE.bazel:4", "first_generated");
        let duplicate_identity = extension(20, "MODULE.bazel:20", "second_generated");
        assert!(matches!(
            crate::ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![first.clone(), duplicate_identity].into()),
            Err(crate::ModuleFileExtensionUseError::DuplicateExtensionIdentity { .. })
        ));

        let colliding_event =
            import_only_extension(7, "//:other.star", "other", "other", "generated");
        assert_eq!(
            crate::ModuleFile::new(None, Box::new([]), Box::new([]))
                .with_extension_uses(vec![first, colliding_event].into()),
            Err(crate::ModuleFileExtensionUseError::DuplicateEventOrdinal(7))
        );
    }

    #[test]
    fn module_file_reserves_declaration_name_as_default_apparent_repo() {
        let module_name = crate::ModuleName::parse("rules_python").unwrap();
        let declaration = crate::ModuleDeclaration::new(
            Some(module_name),
            crate::Version::EMPTY,
            None,
            Box::new([]),
        );
        let extension_use = import_only_extension(
            1,
            "//:extension.star",
            "extension",
            "rules_python",
            "generated",
        );

        assert_eq!(
            crate::ModuleFile::new(Some(declaration), Box::new([]), Box::new([]))
                .with_extension_uses(vec![extension_use].into()),
            Err(crate::ModuleFileExtensionUseError::DuplicateLocalRepoName(
                "rules_python".into()
            ))
        );
    }
}
