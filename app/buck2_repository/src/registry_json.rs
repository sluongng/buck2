/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::Ipv6Addr;

use buck2_bzlmod::Version;
use http::Uri;
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::de;
use serde::de::DeserializeSeed;
use serde::de::MapAccess;
use serde::de::Visitor;

use crate::path::validate_registry_subpath;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceUrl(Box<str>);

impl SourceUrl {
    pub fn parse(value: &str) -> Result<Self, RegistryJsonError> {
        let uri = value
            .parse::<Uri>()
            .map_err(|_| RegistryJsonError::InvalidUrl("malformed URL"))?;
        if value.trim() != value || value.contains('\\') {
            return Err(RegistryJsonError::InvalidUrl("malformed URL"));
        }
        if !matches!(uri.scheme_str(), Some("http" | "https")) {
            return Err(RegistryJsonError::InvalidUrl(
                "only http and https schemes are allowed",
            ));
        }
        let Some(authority) = uri.authority() else {
            return Err(RegistryJsonError::InvalidUrl("a valid host is required"));
        };
        let host = authority.host();
        let invalid_ipv6 = host.starts_with('[')
            && host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .is_none_or(|host| host.parse::<Ipv6Addr>().is_err());
        if host.is_empty() || invalid_ipv6 {
            return Err(RegistryJsonError::InvalidUrl("a valid host is required"));
        }
        if authority.as_str().contains('@') {
            return Err(RegistryJsonError::InvalidUrl("userinfo is not allowed"));
        }
        if uri
            .path_and_query()
            .and_then(|value| value.query())
            .is_some()
        {
            return Err(RegistryJsonError::InvalidUrl("queries are not allowed"));
        }
        // `http::Uri` rejects fragments. Keep the explicit check to make the
        // frozen policy obvious even if its parser becomes more permissive.
        if value.contains('#') {
            return Err(RegistryJsonError::InvalidUrl("fragments are not allowed"));
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Deserialize)]
struct RawBazelRegistryJson {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    mirrors: Vec<String>,
    module_base_path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BazelRegistryJson {
    mirrors: Vec<SourceUrl>,
    module_base_path: Option<Box<str>>,
}

impl BazelRegistryJson {
    pub fn mirrors(&self) -> &[SourceUrl] {
        &self.mirrors
    }

    pub fn module_base_path(&self) -> Option<&str> {
        self.module_base_path.as_deref()
    }

    /// Missing and blank registry metadata are both represented as `None`.
    /// Syntactically malformed nonblank input is always an error.
    pub fn parse_optional(input: Option<&[u8]>) -> Result<Option<Self>, RegistryJsonError> {
        let Some(input) = input else {
            return Ok(None);
        };
        if input.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        let raw: RawBazelRegistryJson = parse_json("bazel_registry.json", input)?;
        let mirrors = raw
            .mirrors
            .iter()
            .map(|url| SourceUrl::parse(url))
            .collect::<Result<_, _>>()?;
        Ok(Some(Self {
            mirrors,
            module_base_path: raw.module_base_path.map(Into::into),
        }))
    }
}

#[derive(Debug, Deserialize)]
struct RawMetadataJson {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    yanked_versions: OrderedMap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataJson {
    yanked_versions: IndexMap<Version, Box<str>>,
}

impl MetadataJson {
    pub fn yanked_versions(&self) -> &IndexMap<Version, Box<str>> {
        &self.yanked_versions
    }

    pub fn parse(input: &[u8]) -> Result<Self, RegistryJsonError> {
        let raw: RawMetadataJson = parse_json("metadata.json", input)?;
        let mut yanked_versions = IndexMap::with_capacity(raw.yanked_versions.0.len());
        for (spelling, reason) in raw.yanked_versions.0 {
            let version =
                Version::parse(&spelling).map_err(|_| RegistryJsonError::InvalidField {
                    field: "yanked_versions",
                    reason: "invalid Bazel version",
                })?;
            if version.is_empty() {
                return Err(RegistryJsonError::InvalidField {
                    field: "yanked_versions",
                    reason: "a yanked version may not be blank",
                });
            }
            if yanked_versions.insert(version, reason.into()).is_some() {
                return Err(RegistryJsonError::InvalidField {
                    field: "yanked_versions",
                    reason: "duplicate normalized yanked version",
                });
            }
        }
        Ok(Self { yanked_versions })
    }
}

#[derive(Debug, Deserialize)]
struct RawArchiveSourceJson {
    #[serde(rename = "type", default = "default_archive_source_kind")]
    kind: String,
    url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    mirror_urls: Vec<String>,
    integrity: Option<String>,
    strip_prefix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    patches: OrderedMap,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    overlay: OrderedMap,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    patch_strip: i32,
    archive_type: Option<String>,
    // Present in the BCR for generated documentation, but not materialization.
    #[serde(rename = "docs_url")]
    _docs_url: Option<de::IgnoredAny>,
}

#[derive(Debug, Deserialize)]
struct RawSourceKind {
    #[serde(rename = "type", default = "default_archive_source_kind")]
    kind: String,
}

fn default_archive_source_kind() -> String {
    "archive".to_owned()
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceJson {
    Archive(ArchiveSourceJson),
}

impl SourceJson {
    pub fn parse(input: &[u8]) -> Result<Self, RegistryJsonError> {
        let source_kind: RawSourceKind = parse_json("source.json", input)?;
        match source_kind.kind.as_str() {
            "archive" => {}
            "git_repository" => {
                return Err(RegistryJsonError::UnsupportedSourceKind("git_repository"));
            }
            "local_path" => {
                return Err(RegistryJsonError::UnsupportedSourceKind("local_path"));
            }
            _ => return Err(RegistryJsonError::UnsupportedSourceKind("unknown")),
        }

        let raw: RawArchiveSourceJson = parse_json("source.json", input)?;
        debug_assert_eq!(raw.kind, source_kind.kind);
        if raw.patch_strip < 0 {
            return Err(RegistryJsonError::InvalidField {
                field: "patch_strip",
                reason: "value may not be negative",
            });
        }

        let url = required("url", raw.url)?;
        let integrity = required("integrity", raw.integrity)?;
        let url = SourceUrl::parse(&url)?;
        let mirror_urls = raw
            .mirror_urls
            .iter()
            .map(|url| SourceUrl::parse(url))
            .collect::<Result<_, _>>()?;
        validate_artifacts("patches", &raw.patches)?;
        validate_artifacts("overlay", &raw.overlay)?;
        Ok(Self::Archive(ArchiveSourceJson {
            url,
            mirror_urls,
            integrity: integrity.into(),
            strip_prefix: raw.strip_prefix.map(Into::into),
            patches: raw
                .patches
                .0
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            overlay: raw
                .overlay
                .0
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            patch_strip: raw.patch_strip,
            archive_type: raw.archive_type.map(Into::into),
        }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveSourceJson {
    url: SourceUrl,
    mirror_urls: Vec<SourceUrl>,
    integrity: Box<str>,
    strip_prefix: Option<Box<str>>,
    patches: IndexMap<Box<str>, Box<str>>,
    overlay: IndexMap<Box<str>, Box<str>>,
    patch_strip: i32,
    archive_type: Option<Box<str>>,
}

impl ArchiveSourceJson {
    pub fn url(&self) -> &SourceUrl {
        &self.url
    }

    pub fn mirror_urls(&self) -> &[SourceUrl] {
        &self.mirror_urls
    }

    pub fn integrity(&self) -> &str {
        &self.integrity
    }

    pub fn strip_prefix(&self) -> Option<&str> {
        self.strip_prefix.as_deref()
    }

    pub fn patches(&self) -> &IndexMap<Box<str>, Box<str>> {
        &self.patches
    }

    pub fn overlay(&self) -> &IndexMap<Box<str>, Box<str>> {
        &self.overlay
    }

    pub fn patch_strip(&self) -> i32 {
        self.patch_strip
    }

    pub fn archive_type(&self) -> Option<&str> {
        self.archive_type.as_deref()
    }
}

fn required(field: &'static str, value: Option<String>) -> Result<String, RegistryJsonError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(RegistryJsonError::InvalidField {
            field,
            reason: "field is required and may not be blank",
        }),
    }
}

fn validate_artifacts(field: &'static str, values: &OrderedMap) -> Result<(), RegistryJsonError> {
    for (path, integrity) in &values.0 {
        validate_registry_subpath(path).map_err(|_| RegistryJsonError::InvalidField {
            field,
            reason: "expected a canonical relative path",
        })?;
        if integrity.trim().is_empty() {
            return Err(RegistryJsonError::InvalidField {
                field,
                reason: "integrity may not be blank",
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct OrderedMap(IndexMap<String, String>);

impl<'de> Deserialize<'de> for OrderedMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OrderedMapVisitor;

        impl<'de> Visitor<'de> for OrderedMapVisitor {
            type Value = OrderedMap;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object with unique string keys and values")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = IndexMap::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((key, value)) = map.next_entry::<String, String>()? {
                    if values.insert(key.clone(), value).is_some() {
                        return Err(de::Error::custom("duplicate object key"));
                    }
                }
                Ok(OrderedMap(values))
            }
        }

        deserializer.deserialize_map(OrderedMapVisitor)
    }
}

fn parse_json<T: for<'de> Deserialize<'de>>(
    document: &'static str,
    input: &[u8],
) -> Result<T, RegistryJsonError> {
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    UniqueJson
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
        .map_err(|error| malformed_json_error(document, &error))?;
    serde_json::from_slice(input).map_err(|error| malformed_json_error(document, &error))
}

fn malformed_json_error(document: &'static str, error: &serde_json::Error) -> RegistryJsonError {
    let reason = match error.classify() {
        serde_json::error::Category::Io => "I/O error",
        serde_json::error::Category::Syntax => "syntax error",
        serde_json::error::Category::Data => "invalid data",
        serde_json::error::Category::Eof => "unexpected end of input",
    };
    RegistryJsonError::Malformed {
        document,
        reason,
        line: error.line(),
        column: error.column(),
    }
}

/// A syntax and duplicate-key validation pass which deliberately retains no
/// JSON values. This also covers objects ignored by the typed model.
struct UniqueJson;

impl<'de> DeserializeSeed<'de> for UniqueJson {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueJsonVisitor)
    }
}

struct UniqueJsonVisitor;

impl<'de> Visitor<'de> for UniqueJsonVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E>(self, _value: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E>(self, _value: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E>(self, _value: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E>(self, _value: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_string<E>(self, _value: String) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<(), A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        while sequence.next_element_seed(UniqueJson)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = HashSet::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key.clone()) {
                return Err(de::Error::custom("duplicate object key"));
            }
            map.next_value_seed(UniqueJson)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryJsonError {
    Malformed {
        document: &'static str,
        reason: &'static str,
        line: usize,
        column: usize,
    },
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },
    InvalidUrl(&'static str),
    UnsupportedSourceKind(&'static str),
}

impl fmt::Display for RegistryJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed {
                document,
                reason,
                line,
                column,
            } => write!(
                f,
                "malformed {document}: {reason} at line {line} column {column}"
            ),
            Self::InvalidField { field, reason } => write!(f, "invalid `{field}`: {reason}"),
            Self::InvalidUrl(reason) => write!(f, "invalid source URL: {reason}"),
            Self::UnsupportedSourceKind(kind) => {
                write!(f, "unsupported registry source kind `{kind}`")
            }
        }
    }
}

impl Error for RegistryJsonError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(input: &str) -> ArchiveSourceJson {
        match SourceJson::parse(input.as_bytes()).unwrap() {
            SourceJson::Archive(source) => source,
        }
    }

    fn assert_redacted(error: RegistryJsonError, sentinel: &str) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(!display.contains(sentinel), "display leaked sentinel");
        assert!(!debug.contains(sentinel), "debug leaked sentinel");
        assert!(display.len() <= 160, "unbounded display: {}", display.len());
        assert!(debug.len() <= 240, "unbounded debug: {}", debug.len());
    }

    #[test]
    fn parses_all_archive_fields_and_preserves_order() {
        let source = archive(
            r#"{
                "type": "archive",
                "url": "https://example.com/source.tar.gz",
                "mirror_urls": ["http://127.0.0.1:8080/source.tar.gz"],
                "integrity": "sha256-source",
                "strip_prefix": "source-1",
                "patches": {"two.patch": "sha256-two", "one.patch": "sha256-one"},
                "overlay": {"z/BUILD": "sha256-z", "a/BUILD": "sha256-a"},
                "patch_strip": 2,
                "archive_type": "tar.gz",
                "docs_url": "https://example.com/docs.tar.gz"
            }"#,
        );
        assert_eq!(source.url().as_str(), "https://example.com/source.tar.gz");
        assert_eq!(
            source.mirror_urls()[0].as_str(),
            "http://127.0.0.1:8080/source.tar.gz"
        );
        assert_eq!(source.integrity(), "sha256-source");
        assert_eq!(source.strip_prefix(), Some("source-1"));
        assert_eq!(
            source
                .patches()
                .keys()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["two.patch", "one.patch"]
        );
        assert_eq!(
            source
                .overlay()
                .keys()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["z/BUILD", "a/BUILD"]
        );
        assert_eq!(source.patch_strip(), 2);
        assert_eq!(source.archive_type(), Some("tar.gz"));
    }

    #[test]
    fn archive_type_defaults_and_exact_field_names() {
        let source = archive(r#"{"url":"https://example.com/a","integrity":"sha256-x"}"#);
        assert_eq!(source.patch_strip(), 0);
        assert!(source.archive_type().is_none());
        let source =
            archive(r#"{"url":"https://example.com/a","integrity":"x","stripPrefix":"a"}"#);
        assert!(source.strip_prefix().is_none());
        assert!(
            SourceJson::parse(br#"{"type":null,"url":"https://example.com/a","integrity":"x"}"#)
                .is_err()
        );
        assert!(
            SourceJson::parse(
                br#"{"url":"https://example.com/a","integrity":"x","docs_url":{"ignored":true}}"#
            )
            .is_ok()
        );
    }

    #[test]
    fn explicit_null_defaults_match_bazel() {
        let registry = BazelRegistryJson::parse_optional(Some(br#"{"mirrors":null}"#))
            .unwrap()
            .unwrap();
        assert!(registry.mirrors().is_empty());
        assert!(registry.module_base_path().is_none());

        let metadata = MetadataJson::parse(br#"{"yanked_versions":null}"#).unwrap();
        assert!(metadata.yanked_versions().is_empty());

        let source = archive(
            r#"{
                "url":"https://example.com/a",
                "integrity":"x",
                "mirror_urls":null,
                "patches":null,
                "overlay":null,
                "patch_strip":null
            }"#,
        );
        assert!(source.mirror_urls().is_empty());
        assert!(source.patches().is_empty());
        assert!(source.overlay().is_empty());
        assert_eq!(source.patch_strip(), 0);
    }

    #[test]
    fn explicit_null_required_and_map_values_remain_invalid() {
        for input in [
            r#"{"url":null,"integrity":"x"}"#,
            r#"{"url":"https://example.com/a","integrity":null}"#,
            r#"{"type":null,"url":"https://example.com/a","integrity":"x"}"#,
            r#"{"url":"https://example.com/a","integrity":"x","patches":{"a":null}}"#,
            r#"{"url":"https://example.com/a","integrity":"x","overlay":{"a":null}}"#,
        ] {
            assert!(
                SourceJson::parse(input.as_bytes()).is_err(),
                "accepted {input}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_fields_at_every_relevant_level() {
        for input in [
            r#"{"url":"https://example.com/a","url":"https://example.com/b","integrity":"x"}"#,
            r#"{"url":"https://example.com/a","integrity":"x","integrity":"y"}"#,
            r#"{"type":"archive","type":"archive","url":"https://example.com/a","integrity":"x"}"#,
            r#"{"url":"https://example.com/a","integrity":"x","patches":{"a":"x","a":"y"}}"#,
            r#"{"url":"https://example.com/a","integrity":"x","overlay":{"a":"x","a":"y"}}"#,
        ] {
            assert!(
                SourceJson::parse(input.as_bytes()).is_err(),
                "accepted {input}"
            );
        }
        assert!(
            BazelRegistryJson::parse_optional(Some(br#"{"mirrors":[],"mirrors":[]}"#)).is_err()
        );
        assert!(MetadataJson::parse(br#"{"yanked_versions":{},"yanked_versions":{}}"#).is_err());
        assert!(MetadataJson::parse(br#"{"yanked_versions":{"1.0":"a","1.0":"b"}}"#).is_err());
        assert!(MetadataJson::parse(br#"{"future":1,"future":2,"yanked_versions":{}}"#).is_err());
    }

    #[test]
    fn rejects_unsupported_source_kinds_with_stable_errors() {
        for (kind, expected) in [
            (
                "git_repository",
                "unsupported registry source kind `git_repository`",
            ),
            (
                "local_path",
                "unsupported registry source kind `local_path`",
            ),
            ("future", "unsupported registry source kind `unknown`"),
        ] {
            let input = format!(r#"{{"type":"{kind}"}}"#);
            assert_eq!(
                SourceJson::parse(input.as_bytes()).unwrap_err().to_string(),
                expected
            );
        }
        assert_eq!(
            SourceJson::parse(
                br#"{"type":"git_repository","remote":"https://example.com/repo.git"}"#
            )
            .unwrap_err()
            .to_string(),
            "unsupported registry source kind `git_repository`"
        );
        assert_eq!(
            SourceJson::parse(br#"{"type":"local_path","path":"../module"}"#)
                .unwrap_err()
                .to_string(),
            "unsupported registry source kind `local_path`"
        );
    }

    #[test]
    fn rejects_malformed_and_blank_required_fields() {
        for input in [
            "{",
            r#"{}"#,
            r#"{"url":"","integrity":"x"}"#,
            r#"{"url":"https://example.com/a"}"#,
            r#"{"url":"https://example.com/a","integrity":" "}"#,
            r#"{"url":"https://example.com/a","integrity":"x","patches":{"a":" "}}"#,
            r#"{"url":"https://example.com/a","integrity":"x","overlay":{"../a":"x"}}"#,
            r#"{"url":"https://example.com/a","integrity":"x","patch_strip":-1}"#,
        ] {
            assert!(
                SourceJson::parse(input.as_bytes()).is_err(),
                "accepted {input}"
            );
        }
    }

    #[test]
    fn enforces_frozen_url_spelling_policy() {
        for url in [
            "ftp://example.com/a",
            "file:///tmp/a",
            "https://user@example.com/a",
            "https://example.com/a?token=x",
            "https://example.com/a#fragment",
            "https://example.com\\a",
            "https://:443/a",
            "https://[]/a",
            "https://[not-ipv6]/a",
            "relative/path",
        ] {
            let input = format!(r#"{{"url":"{url}","integrity":"x"}}"#);
            assert!(
                SourceJson::parse(input.as_bytes()).is_err(),
                "accepted {url}"
            );
        }
        let input = r#"{"url":"https://example.com/a","mirror_urls":["https://user@example.com/a"],"integrity":"x"}"#;
        assert!(SourceJson::parse(input.as_bytes()).is_err());
        assert!(SourceJson::parse(br#"{"url":"https://[::1]/a","integrity":"x"}"#).is_ok());
    }

    #[test]
    fn rejected_urls_do_not_leak_credentials() {
        for url in [
            "https://user:secret-userinfo@example.com/a",
            "https://example.com/a?token=secret-query",
            "https://example.com/secret-path\\tail",
        ] {
            let input = format!(r#"{{"url":"{url}","integrity":"x"}}"#);
            let error = SourceJson::parse(input.as_bytes()).unwrap_err();
            assert!(!error.to_string().contains("secret-"));
            assert!(!format!("{error:?}").contains("secret-"));
        }
        let input =
            br#"{"type":"secret-unknown-kind","url":"https://example.com/a","integrity":"x"}"#;
        let error = SourceJson::parse(input).unwrap_err();
        assert!(!error.to_string().contains("secret-"));
        assert!(!format!("{error:?}").contains("secret-"));
    }

    #[test]
    fn all_rejected_json_inputs_have_bounded_redacted_errors() {
        let huge = "secret-huge-scalar".repeat(300);
        let source =
            format!(r#"{{"url":"https://example.com/a","integrity":"x","patch_strip":"{huge}"}}"#);
        assert_redacted(
            SourceJson::parse(source.as_bytes()).unwrap_err(),
            "secret-huge-scalar",
        );

        assert_redacted(
            MetadataJson::parse(br#"{"yanked_versions":{"!secret-version":"reason"}}"#)
                .unwrap_err(),
            "secret-version",
        );

        for (input, sentinel) in [
            (
                r#"{"url":"https://example.com/a","integrity":"x","patches":{"../secret-patch":"x"}}"#,
                "secret-patch",
            ),
            (
                r#"{"url":"https://example.com/a","integrity":"x","overlay":{"a?secret-overlay":"x"}}"#,
                "secret-overlay",
            ),
            (
                r#"{"url":"https://example.com/a","integrity":"x","patches":{"a":["secret-patch-value"]}}"#,
                "secret-patch-value",
            ),
            (
                r#"{"url":"https://example.com/a","integrity":"x","overlay":{"a":{"secret-overlay-value":true}}}"#,
                "secret-overlay-value",
            ),
            (
                r#"{"secret-duplicate":1,"secret-duplicate":2,"url":"https://example.com/a","integrity":"x"}"#,
                "secret-duplicate",
            ),
            (
                r#"{"url":"https://example.com/a","integrity":"x"} secret-trailing"#,
                "secret-trailing",
            ),
        ] {
            assert_redacted(SourceJson::parse(input.as_bytes()).unwrap_err(), sentinel);
        }
    }

    #[test]
    fn metadata_uses_relaxed_versions_and_preserves_reasons() {
        let metadata = MetadataJson::parse(
            br#"{
                "homepage":"https://example.com",
                "versions":["1.0.patch.3"],
                "yanked_versions":{"1.0.patch.3-pre+build":"reason exactly as declared"}
            }"#,
        )
        .unwrap();
        let (version, reason) = metadata.yanked_versions().first().unwrap();
        assert_eq!(version.normalized(), "1.0.patch.3-pre");
        assert_eq!(reason.as_ref(), "reason exactly as declared");
    }

    #[test]
    fn bazel_registry_distinguishes_absent_blank_and_malformed() {
        assert_eq!(BazelRegistryJson::parse_optional(None).unwrap(), None);
        assert_eq!(
            BazelRegistryJson::parse_optional(Some(b" \n\t")).unwrap(),
            None
        );
        assert!(BazelRegistryJson::parse_optional(Some(b"{")).is_err());
        let parsed = BazelRegistryJson::parse_optional(Some(
            br#"{"mirrors":["https://mirror.example.com/"],"module_base_path":"/srv/modules"}"#,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(parsed.mirrors()[0].as_str(), "https://mirror.example.com/");
        assert_eq!(parsed.module_base_path(), Some("/srv/modules"));
    }

    #[test]
    fn unknown_fields_are_ignored_like_bazel() {
        assert!(
            SourceJson::parse(br#"{"url":"https://example.com/a","integrity":"x","future":true}"#)
                .is_ok()
        );
        assert!(BazelRegistryJson::parse_optional(Some(br#"{"future":true}"#)).is_ok());
        assert!(MetadataJson::parse(br#"{"future":{"nested":true},"yanked_versions":{}}"#).is_ok());
    }
}
