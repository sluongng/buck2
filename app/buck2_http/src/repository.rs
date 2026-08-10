/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use http::HeaderName;
use http::Method;
use http::Uri;
use http::header;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;

use crate::HttpClient;
use crate::HttpError;
use crate::client::DEFAULT_USER_AGENT;

const DEFAULT_MAX_REDIRECTS: usize = 10;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

/// A URI representation that is safe to include in diagnostics.
///
/// Queries and authority user information are deliberately omitted because
/// repository URLs commonly carry credentials in those locations.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RedactedUri(String);

impl RedactedUri {
    pub fn from_uri(uri: &Uri) -> Self {
        let mut value = String::new();
        if let Some(scheme) = uri.scheme_str() {
            value.push_str(scheme);
            value.push_str("://");
        }
        if let Some(host) = uri.host() {
            value.push_str(host);
            if let Some(port) = uri.port_u16() {
                value.push(':');
                value.push_str(&port.to_string());
            }
        }
        value.push_str(uri.path());
        if uri.query().is_some() {
            value.push_str("?<redacted>");
        }
        Self(value)
    }

    pub(crate) fn invalid() -> Self {
        Self("<redacted invalid URI>".to_owned())
    }
}

impl fmt::Display for RedactedUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for RedactedUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(crate) fn redact_uri(uri: &Uri) -> String {
    RedactedUri::from_uri(uri).to_string()
}

fn redact_redirect_uri(uri: &Uri) -> String {
    let mut value = String::new();
    if let Some(scheme) = uri.scheme_str() {
        value.push_str(scheme);
        value.push_str("://");
    }
    if let Some(host) = uri.host() {
        value.push_str(host);
        if let Some(port) = uri.port_u16() {
            value.push(':');
            value.push_str(&port.to_string());
        }
    }
    value.push_str("/<redirected>");
    value
}

/// Fixed safety limits for repository HTTP requests.
///
/// The default accepts HTTPS only. Plain HTTP must be explicitly enabled by a
/// caller for a loopback origin. HTTPS-to-HTTP redirects are rejected even
/// when loopback HTTP is enabled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryHttpPolicy {
    allow_http: bool,
    max_redirects: usize,
    max_response_bytes: u64,
}

impl Default for RepositoryHttpPolicy {
    fn default() -> Self {
        Self {
            allow_http: false,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

impl RepositoryHttpPolicy {
    pub fn new(max_response_bytes: u64) -> Self {
        Self {
            max_response_bytes,
            ..Self::default()
        }
    }

    pub fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    pub fn with_max_redirects(mut self, max_redirects: usize) -> Self {
        self.max_redirects = max_redirects;
        self
    }

    pub fn max_response_bytes(&self) -> u64 {
        self.max_response_bytes
    }

    fn validate_hop(&self, previous: Option<&Uri>, next: &Uri) -> Result<(), HttpError> {
        self.validate_hop_with_diagnostic(previous, next, redact_uri(next))
    }

    fn validate_redirect_hop(&self, previous: &Uri, next: &Uri) -> Result<(), HttpError> {
        self.validate_hop_with_diagnostic(Some(previous), next, redact_redirect_uri(next))
    }

    fn validate_hop_with_diagnostic(
        &self,
        previous: Option<&Uri>,
        next: &Uri,
        uri: String,
    ) -> Result<(), HttpError> {
        let Some(scheme) = next.scheme_str() else {
            return Err(HttpError::RepositoryPolicy {
                uri,
                reason: "URI has no scheme",
            });
        };
        if next.authority().is_none() {
            return Err(HttpError::RepositoryPolicy {
                uri,
                reason: "URI has no authority",
            });
        }
        if previous.and_then(Uri::scheme_str) == Some("https") && scheme == "http" {
            return Err(HttpError::RepositoryPolicy {
                uri,
                reason: "HTTPS-to-HTTP redirect is forbidden",
            });
        }
        if scheme == "http" {
            let loopback = next.host().is_some_and(is_loopback_host);
            if !self.allow_http || !loopback {
                return Err(HttpError::RepositoryPolicy {
                    uri,
                    reason: "plain HTTP is allowed only for explicit loopback origins",
                });
            }
        } else if scheme != "https" {
            return Err(HttpError::RepositoryPolicy {
                uri,
                reason: "only HTTPS is allowed",
            });
        }
        Ok(())
    }
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host);
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// A bounded failure kind for repository request hooks.
///
/// Hook failures intentionally do not accept arbitrary error text so that a
/// credential provider cannot accidentally copy a secret into diagnostics.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RepositoryRequestHookError {
    Rejected,
    CredentialsUnavailable,
}

impl fmt::Display for RepositoryRequestHookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected => f.write_str("request rejected"),
            Self::CredentialsUnavailable => f.write_str("credentials unavailable"),
        }
    }
}

/// Hooks run before the initial request and before every effective redirect.
///
/// `validate_uri` can enforce caller-specific origin policy. On a cross-origin
/// redirect all carried headers are discarded before `prepare_headers` runs,
/// allowing credentials to be resolved for the destination rather than copied
/// from the source.
///
/// Plain HTTP is restricted to loopback and is always unauthenticated: after
/// hooks run, only the same conditional/content-negotiation allowlist accepted
/// from callers is retained. Custom and credential headers are HTTPS-only.
///
/// These hooks observe URI hops, not connector-resolved socket addresses. The
/// current Hyper connector does not expose each selected endpoint, so callers
/// that need IP-range policy must use a connector that enforces it at connect
/// time.
pub trait RepositoryRequestHooks: Send + Sync {
    fn validate_uri(
        &self,
        _previous: Option<&Uri>,
        _next: &Uri,
    ) -> Result<(), RepositoryRequestHookError> {
        Ok(())
    }

    fn prepare_headers(
        &self,
        _uri: &Uri,
        _headers: &mut HeaderMap,
    ) -> Result<(), RepositoryRequestHookError> {
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct NoRepositoryRequestHooks;

impl RepositoryRequestHooks for NoRepositoryRequestHooks {}

impl HttpClient {
    /// Send a policy-checked GET whose returned body cannot exceed the
    /// configured byte limit.
    pub async fn repository_get<'a>(
        &'a self,
        uri: &str,
        headers: HeaderMap,
        policy: &RepositoryHttpPolicy,
    ) -> Result<Response<BoxStream<'a, Result<Bytes, HttpError>>>, HttpError> {
        self.repository_get_with_hooks(uri, headers, policy, &NoRepositoryRequestHooks)
            .await
    }

    /// Like `repository_get`, with per-hop origin and credential hooks.
    pub async fn repository_get_with_hooks<'a>(
        &'a self,
        uri: &str,
        mut headers: HeaderMap,
        policy: &RepositoryHttpPolicy,
        hooks: &dyn RepositoryRequestHooks,
    ) -> Result<Response<BoxStream<'a, Result<Bytes, HttpError>>>, HttpError> {
        let mut current = uri.parse::<Uri>().map_err(|source| HttpError::InvalidUri {
            uri: RedactedUri::invalid().to_string(),
            source,
        })?;
        let initial = redact_uri(&current);
        let mut diagnostic_uri = initial.clone();
        policy.validate_hop(None, &current)?;
        sanitize_caller_headers(&mut headers);
        run_hooks(hooks, None, &current, &mut headers, &diagnostic_uri)?;

        let mut redirects = 0;
        loop {
            sanitize_effective_headers(&mut headers);
            if current.scheme_str() == Some("http") {
                sanitize_caller_headers(&mut headers);
            }
            let mut builder = Request::builder()
                .method(Method::GET)
                .uri(current.clone())
                .header(header::USER_AGENT, DEFAULT_USER_AGENT);
            *builder
                .headers_mut()
                .expect("request URI and method were already validated") = headers.clone();
            if !headers.contains_key(header::USER_AGENT) {
                builder = builder.header(header::USER_AGENT, DEFAULT_USER_AGENT);
            }
            let request = builder
                .body(Bytes::new())
                .map_err(HttpError::BuildRequest)?;
            tracing::debug!(
                method = %request.method(),
                uri = %diagnostic_uri,
                "http: repository request"
            );

            let response = self
                .send_request_impl_with_diagnostic_uri(request, diagnostic_uri.clone())
                .await?;
            if is_redirect(response.status())
                && let Some(location) = response.headers().get(header::LOCATION)
                && let Ok(location) = location.to_str()
            {
                if redirects >= policy.max_redirects {
                    return Err(HttpError::TooManyRedirects {
                        uri: initial,
                        max_redirects: policy.max_redirects,
                    });
                }

                let next = with_redirect(&current, location)?;
                let next_diagnostic_uri = redact_redirect_uri(&next);
                policy.validate_redirect_hop(&current, &next)?;
                apply_redirect_origin_boundary(&current, &next, &mut headers);
                run_hooks(
                    hooks,
                    Some(&current),
                    &next,
                    &mut headers,
                    &next_diagnostic_uri,
                )?;
                current = next;
                diagnostic_uri = next_diagnostic_uri;
                redirects += 1;
                continue;
            }

            if !response.status().is_success() {
                let status = response.status();
                return Err(HttpError::Status {
                    status,
                    uri: diagnostic_uri,
                    // Repository endpoints can reflect request credentials in
                    // error bodies. Keep the existing typed status but never
                    // copy an untrusted response body into diagnostics.
                    text: "<repository response body omitted>".to_owned(),
                });
            }

            return finish_response(response, diagnostic_uri, policy.max_response_bytes);
        }
    }
}

fn run_hooks(
    hooks: &dyn RepositoryRequestHooks,
    previous: Option<&Uri>,
    next: &Uri,
    headers: &mut HeaderMap,
    diagnostic_uri: &str,
) -> Result<(), HttpError> {
    hooks
        .validate_uri(previous, next)
        .map_err(|kind| HttpError::RepositoryRequestHook {
            uri: diagnostic_uri.to_owned(),
            kind,
        })?;
    hooks
        .prepare_headers(next, headers)
        .map_err(|kind| HttpError::RepositoryRequestHook {
            uri: diagnostic_uri.to_owned(),
            kind,
        })
}

fn finish_response<'a>(
    response: Response<BoxStream<'a, hyper::Result<Bytes>>>,
    diagnostic_uri: String,
    limit: u64,
) -> Result<Response<BoxStream<'a, Result<Bytes, HttpError>>>, HttpError> {
    if let Some(advertised_bytes) = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && advertised_bytes > limit
    {
        return Err(HttpError::ResponseTooLarge {
            uri: diagnostic_uri,
            limit_bytes: limit,
            advertised_bytes: Some(advertised_bytes),
        });
    }

    Ok(response.map(move |body| bounded_body(body, diagnostic_uri, limit)))
}

fn bounded_body<'a>(
    body: BoxStream<'a, hyper::Result<Bytes>>,
    uri: String,
    limit: u64,
) -> BoxStream<'a, Result<Bytes, HttpError>> {
    futures::stream::unfold(
        (body, 0u64, false, uri),
        move |(mut body, seen, done, uri)| async move {
            if done {
                return None;
            }
            match body.next().await {
                Some(Ok(bytes)) => {
                    let next_seen = seen.saturating_add(bytes.len() as u64);
                    if next_seen > limit {
                        Some((
                            Err(HttpError::ResponseTooLarge {
                                uri: uri.clone(),
                                limit_bytes: limit,
                                advertised_bytes: None,
                            }),
                            (body, seen, true, uri),
                        ))
                    } else {
                        Some((Ok(bytes), (body, next_seen, false, uri)))
                    }
                }
                Some(Err(source)) => Some((
                    Err(HttpError::ReadResponse {
                        uri: uri.clone(),
                        source,
                    }),
                    (body, seen, true, uri),
                )),
                None => None,
            }
        },
    )
    .boxed()
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn with_redirect(current: &Uri, location: &str) -> Result<Uri, HttpError> {
    if location.contains('#') {
        return Err(HttpError::RepositoryPolicy {
            uri: "<redacted redirect URI>".to_owned(),
            reason: "redirect Location contains a fragment",
        });
    }
    if has_uri_scheme(location) {
        let absolute = location
            .parse::<Uri>()
            .map_err(|source| HttpError::InvalidUri {
                uri: RedactedUri::invalid().to_string(),
                source,
            })?;
        return rebuild_uri_with_normalized_path(&absolute);
    }

    // `http::Uri` parses a bare relative reference such as `next` as an
    // authority-form URI. Redirect Location values use RFC 3986 reference
    // syntax, so resolve the raw header spelling instead.
    let (reference, location_query) = location
        .split_once('?')
        .map_or((location, None), |(path, query)| (path, Some(query)));
    let (location_authority, location_path) =
        if let Some(network_path) = reference.strip_prefix("//") {
            let boundary = network_path.find('/').unwrap_or(network_path.len());
            let (authority, path) = network_path.split_at(boundary);
            (Some(authority), path)
        } else {
            (None, reference)
        };

    let mut redirected = Uri::builder();
    if let Some(scheme) = current.scheme() {
        redirected = redirected.scheme(scheme.clone());
    }
    if let Some(authority) = location_authority {
        redirected = redirected.authority(authority);
    } else if let Some(authority) = current.authority() {
        redirected = redirected.authority(authority.clone());
    }

    let path = if location_authority.is_some() {
        if location_path.is_empty() {
            "/".to_owned()
        } else {
            normalize_redirect_path(location_path)
        }
    } else if location_path.is_empty() {
        current.path().to_owned()
    } else if location_path.starts_with('/') {
        normalize_redirect_path(location_path)
    } else {
        let base = current
            .path()
            .rsplit_once('/')
            .map_or("", |(directory, _)| directory);
        normalize_redirect_path(&format!("{base}/{location_path}"))
    };
    let query = if location_authority.is_some() {
        location_query
    } else if location_path.is_empty() {
        location_query.or_else(|| current.query())
    } else {
        location_query
    };
    let path_and_query = query.map_or(path.clone(), |query| format!("{path}?{query}"));
    redirected = redirected.path_and_query(path_and_query);
    redirected.build().map_err(HttpError::BuildRequest)
}

fn rebuild_uri_with_normalized_path(uri: &Uri) -> Result<Uri, HttpError> {
    let path = normalize_redirect_path(uri.path());
    let path_and_query = uri
        .query()
        .map_or(path.clone(), |query| format!("{path}?{query}"));
    let mut rebuilt = Uri::builder();
    if let Some(scheme) = uri.scheme() {
        rebuilt = rebuilt.scheme(scheme.clone());
    }
    if let Some(authority) = uri.authority() {
        rebuilt = rebuilt.authority(authority.clone());
    }
    rebuilt
        .path_and_query(path_and_query)
        .build()
        .map_err(HttpError::BuildRequest)
}

fn has_uri_scheme(reference: &str) -> bool {
    let Some(colon) = reference.find(':') else {
        return false;
    };
    if reference[..colon].contains(['/', '?', '#']) {
        return false;
    }
    let mut chars = reference[..colon].chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn normalize_redirect_path(path: &str) -> String {
    // RFC 3986 section 5.2.4. In particular, moving path segments one at a
    // time preserves empty segments (`//`), which may be significant to an
    // object server or signed URL.
    let mut input = path.to_owned();
    let mut output = String::new();
    while !input.is_empty() {
        if input.starts_with("../") {
            input.drain(..3);
        } else if input.starts_with("./") {
            input.drain(..2);
        } else if input.starts_with("/./") {
            input.replace_range(..3, "/");
        } else if input == "/." {
            input.replace_range(.., "/");
        } else if input.starts_with("/../") {
            input.replace_range(..4, "/");
            remove_last_path_segment(&mut output);
        } else if input == "/.." {
            input.replace_range(.., "/");
            remove_last_path_segment(&mut output);
        } else if input == "." || input == ".." {
            input.clear();
        } else {
            let end = if input.starts_with('/') {
                input[1..]
                    .find('/')
                    .map_or(input.len(), |position| position + 1)
            } else {
                input.find('/').unwrap_or(input.len())
            };
            output.push_str(&input[..end]);
            input.drain(..end);
        }
    }
    output
}

fn remove_last_path_segment(path: &mut String) {
    if let Some(position) = path.rfind('/') {
        path.truncate(position);
    } else {
        path.clear();
    }
}

fn effective_port(uri: &Uri) -> Option<u16> {
    uri.port_u16().or_else(|| match uri.scheme_str() {
        Some("http") => Some(80),
        Some("https") => Some(443),
        _ => None,
    })
}

fn is_cross_origin(left: &Uri, right: &Uri) -> bool {
    left.scheme_str() != right.scheme_str()
        || !left
            .host()
            .zip(right.host())
            .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
        || effective_port(left) != effective_port(right)
}

fn sanitize_caller_headers(headers: &mut HeaderMap) {
    // Repository credentials are destination-scoped and must be supplied by
    // `prepare_headers`. Retain only standard conditional/content-negotiation
    // headers whose meaning is independent of authentication and routing.
    let mut safe = HeaderMap::new();
    for name in [
        header::ACCEPT,
        header::ACCEPT_ENCODING,
        header::ACCEPT_LANGUAGE,
        header::CACHE_CONTROL,
        header::IF_MATCH,
        header::IF_MODIFIED_SINCE,
        header::IF_NONE_MATCH,
        header::IF_UNMODIFIED_SINCE,
        header::RANGE,
    ] {
        for value in headers.get_all(&name) {
            safe.append(name.clone(), value.clone());
        }
    }
    *headers = safe;
}

fn sanitize_effective_headers(headers: &mut HeaderMap) {
    let connection_nominated = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    for name in connection_nominated {
        headers.remove(name);
    }

    // Hyper must derive framing, routing, and proxy state from the effective
    // request. Hooks may provide destination Authorization/Cookie/custom auth
    // fields, but cannot override these transport-controlled headers.
    for name in [
        header::CONNECTION,
        header::CONTENT_LENGTH,
        header::EXPECT,
        header::HOST,
        HeaderName::from_static("http2-settings"),
        HeaderName::from_static("keep-alive"),
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        HeaderName::from_static("proxy-connection"),
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ] {
        headers.remove(name);
    }
}

fn apply_redirect_origin_boundary(current: &Uri, next: &Uri, headers: &mut HeaderMap) {
    if is_cross_origin(current, next) {
        headers.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::TryStreamExt;
    use http::HeaderValue;
    use httptest::Expectation;
    use httptest::matchers::*;
    use httptest::responders;

    use super::*;

    #[test]
    fn redacted_uri_hides_query_and_user_info() {
        let uri: Uri = "https://user:secret@example.com/archive?token=secret"
            .parse()
            .unwrap();
        let rendered = format!("{:?}", RedactedUri::from_uri(&uri));
        assert_eq!("https://example.com/archive?<redacted>", rendered);
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("token"));
    }

    #[test]
    fn rejects_https_downgrade_even_when_http_is_allowed() {
        let policy = RepositoryHttpPolicy::default().with_allow_http(true);
        let previous: Uri = "https://example.com/archive".parse().unwrap();
        let next: Uri = "http://example.com/archive".parse().unwrap();
        assert!(matches!(
            policy.validate_hop(Some(&previous), &next),
            Err(HttpError::RepositoryPolicy {
                reason: "HTTPS-to-HTTP redirect is forbidden",
                ..
            })
        ));
    }

    #[test]
    fn rejects_remote_http_even_when_http_is_enabled() {
        let policy = RepositoryHttpPolicy::default().with_allow_http(true);
        let uri: Uri = "http://example.com/archive".parse().unwrap();
        assert!(matches!(
            policy.validate_hop(None, &uri),
            Err(HttpError::RepositoryPolicy {
                reason: "plain HTTP is allowed only for explicit loopback origins",
                ..
            })
        ));
    }

    #[test]
    fn permits_explicit_http_for_loopback_hosts() {
        let policy = RepositoryHttpPolicy::default().with_allow_http(true);
        for value in [
            "http://127.0.0.1/archive",
            "http://[::1]/archive",
            "http://localhost/archive",
            "http://localhost./archive",
            "http://registry.test.localhost/archive",
        ] {
            let uri: Uri = value.parse().unwrap();
            policy.validate_hop(None, &uri).unwrap();
        }
    }

    #[tokio::test]
    async fn rejects_plain_http_before_network() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/never"))
                .times(0)
                .respond_with(responders::status_code(200)),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get(
                &server.url_str("/never"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default(),
            )
            .await;
        assert!(matches!(
            result,
            Err(HttpError::RepositoryPolicy {
                reason: "plain HTTP is allowed only for explicit loopback origins",
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn default_ports_are_same_origin() {
        let implicit_https: Uri = "https://example.com/a".parse().unwrap();
        let explicit_https: Uri = "https://example.com:443/b".parse().unwrap();
        let other_scheme: Uri = "http://example.com:80/b".parse().unwrap();
        assert!(!is_cross_origin(&implicit_https, &explicit_https));
        assert!(is_cross_origin(&implicit_https, &other_scheme));
    }

    struct RejectingHooks;

    impl RepositoryRequestHooks for RejectingHooks {
        fn validate_uri(
            &self,
            _previous: Option<&Uri>,
            _next: &Uri,
        ) -> Result<(), RepositoryRequestHookError> {
            Err(RepositoryRequestHookError::Rejected)
        }
    }

    #[tokio::test]
    async fn hook_rejection_redacts_uri() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get_with_hooks(
                "https://example.com/archive?token=secret",
                HeaderMap::new(),
                &RepositoryHttpPolicy::default(),
                &RejectingHooks,
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected hook rejection"),
        };
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("token"));
        Ok(())
    }

    #[test]
    fn resolves_rfc3986_redirect_references() {
        let current: Uri = "https://example.com/a/b/archive?old=value".parse().unwrap();
        let cases = [
            ("/absolute", "https://example.com/absolute"),
            ("relative", "https://example.com/a/b/relative"),
            ("../parent", "https://example.com/a/parent"),
            ("?new=value", "https://example.com/a/b/archive?new=value"),
            ("//mirror.example/new", "https://mirror.example/new"),
            (
                "https://mirror.example/a/../objects//digest?x=1",
                "https://mirror.example/objects//digest?x=1",
            ),
            ("/objects//digest", "https://example.com/objects//digest"),
            ("nested//digest", "https://example.com/a/b/nested//digest"),
        ];
        for (location, expected) in cases {
            assert_eq!(expected, with_redirect(&current, location).unwrap());
        }
    }

    struct RecordingHooks {
        destinations: Mutex<Vec<String>>,
        observed_headers: Mutex<Vec<HeaderMap>>,
    }

    impl RepositoryRequestHooks for RecordingHooks {
        fn prepare_headers(
            &self,
            uri: &Uri,
            headers: &mut HeaderMap,
        ) -> Result<(), RepositoryRequestHookError> {
            self.destinations.lock().unwrap().push(redact_uri(uri));
            self.observed_headers.lock().unwrap().push(headers.clone());
            if uri.path() == "/start" {
                headers.insert(header::AUTHORIZATION, HeaderValue::from_static("secret"));
                headers.insert(header::COOKIE, HeaderValue::from_static("session=secret"));
                headers.insert(
                    HeaderName::from_static("x-api-key"),
                    HeaderValue::from_static("api-secret"),
                );
                headers.insert(header::HOST, HeaderValue::from_static("attacker.invalid"));
                headers.insert(
                    header::CONNECTION,
                    HeaderValue::from_static("x-connection-secret"),
                );
                headers.insert(
                    HeaderName::from_static("x-connection-secret"),
                    HeaderValue::from_static("connection-secret"),
                );
                headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("999"));
                headers.insert(
                    header::TRANSFER_ENCODING,
                    HeaderValue::from_static("chunked"),
                );
                headers.insert(
                    HeaderName::from_static("keep-alive"),
                    HeaderValue::from_static("timeout=5"),
                );
                headers.insert(
                    header::PROXY_AUTHENTICATE,
                    HeaderValue::from_static("secret"),
                );
                headers.insert(
                    HeaderName::from_static("http2-settings"),
                    HeaderValue::from_static("secret"),
                );
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn cross_origin_redirect_clears_headers_before_hook() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let destination = httptest::Server::run();
        let destination_host = destination.url("/").authority().unwrap().to_string();
        destination.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/finish"),
                request::headers(not(contains(key(header::AUTHORIZATION.as_str())))),
                request::headers(not(contains(key(header::COOKIE.as_str())))),
                request::headers(not(contains(key("x-api-key")))),
                request::headers(not(contains(key("x-connection-secret")))),
                request::headers(not(contains(key(header::CONNECTION.as_str())))),
                request::headers(not(contains((header::CONTENT_LENGTH.as_str(), "999")))),
                request::headers(not(contains((
                    header::TRANSFER_ENCODING.as_str(),
                    "chunked"
                )))),
                request::headers(not(contains(key("keep-alive")))),
                request::headers(not(contains(key(header::PROXY_AUTHENTICATE.as_str())))),
                request::headers(not(contains(key("http2-settings")))),
                request::headers(contains((header::HOST.as_str(), destination_host))),
            ])
            .respond_with(responders::status_code(200)),
        );
        let source = httptest::Server::run();
        let source_host = source.url("/").authority().unwrap().to_string();
        source.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/start"),
                request::headers(not(contains(key(header::AUTHORIZATION.as_str())))),
                request::headers(not(contains(key(header::COOKIE.as_str())))),
                request::headers(not(contains(key("x-api-key")))),
                request::headers(not(contains(key("x-connection-secret")))),
                request::headers(not(contains(key(header::CONNECTION.as_str())))),
                request::headers(not(contains((header::CONTENT_LENGTH.as_str(), "999")))),
                request::headers(not(contains((
                    header::TRANSFER_ENCODING.as_str(),
                    "chunked"
                )))),
                request::headers(not(contains(key("keep-alive")))),
                request::headers(not(contains(key(header::PROXY_AUTHENTICATE.as_str())))),
                request::headers(not(contains(key("http2-settings")))),
                request::headers(contains((header::HOST.as_str(), source_host))),
            ])
            .respond_with(
                responders::status_code(302)
                    .append_header(header::LOCATION, destination.url_str("/finish")),
            ),
        );

        let hooks = RecordingHooks {
            destinations: Mutex::new(Vec::new()),
            observed_headers: Mutex::new(Vec::new()),
        };
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("attacker.invalid"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("caller-secret"),
        );
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("caller-api-secret"),
        );
        headers.insert(header::COOKIE, HeaderValue::from_static("caller=secret"));
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("x-caller-connection-secret"),
        );
        headers.insert(
            HeaderName::from_static("x-caller-connection-secret"),
            HeaderValue::from_static("secret"),
        );
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("123"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        client
            .repository_get_with_hooks(
                &source.url_str("/start"),
                headers,
                &RepositoryHttpPolicy::default().with_allow_http(true),
                &hooks,
            )
            .await?;
        assert_eq!(2, hooks.destinations.lock().unwrap().len());
        let observed = hooks.observed_headers.lock().unwrap();
        assert!(observed[0].is_empty(), "caller headers must be sanitized");
        assert!(
            observed[1].is_empty(),
            "cross-origin headers must be cleared before destination hooks"
        );
        Ok(())
    }

    #[test]
    fn https_cross_origin_boundary_clears_every_header() {
        let current: Uri = "https://registry.example/archive".parse().unwrap();
        let next: Uri = "https://mirror.example/archive".parse().unwrap();
        let mut headers = HeaderMap::new();
        for (name, value) in [
            (header::AUTHORIZATION, "secret"),
            (header::HOST, "registry.example"),
            (HeaderName::from_static("x-api-key"), "api-secret"),
            (header::CONNECTION, "x-connection-secret"),
            (
                HeaderName::from_static("x-connection-secret"),
                "connection-secret",
            ),
            (header::CONTENT_LENGTH, "999"),
            (header::TRANSFER_ENCODING, "chunked"),
        ] {
            headers.insert(name, HeaderValue::from_str(value).unwrap());
        }
        apply_redirect_origin_boundary(&current, &next, &mut headers);
        assert!(headers.is_empty());
    }

    struct SameOriginHooks;

    impl RepositoryRequestHooks for SameOriginHooks {
        fn prepare_headers(
            &self,
            uri: &Uri,
            headers: &mut HeaderMap,
        ) -> Result<(), RepositoryRequestHookError> {
            if uri.path() == "/same-start" {
                headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("safe-etag"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn same_origin_redirect_preserves_hook_headers() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/same-start")).respond_with(
                responders::status_code(302).append_header(header::LOCATION, "/same-finish"),
            ),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/same-finish"),
                request::headers(contains((header::IF_NONE_MATCH.as_str(), "safe-etag"))),
            ])
            .respond_with(responders::status_code(200)),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        client
            .repository_get_with_hooks(
                &server.url_str("/same-start"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default().with_allow_http(true),
                &SameOriginHooks,
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn redirect_limit_is_exact() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/one"))
                .respond_with(responders::status_code(302).append_header(header::LOCATION, "/two")),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/two")).respond_with(
                responders::status_code(302).append_header(header::LOCATION, "/three"),
            ),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/three"))
                .times(0)
                .respond_with(responders::status_code(200)),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get(
                &server.url_str("/one"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default()
                    .with_allow_http(true)
                    .with_max_redirects(1),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected redirect limit error"),
        };
        assert!(matches!(
            error,
            HttpError::TooManyRedirects {
                max_redirects: 1,
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn exposes_not_found_and_gone_statuses() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/missing"))
                .respond_with(responders::status_code(404)),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/gone"))
                .respond_with(responders::status_code(410)),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let policy = RepositoryHttpPolicy::default().with_allow_http(true);
        let missing_result = client
            .repository_get(&server.url_str("/missing"), HeaderMap::new(), &policy)
            .await;
        let gone_result = client
            .repository_get(&server.url_str("/gone"), HeaderMap::new(), &policy)
            .await;
        let missing = match missing_result {
            Err(error) => error,
            Ok(_) => panic!("expected not-found error"),
        };
        let gone = match gone_result {
            Err(error) => error,
            Ok(_) => panic!("expected gone error"),
        };
        assert!(missing.is_not_found());
        assert!(gone.is_gone());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_advertised_content_length() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/large"))
                .respond_with(responders::status_code(200).body("too large")),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get(
                &server.url_str("/large"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::new(3).with_allow_http(true),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected oversized response error"),
        };
        assert!(matches!(
            error,
            HttpError::ResponseTooLarge {
                advertised_bytes: Some(_),
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_streamed_body_overrun() {
        let body = futures::stream::iter([
            Ok::<_, hyper::Error>(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ])
        .boxed();
        let error = bounded_body(body, "https://example.com/archive".to_owned(), 5)
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HttpError::ResponseTooLarge {
                advertised_bytes: None,
                ..
            }
        ));
    }

    struct AuthorizationHooks;

    impl RepositoryRequestHooks for AuthorizationHooks {
        fn prepare_headers(
            &self,
            _uri: &Uri,
            headers: &mut HeaderMap,
        ) -> Result<(), RepositoryRequestHookError> {
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer authorization-secret"),
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn formatted_error_redacts_query_and_authorization() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/missing"),
                request::headers(not(contains(key(header::AUTHORIZATION.as_str())))),
            ])
            .respond_with(responders::status_code(404).body("Bearer authorization-secret")),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get_with_hooks(
                &format!("{}?token=secret", server.url_str("/missing")),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default().with_allow_http(true),
                &AuthorizationHooks,
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected HTTP status error"),
        };
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("Bearer"));
        Ok(())
    }

    #[tokio::test]
    async fn redirected_status_error_redacts_location_path() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/redirect")).respond_with(
                responders::status_code(302).append_header(
                    header::LOCATION,
                    "/Bearer-location-secret?token=query-secret",
                ),
            ),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/Bearer-location-secret"))
                .respond_with(responders::status_code(404)),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .build();
        let result = client
            .repository_get(
                &server.url_str("/redirect"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default().with_allow_http(true),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected redirected status error"),
        };
        let rendered = format!("{error:?}");
        assert!(rendered.contains("/<redirected>"));
        assert!(!rendered.contains("location-secret"));
        assert!(!rendered.contains("query-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn redirected_send_error_redacts_location_path() -> buck2_error::Result<()> {
        buck2_certs::certs::maybe_setup_cryptography();
        let unavailable = std::net::TcpListener::bind("127.0.0.1:0")?;
        let unavailable_address = unavailable.local_addr()?;
        drop(unavailable);

        let server = httptest::Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/redirect")).respond_with(
                responders::status_code(302).append_header(
                    header::LOCATION,
                    format!("http://{unavailable_address}/Bearer-send-secret?token=query-secret"),
                ),
            ),
        );
        let client = crate::HttpClientBuilder::https_with_system_roots()
            .await?
            .with_connect_timeout(Some(std::time::Duration::from_secs(1)))
            .build();
        let result = client
            .repository_get(
                &server.url_str("/redirect"),
                HeaderMap::new(),
                &RepositoryHttpPolicy::default().with_allow_http(true),
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected redirected send error"),
        };
        let rendered = format!("{error:?}");
        assert!(rendered.contains("/<redirected>"));
        assert!(!rendered.contains("send-secret"));
        assert!(!rendered.contains("query-secret"));
        Ok(())
    }
}
