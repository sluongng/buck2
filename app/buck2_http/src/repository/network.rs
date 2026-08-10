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
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use http::Uri;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tower_service::Service;

type BoxError = Box<dyn Error + Send + Sync>;

/// The immutable network boundary attached to a repository HTTP client.
///
/// Repository clients accept globally routable unicast destinations by
/// default. Loopback access is available only as an explicit testing/local
/// registry exception, and only when the URI itself names a loopback origin.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RepositoryNetworkPolicy {
    allow_explicit_loopback: bool,
}

impl RepositoryNetworkPolicy {
    pub fn with_allow_explicit_loopback(mut self, allow: bool) -> Self {
        self.allow_explicit_loopback = allow;
        self
    }
}

#[derive(Debug, buck2_error::Error, Eq, PartialEq)]
#[buck2(tag = Http)]
pub enum RepositoryClientBuildError {
    #[error("repository HTTP clients do not support proxies")]
    ProxyUnsupported,
    #[error("repository HTTP clients do not support VPNless or X2P")]
    VpnlessUnsupported,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DestinationClass {
    Public,
    ExplicitLoopback,
}

impl DestinationClass {
    fn for_host(host: &str, policy: &RepositoryNetworkPolicy) -> Self {
        if policy.allow_explicit_loopback && is_explicit_loopback_host(host) {
            Self::ExplicitLoopback
        } else {
            Self::Public
        }
    }

    fn permits(self, ip: IpAddr) -> bool {
        match self {
            Self::Public => is_public_ip(ip),
            Self::ExplicitLoopback => is_loopback_ip(ip),
        }
    }
}

fn static_io_error(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn host_without_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host)
}

fn is_explicit_loopback_host(host: &str) -> bool {
    let host = host_without_brackets(host);
    let host = host.strip_suffix('.').unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host.parse::<IpAddr>().is_ok_and(is_loopback_ip)
}

fn parse_ip_literal(host: &str) -> Result<Option<IpAddr>, io::Error> {
    let host = host_without_brackets(host);
    if host.contains('%') {
        return Err(static_io_error(
            "repository destination uses a scoped IP literal",
        ));
    }
    Ok(host.parse().ok())
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    normalize_ip(ip).is_loopback()
}

fn ipv4_in_prefix(ip: Ipv4Addr, network: Ipv4Addr, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    u32::from(ip) & mask == u32::from(network) & mask
}

fn ipv6_in_prefix(ip: Ipv6Addr, network: Ipv6Addr, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    u128::from(ip) & mask == u128::from(network) & mask
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    const DENIED: &[(Ipv4Addr, u32)] = &[
        (Ipv4Addr::new(0, 0, 0, 0), 8),
        (Ipv4Addr::new(10, 0, 0, 0), 8),
        (Ipv4Addr::new(100, 64, 0, 0), 10),
        (Ipv4Addr::new(127, 0, 0, 0), 8),
        (Ipv4Addr::new(169, 254, 0, 0), 16),
        (Ipv4Addr::new(172, 16, 0, 0), 12),
        (Ipv4Addr::new(192, 0, 0, 0), 24),
        (Ipv4Addr::new(192, 0, 2, 0), 24),
        (Ipv4Addr::new(192, 88, 99, 0), 24),
        (Ipv4Addr::new(192, 168, 0, 0), 16),
        (Ipv4Addr::new(198, 18, 0, 0), 15),
        (Ipv4Addr::new(198, 51, 100, 0), 24),
        (Ipv4Addr::new(203, 0, 113, 0), 24),
        (Ipv4Addr::new(224, 0, 0, 0), 4),
        (Ipv4Addr::new(240, 0, 0, 0), 4),
    ];
    !DENIED
        .iter()
        .any(|(network, prefix)| ipv4_in_prefix(ip, *network, *prefix))
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    const DENIED: &[(Ipv6Addr, u32)] = &[
        (Ipv6Addr::UNSPECIFIED, 128),
        (Ipv6Addr::LOCALHOST, 128),
        (Ipv6Addr::UNSPECIFIED, 96),
        (Ipv6Addr::new(0x0064, 0xff9b, 0, 0, 0, 0, 0, 0), 96),
        (Ipv6Addr::new(0x0064, 0xff9b, 1, 0, 0, 0, 0, 0), 48),
        (Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0), 64),
        (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23),
        (Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32),
        (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16),
        (Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20),
        (Ipv6Addr::new(0x5f00, 0, 0, 0, 0, 0, 0, 0), 16),
        (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
        (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
        (Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10),
        (Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0), 8),
    ];
    // Current globally routable unicast allocations are within 2000::/3.
    ipv6_in_prefix(ip, Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3)
        && !DENIED
            .iter()
            .any(|(network, prefix)| ipv6_in_prefix(ip, *network, *prefix))
}

fn is_public_ip(ip: IpAddr) -> bool {
    match normalize_ip(ip) {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn effective_port(uri: &Uri) -> Result<u16, io::Error> {
    uri.port_u16()
        .or_else(|| match uri.scheme_str() {
            Some("http") => Some(80),
            Some("https") => Some(443),
            _ => None,
        })
        .ok_or_else(|| static_io_error("repository destination has no supported port"))
}

fn validate_uri_before_connect(
    uri: &Uri,
    policy: &RepositoryNetworkPolicy,
) -> Result<(), io::Error> {
    let host = uri
        .host()
        .ok_or_else(|| static_io_error("repository destination has no host"))?;
    let _ = effective_port(uri)?;
    if let Some(ip) = parse_ip_literal(host)? {
        let class = DestinationClass::for_host(host, policy);
        if !class.permits(ip) {
            return Err(static_io_error(
                "repository destination IP address is forbidden",
            ));
        }
    }
    Ok(())
}

fn validate_connected_peer(
    uri: &Uri,
    peer: SocketAddr,
    policy: &RepositoryNetworkPolicy,
) -> Result<(), io::Error> {
    let host = uri
        .host()
        .ok_or_else(|| static_io_error("repository destination has no host"))?;
    if !DestinationClass::for_host(host, policy).permits(peer.ip()) {
        return Err(static_io_error(
            "repository connected peer IP address is forbidden",
        ));
    }
    if peer.port() != effective_port(uri)? {
        return Err(static_io_error(
            "repository connected peer port does not match the destination",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct RepositoryResolver<R> {
    inner: R,
    policy: RepositoryNetworkPolicy,
}

impl<R> RepositoryResolver<R> {
    pub(crate) fn new(inner: R, policy: RepositoryNetworkPolicy) -> Self {
        Self { inner, policy }
    }
}

impl<R> Service<Name> for RepositoryResolver<R>
where
    R: Service<Name> + Send,
    R::Response: Iterator<Item = SocketAddr> + Send + 'static,
    R::Future: Send + 'static,
{
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|_| static_io_error("repository destination resolver is not available"))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let class = DestinationClass::for_host(name.as_str(), &self.policy);
        let future = self.inner.call(name);
        Box::pin(async move {
            let addresses = future
                .await
                .map_err(|_| static_io_error("repository destination resolution failed"))?
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(static_io_error(
                    "repository destination resolved to no addresses",
                ));
            }
            for address in &addresses {
                if address.port() != 0 {
                    return Err(static_io_error(
                        "repository resolver returned an unexpected port",
                    ));
                }
                if !class.permits(address.ip()) {
                    return Err(static_io_error(
                        "repository destination resolution includes a forbidden address",
                    ));
                }
            }
            Ok(addresses.into_iter())
        })
    }
}

trait PeerAddress {
    fn peer_address(&self) -> io::Result<SocketAddr>;
}

impl PeerAddress for TokioIo<TcpStream> {
    fn peer_address(&self) -> io::Result<SocketAddr> {
        self.inner().peer_addr()
    }
}

#[derive(Clone)]
pub(crate) struct PeerCheckedConnector<C> {
    inner: C,
    policy: RepositoryNetworkPolicy,
}

impl<C> PeerCheckedConnector<C> {
    pub(crate) fn new(inner: C, policy: RepositoryNetworkPolicy) -> Self {
        Self { inner, policy }
    }
}

impl<C> Service<Uri> for PeerCheckedConnector<C>
where
    C: Service<Uri> + Send,
    C::Response: PeerAddress + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
{
    type Response = C::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        if let Err(error) = validate_uri_before_connect(&uri, &self.policy) {
            return Box::pin(async move { Err(Box::new(error) as BoxError) });
        }
        let future = self.inner.call(uri.clone());
        let policy = self.policy.clone();
        Box::pin(async move {
            let stream = future.await.map_err(Into::into)?;
            let peer = stream.peer_address().map_err(|_| {
                Box::new(static_io_error(
                    "repository connected peer address is unavailable",
                )) as BoxError
            })?;
            validate_connected_peer(&uri, peer, &policy)
                .map_err(|error| Box::new(error) as BoxError)?;
            // `HttpConnector` opens this socket directly to one of the
            // validated `SocketAddr` values returned above. `peer_addr()` is
            // therefore also an independent check of the selected candidate,
            // including the effective URI port, before TLS starts.
            Ok(stream)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Ready;
    use std::future::ready;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn ipv4_special_range_boundaries() {
        let denied = [
            "0.0.0.0",
            "0.255.255.255",
            "10.0.0.0",
            "10.255.255.255",
            "100.64.0.0",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.255.255",
            "172.16.0.0",
            "172.31.255.255",
            "192.0.0.0",
            "192.0.0.255",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.255.255",
            "198.18.0.0",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.0",
            "255.255.255.255",
        ];
        for value in denied {
            assert!(!is_public_ip(IpAddr::from_str(value).unwrap()), "{value}");
        }
        for value in [
            "1.0.0.1",
            "9.255.255.255",
            "11.0.0.0",
            "100.63.255.255",
            "100.128.0.0",
            "126.255.255.255",
            "128.0.0.0",
            "169.253.255.255",
            "169.255.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.167.255.255",
            "192.169.0.0",
            "198.17.255.255",
            "198.20.0.0",
            "223.255.255.255",
        ] {
            assert!(is_public_ip(IpAddr::from_str(value).unwrap()), "{value}");
        }
    }

    #[test]
    fn ipv6_special_ranges_and_mapped_addresses() {
        for value in [
            "::",
            "::1",
            "64:ff9b::1",
            "64:ff9b:1::1",
            "100::1",
            "2001::1",
            "2001:db8::1",
            "2002::1",
            "3fff::1",
            "5f00::1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff00::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.1.1",
        ] {
            assert!(!is_public_ip(IpAddr::from_str(value).unwrap()), "{value}");
        }
        for value in [
            "2001:4860:4860::8888",
            "2606:4700:4700::1111",
            "::ffff:8.8.8.8",
        ] {
            assert!(is_public_ip(IpAddr::from_str(value).unwrap()), "{value}");
        }
    }

    #[test]
    fn loopback_requires_explicit_policy_and_origin() {
        let strict = RepositoryNetworkPolicy::default();
        let local = strict.clone().with_allow_explicit_loopback(true);
        for host in [
            "localhost",
            "localhost.",
            "repo.localhost",
            "127.0.0.1",
            "::1",
        ] {
            assert_eq!(
                DestinationClass::Public,
                DestinationClass::for_host(host, &strict)
            );
            assert_eq!(
                DestinationClass::ExplicitLoopback,
                DestinationClass::for_host(host, &local)
            );
        }
        assert_eq!(
            DestinationClass::Public,
            DestinationClass::for_host("example.com", &local)
        );
    }

    #[derive(Clone)]
    struct FakeResolver {
        addresses: Vec<SocketAddr>,
    }

    impl Service<Name> for FakeResolver {
        type Response = std::vec::IntoIter<SocketAddr>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _name: Name) -> Self::Future {
            ready(Ok(self.addresses.clone().into_iter()))
        }
    }

    async fn resolve(
        name: &str,
        addresses: &[&str],
        policy: RepositoryNetworkPolicy,
    ) -> io::Result<Vec<SocketAddr>> {
        let mut resolver = RepositoryResolver::new(
            FakeResolver {
                addresses: addresses
                    .iter()
                    .map(|value| value.parse().unwrap())
                    .collect(),
            },
            policy,
        );
        resolver
            .call(name.parse().unwrap())
            .await
            .map(Iterator::collect)
    }

    #[tokio::test]
    async fn resolver_checks_complete_candidate_set() {
        assert_eq!(
            2,
            resolve(
                "example.com",
                &["8.8.8.8:0", "[2001:4860:4860::8888]:0"],
                RepositoryNetworkPolicy::default(),
            )
            .await
            .unwrap()
            .len()
        );
        assert!(
            resolve(
                "example.com",
                &["8.8.8.8:0", "127.0.0.1:0"],
                RepositoryNetworkPolicy::default(),
            )
            .await
            .is_err()
        );
        assert!(
            resolve(
                "example.com",
                &["127.0.0.1:0", "8.8.8.8:0"],
                RepositoryNetworkPolicy::default(),
            )
            .await
            .is_err()
        );
        assert!(
            resolve("example.com", &[], RepositoryNetworkPolicy::default())
                .await
                .is_err()
        );
        assert!(
            resolve(
                "example.com",
                &["8.8.8.8:443"],
                RepositoryNetworkPolicy::default(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn resolver_loopback_exception_is_origin_bound() {
        let local = RepositoryNetworkPolicy::default().with_allow_explicit_loopback(true);
        assert!(
            resolve("localhost", &["127.0.0.1:0"], local.clone())
                .await
                .is_ok()
        );
        assert!(
            resolve("example.com", &["127.0.0.1:0"], local.clone())
                .await
                .is_err()
        );
        assert!(
            resolve("localhost", &["127.0.0.1:0", "8.8.8.8:0"], local,)
                .await
                .is_err()
        );
    }

    #[derive(Clone)]
    struct SecretFailingResolver;

    #[derive(Debug)]
    struct SecretResolverError;

    impl std::fmt::Display for SecretResolverError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("resolver-secret")
        }
    }

    impl Error for SecretResolverError {}

    impl Service<Name> for SecretFailingResolver {
        type Response = std::vec::IntoIter<SocketAddr>;
        type Error = SecretResolverError;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _name: Name) -> Self::Future {
            ready(Err(SecretResolverError))
        }
    }

    #[tokio::test]
    async fn resolver_errors_are_bounded() {
        let mut resolver =
            RepositoryResolver::new(SecretFailingResolver, RepositoryNetworkPolicy::default());
        let error = resolver
            .call("secret-name.example".parse().unwrap())
            .await
            .unwrap_err();
        let rendered = format!("{error:?} {error}");
        assert!(rendered.contains("repository destination resolution failed"));
        assert!(!rendered.contains("resolver-secret"));
        assert!(!rendered.contains("secret-name"));
    }

    #[derive(Clone)]
    struct FakeConnector {
        calls: Arc<AtomicUsize>,
        peer: SocketAddr,
    }

    #[derive(Debug)]
    struct FakeStream(SocketAddr);

    impl PeerAddress for FakeStream {
        fn peer_address(&self) -> io::Result<SocketAddr> {
            Ok(self.0)
        }
    }

    impl Service<Uri> for FakeConnector {
        type Response = FakeStream;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(FakeStream(self.peer)))
        }
    }

    #[tokio::test]
    async fn connector_checks_literals_before_connect_and_peer_after_connect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = FakeConnector {
            calls: calls.clone(),
            peer: "8.8.8.8:443".parse().unwrap(),
        };
        let mut connector = PeerCheckedConnector::new(inner, RepositoryNetworkPolicy::default());
        let forbidden: Uri = "https://127.0.0.1/private?token=secret".parse().unwrap();
        let error = connector.call(forbidden).await.unwrap_err().to_string();
        assert_eq!(0, calls.load(Ordering::SeqCst));
        assert!(!error.contains("secret"));

        let scoped: Uri = "https://[fe80::1%25eth0]/private".parse().unwrap();
        assert!(connector.call(scoped).await.is_err());
        assert_eq!(0, calls.load(Ordering::SeqCst));

        let wrong_peer = FakeConnector {
            calls: calls.clone(),
            peer: "127.0.0.1:443".parse().unwrap(),
        };
        let mut connector =
            PeerCheckedConnector::new(wrong_peer, RepositoryNetworkPolicy::default());
        assert!(
            connector
                .call("https://example.com/archive".parse().unwrap())
                .await
                .is_err()
        );

        let loopback = FakeConnector {
            calls: calls.clone(),
            peer: "127.0.0.1:8080".parse().unwrap(),
        };
        let mut connector = PeerCheckedConnector::new(
            loopback,
            RepositoryNetworkPolicy::default().with_allow_explicit_loopback(true),
        );
        assert!(
            connector
                .call("http://localhost:8080/archive".parse().unwrap())
                .await
                .is_ok()
        );
        assert!(
            connector
                .call("http://localhost.evil:8080/archive".parse().unwrap())
                .await
                .is_err()
        );

        let wrong_port = FakeConnector {
            calls,
            peer: "8.8.8.8:80".parse().unwrap(),
        };
        let mut connector =
            PeerCheckedConnector::new(wrong_port, RepositoryNetworkPolicy::default());
        assert!(
            connector
                .call("https://example.com/archive".parse().unwrap())
                .await
                .is_err()
        );
    }
}
