/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;

use allocative::Allocative;
use derive_more::Display;
use dice::DiceComputations;
use dice::DiceProjectionComputations;
use dice::DiceTransactionUpdater;
use dice::InjectedKey;
use dice::OpaqueValue;
use dice::PagableValueSerialize;
use dice::ProjectionKey;
use dice::ValueSerialize;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::BzlmodConfig;
use crate::BzlmodLocalStoreConfig;
use crate::BzlmodRemoteCacheConfig;
use crate::BzlmodResolutionConfig;
use crate::BzlmodTransportConfig;

#[derive(
    Clone, Copy, Dupe, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable
)]
#[display("BzlmodConfigKey")]
#[pagable_typetag(dice::DiceKeyDyn)]
struct BzlmodConfigKey;

impl InjectedKey for BzlmodConfigKey {
    type Value = BzlmodConfig;

    fn equality(x: &Self::Value, y: &Self::Value) -> bool {
        x == y
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

macro_rules! projection_key {
    ($key:ident, $value:ty, $field:ident, $display:literal) => {
        #[derive(Clone, Copy, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
        #[display($display)]
        #[pagable_typetag(dice::DiceProjectionDyn)]
        struct $key;

        impl ProjectionKey for $key {
            type DeriveFromKey = BzlmodConfigKey;
            type Value = Arc<$value>;

            fn compute(
                &self,
                config: &BzlmodConfig,
                _ctx: &DiceProjectionComputations,
            ) -> Self::Value {
                Arc::new(config.$field().clone())
            }

            fn equality(x: &Self::Value, y: &Self::Value) -> bool {
                x == y
            }

            fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
                PagableValueSerialize::<Self::Value>::new()
            }
        }
    };
}

projection_key!(
    BzlmodResolutionProjectionKey,
    BzlmodResolutionConfig,
    resolution,
    "BzlmodResolutionProjectionKey"
);
projection_key!(
    BzlmodTransportProjectionKey,
    BzlmodTransportConfig,
    transport,
    "BzlmodTransportProjectionKey"
);
projection_key!(
    BzlmodLocalStoreProjectionKey,
    BzlmodLocalStoreConfig,
    local_store,
    "BzlmodLocalStoreProjectionKey"
);
projection_key!(
    BzlmodRemoteCacheProjectionKey,
    BzlmodRemoteCacheConfig,
    remote_cache,
    "BzlmodRemoteCacheProjectionKey"
);

pub trait SetBzlmodConfig {
    fn set_bzlmod_config(&mut self, config: BzlmodConfig) -> buck2_error::Result<()>;
}

impl SetBzlmodConfig for DiceTransactionUpdater {
    fn set_bzlmod_config(&mut self, config: BzlmodConfig) -> buck2_error::Result<()> {
        Ok(self.changed_to([(BzlmodConfigKey, config)])?)
    }
}

#[allow(async_fn_in_trait)]
pub trait HasBzlmodConfig {
    async fn get_bzlmod_resolution_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodResolutionConfig>>;

    async fn get_bzlmod_transport_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodTransportConfig>>;

    async fn get_bzlmod_local_store_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodLocalStoreConfig>>;

    async fn get_bzlmod_remote_cache_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodRemoteCacheConfig>>;
}

impl HasBzlmodConfig for DiceComputations<'_> {
    async fn get_bzlmod_resolution_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodResolutionConfig>> {
        project(self, &BzlmodResolutionProjectionKey).await
    }

    async fn get_bzlmod_transport_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodTransportConfig>> {
        project(self, &BzlmodTransportProjectionKey).await
    }

    async fn get_bzlmod_local_store_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodLocalStoreConfig>> {
        project(self, &BzlmodLocalStoreProjectionKey).await
    }

    async fn get_bzlmod_remote_cache_config(
        &mut self,
    ) -> buck2_error::Result<Arc<BzlmodRemoteCacheConfig>> {
        project(self, &BzlmodRemoteCacheProjectionKey).await
    }
}

async fn project<'d, P>(
    ctx: &mut DiceComputations<'d>,
    projection: &P,
) -> buck2_error::Result<P::Value>
where
    P: ProjectionKey<DeriveFromKey = BzlmodConfigKey>,
{
    let config: OpaqueValue<'d, BzlmodConfigKey> = ctx.compute_opaque(&BzlmodConfigKey).await?;
    Ok(ctx.projection(&config, projection)?)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use async_trait::async_trait;
    use buck2_common::legacy_configs::configs::testing::parse;
    use dice::CancellationContext;
    use dice::DetectCycles;
    use dice::Dice;
    use dice::Key;
    use dice::NoValueSerialize;

    use super::*;
    use crate::ParsedBzlmodConfig;

    fn config(extra: &str) -> BzlmodConfig {
        let text = format!("[buck2]\nstarlark_dialect = bazel\n[bzlmod]\nenabled = true\n{extra}");
        let legacy = parse(&[("config", &text)], "config").unwrap();
        ParsedBzlmodConfig::from_legacy_config(&legacy)
            .unwrap()
            .into_parts()
            .0
    }

    static RESOLUTION_CONSUMER_COMPUTES: AtomicUsize = AtomicUsize::new(0);
    static PROJECTION_CONSUMER_COMPUTES: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];

    #[derive(Clone, Copy, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
    #[display("ResolutionConsumer")]
    #[pagable_typetag(dice::DiceKeyDyn)]
    struct ResolutionConsumer;

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
    enum ProjectionKind {
        Resolution,
        Transport,
        LocalStore,
        RemoteCache,
    }

    #[derive(Clone, Copy, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
    #[display("ProjectionConsumer({:?})", _0)]
    #[pagable_typetag(dice::DiceKeyDyn)]
    struct ProjectionConsumer(ProjectionKind);

    #[async_trait]
    impl Key for ResolutionConsumer {
        type Value = bool;

        async fn compute(
            &self,
            ctx: &mut DiceComputations,
            _cancellations: &CancellationContext,
        ) -> Self::Value {
            RESOLUTION_CONSUMER_COMPUTES.fetch_add(1, Ordering::SeqCst);
            ctx.get_bzlmod_resolution_config().await.unwrap().enabled()
        }

        fn equality(x: &Self::Value, y: &Self::Value) -> bool {
            x == y
        }

        fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
            NoValueSerialize::<Self::Value>::new()
        }
    }

    #[async_trait]
    impl Key for ProjectionConsumer {
        type Value = u64;

        async fn compute(
            &self,
            ctx: &mut DiceComputations,
            _cancellations: &CancellationContext,
        ) -> Self::Value {
            PROJECTION_CONSUMER_COMPUTES[self.0 as usize].fetch_add(1, Ordering::SeqCst);
            match self.0 {
                ProjectionKind::Resolution => {
                    u64::from(ctx.get_bzlmod_resolution_config().await.unwrap().enabled())
                }
                ProjectionKind::Transport => ctx
                    .get_bzlmod_transport_config()
                    .await
                    .unwrap()
                    .max_redirects()
                    .into(),
                ProjectionKind::LocalStore => ctx
                    .get_bzlmod_local_store_config()
                    .await
                    .unwrap()
                    .authorization()
                    .provider_digest()
                    .len() as u64,
                ProjectionKind::RemoteCache => u64::from(
                    ctx.get_bzlmod_remote_cache_config()
                        .await
                        .unwrap()
                        .trust_domain()
                        .is_some(),
                ),
            }
        }

        fn equality(x: &Self::Value, y: &Self::Value) -> bool {
            x == y
        }

        fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
            NoValueSerialize::<Self::Value>::new()
        }
    }

    #[test]
    fn projection_structs_partition_configuration_identity() {
        let baseline = config("");
        let resolution = config("lock_mode = refresh\n");
        assert_ne!(baseline.resolution(), resolution.resolution());
        assert_eq!(baseline.transport(), resolution.transport());
        assert_eq!(baseline.local_store(), resolution.local_store());
        assert_eq!(baseline.remote_cache(), resolution.remote_cache());

        let transport = config("max_redirects = 11\n");
        assert_eq!(baseline.resolution(), transport.resolution());
        assert_ne!(baseline.transport(), transport.transport());
        assert_eq!(baseline.local_store(), transport.local_store());
        assert_eq!(baseline.remote_cache(), transport.remote_cache());

        let local = config("repository_cache = /tmp/bzlmod-a\n");
        assert_eq!(baseline.resolution(), local.resolution());
        assert_eq!(baseline.transport(), local.transport());
        assert_ne!(baseline.local_store(), local.local_store());
        assert_eq!(baseline.remote_cache(), local.remote_cache());

        let remote = config("remote_repository_cache_trust_domain = team\n");
        assert_eq!(baseline.resolution(), remote.resolution());
        assert_eq!(baseline.transport(), remote.transport());
        assert_eq!(baseline.local_store(), remote.local_store());
        assert_ne!(baseline.remote_cache(), remote.remote_cache());

        let helper_a = config("credential_helpers = example.com=/bin/helper-a\n");
        let helper_b = config("credential_helpers = example.com=/bin/helper-b\n");
        assert_eq!(helper_a.resolution(), helper_b.resolution());
        assert_ne!(helper_a.transport(), helper_b.transport());
        assert_ne!(helper_a.local_store(), helper_b.local_store());
        assert_ne!(helper_a.remote_cache(), helper_b.remote_cache());
        assert_eq!(
            helper_a
                .transport()
                .credential_provider()
                .authorization_domains(),
            helper_b
                .transport()
                .credential_provider()
                .authorization_domains()
        );
        assert_ne!(
            helper_a.transport().credential_provider().provider_digest(),
            helper_b.transport().credential_provider().provider_digest()
        );
    }

    #[tokio::test]
    async fn dice_does_not_invalidate_unchanged_projection_consumers() -> buck2_error::Result<()> {
        RESOLUTION_CONSUMER_COMPUTES.store(0, Ordering::SeqCst);
        let dice = Dice::builder().build(DetectCycles::Disabled);
        let mut updater = dice.updater();
        updater.set_bzlmod_config(config(""))?;
        let ctx = updater.commit().await;
        assert!(*ctx.compute(&ResolutionConsumer).await?);
        assert_eq!(RESOLUTION_CONSUMER_COMPUTES.load(Ordering::SeqCst), 1);

        let mut updater = dice.updater();
        updater.set_bzlmod_config(config("max_redirects = 11\n"))?;
        let ctx = updater.commit().await;
        assert!(*ctx.compute(&ResolutionConsumer).await?);
        assert_eq!(RESOLUTION_CONSUMER_COMPUTES.load(Ordering::SeqCst), 1);

        let mut updater = dice.updater();
        updater.set_bzlmod_config(config("lock_mode = refresh\n"))?;
        let ctx = updater.commit().await;
        assert!(*ctx.compute(&ResolutionConsumer).await?);
        assert_eq!(RESOLUTION_CONSUMER_COMPUTES.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn dice_cache_projections_track_authorization_and_trust() -> buck2_error::Result<()> {
        for counter in &PROJECTION_CONSUMER_COMPUTES {
            counter.store(0, Ordering::SeqCst);
        }
        let dice = Dice::builder().build(DetectCycles::Disabled);
        let mut updater = dice.updater();
        updater.set_bzlmod_config(config("credential_helpers = example.com=/bin/helper-a\n"))?;
        let ctx = updater.commit().await;
        for kind in [
            ProjectionKind::Resolution,
            ProjectionKind::Transport,
            ProjectionKind::LocalStore,
            ProjectionKind::RemoteCache,
        ] {
            ctx.compute(&ProjectionConsumer(kind)).await?;
        }
        assert_eq!(projection_counts(), [1, 1, 1, 1]);

        let mut updater = dice.updater();
        updater.set_bzlmod_config(config("credential_helpers = example.com=/bin/helper-b\n"))?;
        let ctx = updater.commit().await;
        for kind in [
            ProjectionKind::Resolution,
            ProjectionKind::Transport,
            ProjectionKind::LocalStore,
            ProjectionKind::RemoteCache,
        ] {
            ctx.compute(&ProjectionConsumer(kind)).await?;
        }
        assert_eq!(projection_counts(), [1, 2, 2, 2]);

        let mut updater = dice.updater();
        updater.set_bzlmod_config(config(
            "credential_helpers = example.com=/bin/helper-b\nremote_repository_cache_trust_domain = team\n",
        ))?;
        let ctx = updater.commit().await;
        for kind in [
            ProjectionKind::Resolution,
            ProjectionKind::Transport,
            ProjectionKind::LocalStore,
            ProjectionKind::RemoteCache,
        ] {
            ctx.compute(&ProjectionConsumer(kind)).await?;
        }
        assert_eq!(projection_counts(), [1, 2, 2, 3]);
        Ok(())
    }

    fn projection_counts() -> [usize; 4] {
        std::array::from_fn(|index| PROJECTION_CONSUMER_COMPUTES[index].load(Ordering::SeqCst))
    }
}
