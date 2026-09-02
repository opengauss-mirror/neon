use anyhow::Result;
use pageserver_api::controller_api::{PlacementPolicy, TenantCreateRequest};
use pageserver_api::models::{
    SafekeepersInfo, TenantConfig, TimelineCreateRequest, TimelineCreateRequestMode, TimelineInfo,
};
use pageserver_api::shard::{DEFAULT_STRIPE_SIZE, ShardCount, ShardStripeSize, TenantShardId};
use safekeeper_api::PgMajorVersion;
use utils::id::{TenantId, TimelineId};

use super::state::ControlPlaneStateStore;
use super::storage_controller_ops::StorageControllerApi;

pub struct TenantCreateOptions {
    pub tenant_id: Option<TenantId>,
    pub timeline_id: Option<TimelineId>,
    pub branch_name: String,
    pub set_default: bool,
    pub pg_version: PgMajorVersion,
    pub shard_count: u8,
    pub shard_stripe_size: Option<u32>,
    pub placement_policy: Option<PlacementPolicy>,
    pub config: TenantConfig,
}

pub struct TenantCreateOutput {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub branch_name: String,
    pub timeline_info: TimelineInfo,
    pub safekeepers: Option<SafekeepersInfo>,
}

pub async fn create_tenant(
    storage_controller: &dyn StorageControllerApi,
    store: &mut dyn ControlPlaneStateStore,
    opts: TenantCreateOptions,
) -> Result<TenantCreateOutput> {
    let tenant_id = opts.tenant_id.unwrap_or_else(TenantId::generate);
    storage_controller
        .tenant_create(TenantCreateRequest {
            new_tenant_id: TenantShardId::unsharded(tenant_id),
            generation: None,
            shard_parameters: pageserver_api::models::ShardParameters {
                count: ShardCount::new(opts.shard_count),
                stripe_size: opts
                    .shard_stripe_size
                    .map(ShardStripeSize)
                    .unwrap_or(DEFAULT_STRIPE_SIZE),
            },
            placement_policy: opts.placement_policy,
            config: opts.config,
        })
        .await?;

    let timeline_id = opts.timeline_id.unwrap_or_else(TimelineId::generate);
    let timeline_create = storage_controller
        .tenant_timeline_create(
            tenant_id,
            TimelineCreateRequest {
                new_timeline_id: timeline_id,
                mode: TimelineCreateRequestMode::Bootstrap {
                    existing_initdb_timeline_id: None,
                    pg_version: Some(opts.pg_version),
                },
            },
        )
        .await?;
    let timeline_info = timeline_create.timeline_info;

    store.upsert_branch_mapping(opts.branch_name.clone(), tenant_id, timeline_id)?;
    if opts.set_default {
        store.set_default_tenant(tenant_id)?;
    }

    Ok(TenantCreateOutput {
        tenant_id,
        timeline_id,
        branch_name: opts.branch_name,
        timeline_info,
        safekeepers: timeline_create.safekeepers,
    })
}
