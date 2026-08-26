use anyhow::Result;
use pageserver_api::models::{
    SafekeepersInfo, TimelineCreateRequest, TimelineCreateRequestMode, TimelineInfo,
};
use safekeeper_api::PgMajorVersion;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use super::state::ControlPlaneStateStore;
use super::storage_controller_ops::StorageControllerApi;

pub struct TimelineCreateOptions {
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub branch_name: String,
    pub pg_version: PgMajorVersion,
}

pub struct TimelineBranchOptions {
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub branch_name: String,
    pub ancestor_timeline_id: TimelineId,
    pub ancestor_start_lsn: Option<Lsn>,
}

pub struct TimelineCreateOutput {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub branch_name: String,
    pub timeline_info: TimelineInfo,
    pub safekeepers: Option<SafekeepersInfo>,
}

pub async fn create_timeline(
    storage_controller: &dyn StorageControllerApi,
    store: &mut dyn ControlPlaneStateStore,
    opts: TimelineCreateOptions,
) -> Result<TimelineCreateOutput> {
    let timeline_id = opts.timeline_id.unwrap_or_else(TimelineId::generate);
    let timeline_create = storage_controller
        .tenant_timeline_create(
            opts.tenant_id,
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

    store.upsert_branch_mapping(opts.branch_name.clone(), opts.tenant_id, timeline_id)?;
    Ok(TimelineCreateOutput {
        tenant_id: opts.tenant_id,
        timeline_id,
        branch_name: opts.branch_name,
        timeline_info,
        safekeepers: timeline_create.safekeepers,
    })
}

pub async fn branch_timeline(
    storage_controller: &dyn StorageControllerApi,
    store: &mut dyn ControlPlaneStateStore,
    opts: TimelineBranchOptions,
) -> Result<TimelineCreateOutput> {
    let timeline_id = opts.timeline_id.unwrap_or_else(TimelineId::generate);
    let timeline_create = storage_controller
        .tenant_timeline_create(
            opts.tenant_id,
            TimelineCreateRequest {
                new_timeline_id: timeline_id,
                mode: TimelineCreateRequestMode::Branch {
                    ancestor_timeline_id: opts.ancestor_timeline_id,
                    ancestor_start_lsn: opts.ancestor_start_lsn,
                    read_only: false,
                    pg_version: None,
                },
            },
        )
        .await?;
    let timeline_info = timeline_create.timeline_info;

    store.upsert_branch_mapping_with_extra(
        opts.branch_name.clone(),
        opts.tenant_id,
        timeline_id,
        serde_json::json!({
            "ancestor_timeline_id": opts.ancestor_timeline_id,
            "ancestor_start_lsn": opts.ancestor_start_lsn,
        }),
    )?;
    Ok(TimelineCreateOutput {
        tenant_id: opts.tenant_id,
        timeline_id,
        branch_name: opts.branch_name,
        timeline_info,
        safekeepers: timeline_create.safekeepers,
    })
}

pub async fn timeline_last_record_lsn(
    storage_controller: &dyn StorageControllerApi,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<Lsn> {
    Ok(storage_controller
        .tenant_timeline_detail(tenant_id, timeline_id)
        .await?
        .last_record_lsn)
}
