use anyhow::{Result, anyhow};
use utils::id::{TenantId, TenantTimelineId, TimelineId};

use super::state::ControlPlaneStateStore;

pub fn map_branch(
    store: &mut dyn ControlPlaneStateStore,
    branch_name: String,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<TenantTimelineId> {
    store.upsert_branch_mapping(branch_name, tenant_id, timeline_id)?;
    Ok(TenantTimelineId::new(tenant_id, timeline_id))
}

pub fn resolve_tenant(
    store: &dyn ControlPlaneStateStore,
    tenant_id: Option<TenantId>,
) -> Result<TenantId> {
    tenant_id
        .or(store.get_default_tenant()?)
        .ok_or_else(|| anyhow!("no tenant id specified and default tenant is not set"))
}

pub fn resolve_timeline(
    store: &dyn ControlPlaneStateStore,
    branch_name: &str,
    tenant_id: TenantId,
) -> Result<TimelineId> {
    if let Ok(timeline_id) = branch_name.parse::<TimelineId>() {
        return Ok(timeline_id);
    }

    store
        .get_branch_mapping(branch_name, tenant_id)?
        .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))
}
