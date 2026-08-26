use anyhow::Result;
use serde_json::Value;
use utils::id::{TenantId, TimelineId};

use crate::local_env::LocalEnv;

pub trait ControlPlaneStateStore: Send {
    fn get_default_tenant(&self) -> Result<Option<TenantId>>;
    fn set_default_tenant(&mut self, tenant_id: TenantId) -> Result<()>;

    fn get_branch_mapping(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Result<Option<TimelineId>>;

    fn upsert_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<()>;

    fn upsert_branch_mapping_with_extra(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        _extra: Value,
    ) -> Result<()> {
        self.upsert_branch_mapping(branch_name, tenant_id, timeline_id)
    }
}

impl ControlPlaneStateStore for LocalEnv {
    fn get_default_tenant(&self) -> Result<Option<TenantId>> {
        Ok(self.default_tenant_id)
    }

    fn set_default_tenant(&mut self, tenant_id: TenantId) -> Result<()> {
        self.default_tenant_id = Some(tenant_id);
        Ok(())
    }

    fn get_branch_mapping(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Result<Option<TimelineId>> {
        Ok(self.get_branch_timeline_id(branch_name, tenant_id))
    }

    fn upsert_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<()> {
        self.register_branch_mapping(branch_name, tenant_id, timeline_id)
    }
}
