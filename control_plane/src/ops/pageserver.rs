use anyhow::Result;
use pageserver_api::controller_api::{
    MigrationConfig, NodeDescribeResponse, TenantDescribeResponse, TenantShardMigrateRequest,
    TenantShardMigrateResponse,
};
use pageserver_api::shard::TenantShardId;
use serde::Serialize;
use utils::id::{NodeId, TenantId};

use super::storage_controller_ops::StorageControllerApi;

pub struct PageServerConfigOptions {
    pub service_name: String,
    pub node_id: NodeId,
    pub og_version: String,
    pub storage_controller_http: String,
    pub broker_endpoint: String,
}

#[derive(Debug, Serialize)]
pub struct PageServerRenderedConfig {
    pub identity_toml: String,
    pub pageserver_toml: String,
    pub metadata_json: String,
}

pub fn render_pageserver_config(opts: PageServerConfigOptions) -> Result<PageServerRenderedConfig> {
    let metadata = serde_json::json!({
        "host": opts.service_name,
        "port": 6400,
        "http_host": opts.service_name,
        "http_port": 9898,
        "https_port": null,
        "grpc_host": null,
        "grpc_port": null,
        "availability_zone_id": "local",
    });

    Ok(PageServerRenderedConfig {
        identity_toml: format!("id={}\n", opts.node_id),
        pageserver_toml: [
            format!("broker_endpoint='{}'", opts.broker_endpoint),
            format!("pg_distrib_dir='/usr/local/{}'", opts.og_version),
            "listen_pg_addr='0.0.0.0:6400'".to_string(),
            "listen_http_addr='0.0.0.0:9898'".to_string(),
            "remote_storage={local_path='/data/.neon/shared_remote_storage'}".to_string(),
            format!(
                "control_plane_api='{}/upcall/v1/'",
                opts.storage_controller_http.trim_end_matches('/')
            ),
            "control_plane_emergency_mode=false".to_string(),
            "availability_zone='local'".to_string(),
            "virtual_file_io_mode=\"buffered\"".to_string(),
            "".to_string(),
        ]
        .join("\n"),
        metadata_json: format!("{}\n", serde_json::to_string_pretty(&metadata)?),
    })
}

pub async fn list_pageservers(
    storage_controller: &dyn StorageControllerApi,
) -> Result<Vec<NodeDescribeResponse>> {
    storage_controller.node_list().await
}

pub async fn list_tenants(
    storage_controller: &dyn StorageControllerApi,
    limit: Option<usize>,
) -> Result<Vec<TenantDescribeResponse>> {
    storage_controller.tenant_list(limit).await
}

pub async fn list_tenant_shards(
    storage_controller: &dyn StorageControllerApi,
    tenant_id: Option<TenantId>,
    limit: Option<usize>,
) -> Result<Vec<serde_json::Value>> {
    let tenants = list_tenants(storage_controller, limit).await?;
    let mut rows = Vec::new();
    for tenant in tenants {
        if tenant_id.is_some_and(|wanted| wanted != tenant.tenant_id) {
            continue;
        }
        for shard in tenant.shards {
            let mut row = serde_json::to_value(shard)?;
            row["tenant_id"] = serde_json::json!(tenant.tenant_id);
            rows.push(row);
        }
    }
    Ok(rows)
}

pub async fn drain_pageserver(
    storage_controller: &dyn StorageControllerApi,
    node_id: NodeId,
) -> Result<()> {
    storage_controller.node_drain(node_id).await
}

pub async fn fill_pageserver(
    storage_controller: &dyn StorageControllerApi,
    node_id: NodeId,
) -> Result<()> {
    storage_controller.node_fill(node_id).await
}

pub struct TenantShardMigrateOptions {
    pub tenant_shard_id: TenantShardId,
    pub node_id: NodeId,
    pub origin_node_id: Option<NodeId>,
    pub prewarm: Option<bool>,
    pub override_scheduler: bool,
}

pub async fn migrate_tenant_shard(
    storage_controller: &dyn StorageControllerApi,
    opts: TenantShardMigrateOptions,
) -> Result<TenantShardMigrateResponse> {
    let mut migration_config = MigrationConfig {
        override_scheduler: opts.override_scheduler,
        ..Default::default()
    };
    if let Some(prewarm) = opts.prewarm {
        migration_config.prewarm = prewarm;
    }

    storage_controller
        .tenant_shard_migrate(
            opts.tenant_shard_id,
            TenantShardMigrateRequest {
                node_id: opts.node_id,
                origin_node_id: opts.origin_node_id,
                migration_config,
            },
        )
        .await
}

pub async fn start_pageserver_delete(
    storage_controller: &dyn StorageControllerApi,
    node_id: NodeId,
    force: bool,
) -> Result<()> {
    storage_controller.node_start_delete(node_id, force).await
}
