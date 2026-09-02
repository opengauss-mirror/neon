use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use compute_api::requests::{COMPUTE_AUDIENCE, ComputeClaims, ComputeClaimsScope};
use control_plane::BranchMergeStrategy;
use control_plane::merge_oggit::{OggitGcParentRequest, OggitGcParentResult, oggit_gc_parent};
use control_plane::ops::branch::{
    BranchConflictResolution, BranchDiffOptions, BranchEndpointRef, BranchMergeOptions,
    BranchResolveOptions, BranchTargetOptions, abort_merge, conflicts, continue_merge, diff_branch,
    merge_branch, merge_status, resolve_conflict,
};
use control_plane::ops::endpoint::{
    EndpointRenderInput, compute_mode_from_options,
    render_endpoint_config as render_endpoint_config_spec,
};
use control_plane::ops::mappings::{map_branch, resolve_tenant, resolve_timeline};
use control_plane::ops::pageserver::{
    PageServerConfigOptions, TenantShardMigrateOptions, drain_pageserver, fill_pageserver,
    list_pageservers, list_tenant_shards, migrate_tenant_shard, render_pageserver_config,
    start_pageserver_delete,
};
use control_plane::ops::state::ControlPlaneStateStore;
use control_plane::ops::storage_controller_ops::{HttpStorageControllerApi, StorageControllerApi};
use control_plane::ops::tenant::{TenantCreateOptions, create_tenant};
use control_plane::ops::timeline::{
    TimelineBranchOptions, TimelineCreateOptions, branch_timeline, create_timeline,
    timeline_last_record_lsn,
};
use endpoint_storage::claims::EndpointStorageClaims;
use futures::StreamExt;
use jsonwebtoken::{DecodingKey, Validation};
use pageserver_api::controller_api::{
    AvailabilityZone, MigrationConfig, NodeAvailabilityWrapper, NodeConfigureRequest,
    NodeSchedulingPolicy, PlacementPolicy, SafekeeperSchedulingPolicyRequest,
    ShardSchedulingPolicy, ShardsPreferredAzsRequest, SkSchedulingPolicy, TenantDescribeResponse,
    TenantPolicyRequest, TenantShardMigrateRequest, TimelineSafekeeperMigrateRequest,
};
use pageserver_api::models::{
    SafekeepersInfo, TenantConfig, TenantConfigRequest, TenantShardSplitRequest, TimelineInfo,
};
use pageserver_api::shard::{ShardStripeSize, TenantShardId};
use pageserver_client::mgmt_api::ResponseErrorMessageExt;
use reqwest::Client;
use safekeeper_api::membership::{Configuration, SafekeeperGeneration, SafekeeperId};
use safekeeper_api::models::{
    TimelineCreateRequest as SafekeeperTimelineCreateRequest,
    TimelineLocateResponse as SafekeeperTimelineLocateResponse,
};
use safekeeper_api::{PgMajorVersion, PgVersionId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_opengauss::NoTls;
use utils::auth::{Claims, Scope, encode_from_key_file};
use utils::id::{EndpointId, NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

const BASE_COMPUTE_CONFIG: &str = include_str!("../../../compute/gaussdb/configs/config.json");
const DOCKER_DEV_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
"#;
const DOCKER_DEV_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
"#;

#[derive(Clone)]
struct AppState {
    state_dir: PathBuf,
    storage_controller_url: String,
    host_storage_controller_url: String,
    compose_project: String,
    og_version: String,
    storage_image: String,
    compute_image: String,
    client: Client,
}

struct DockerStateStore<'a> {
    state_dir: &'a Path,
}

impl<'a> DockerStateStore<'a> {
    fn new(state_dir: &'a Path) -> Self {
        Self { state_dir }
    }
}

fn docker_storage_controller_api(state: &AppState) -> Result<HttpStorageControllerApi> {
    Ok(HttpStorageControllerApi::new(
        state
            .storage_controller_url
            .parse()
            .context("parsing storage_controller_url")?,
        state.client.clone(),
    ))
}

impl ControlPlaneStateStore for DockerStateStore<'_> {
    fn get_default_tenant(&self) -> Result<Option<TenantId>> {
        let path = self.state_dir.join("env.json");
        if !path.exists() {
            return Ok(None);
        }
        let file =
            std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let env: Value = serde_json::from_reader(file)
            .with_context(|| format!("decoding {}", path.display()))?;
        env.get("default_tenant_id")
            .and_then(Value::as_str)
            .map(TenantId::from_str)
            .transpose()
            .with_context(|| format!("parsing default_tenant_id from {}", path.display()))
    }

    fn set_default_tenant(&mut self, tenant_id: TenantId) -> Result<()> {
        let path = self.state_dir.join("env.json");
        let mut env = if path.exists() {
            let file = std::fs::File::open(&path)
                .with_context(|| format!("opening {}", path.display()))?;
            serde_json::from_reader(file).with_context(|| format!("decoding {}", path.display()))?
        } else {
            json!({})
        };
        env["default_tenant_id"] = Value::String(tenant_id.to_string());
        write_json_file(&path, &env)
    }

    fn get_branch_mapping(
        &self,
        branch_name: &str,
        tenant_id: TenantId,
    ) -> Result<Option<TimelineId>> {
        if let Ok(timeline_id) = branch_name.parse::<TimelineId>() {
            return Ok(Some(timeline_id));
        }
        let mappings = load_mappings(self.state_dir)?;
        let Some(mapping) = mappings.get(branch_name) else {
            return Ok(None);
        };
        let Some(mapped_tenant_id) = mapping.get("tenant_id").and_then(Value::as_str) else {
            return Ok(None);
        };
        if mapped_tenant_id != tenant_id.to_string() {
            return Ok(None);
        }
        mapping
            .get("timeline_id")
            .and_then(Value::as_str)
            .map(TimelineId::from_str)
            .transpose()
            .with_context(|| format!("parsing timeline_id for branch {branch_name}"))
    }

    fn upsert_branch_mapping(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<()> {
        upsert_mapping(
            self.state_dir,
            &branch_name,
            &tenant_id.to_string(),
            &timeline_id.to_string(),
        )?;
        Ok(())
    }

    fn upsert_branch_mapping_with_extra(
        &mut self,
        branch_name: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        extra: Value,
    ) -> Result<()> {
        upsert_mapping_with_extra(
            self.state_dir,
            &branch_name,
            &tenant_id.to_string(),
            &timeline_id.to_string(),
            extra,
        )?;
        Ok(())
    }
}

fn docker_branch_tenant_id(state_dir: &Path, branch_name: &str) -> Result<Option<TenantId>> {
    let mappings = load_mappings(state_dir)?;
    mappings
        .get(branch_name)
        .and_then(|mapping| mapping.get("tenant_id"))
        .and_then(Value::as_str)
        .map(TenantId::from_str)
        .transpose()
        .with_context(|| format!("parsing tenant_id for branch {branch_name}"))
}

#[derive(Debug, Deserialize)]
struct NotifyAttachRequest {
    tenant_id: String,
    stripe_size: Option<usize>,
    shards: Vec<NotifyAttachRequestShard>,
}

#[derive(Debug, Deserialize)]
struct NotifyAttachRequestShard {
    node_id: u64,
    shard_number: u64,
}

#[derive(Debug, Deserialize)]
struct NotifySafekeepersRequest {
    tenant_id: String,
    timeline_id: String,
    generation: u32,
    safekeepers: Vec<SafekeeperInfo>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct SafekeeperInfo {
    id: u64,
    hostname: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NodeDescribeResponse {
    id: u64,
    listen_http_addr: String,
    listen_http_port: u16,
    listen_pg_addr: String,
    listen_pg_port: u16,
    listen_grpc_addr: Option<String>,
    listen_grpc_port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct EndpointRecord {
    endpoint_id: String,
    tenant_id: String,
    timeline_id: String,
    #[serde(default = "default_branch_name")]
    branch_name: String,
    service_name: String,
    #[serde(default)]
    container_name: Option<String>,
    compute_http: String,
    #[serde(default)]
    compute_pg: Option<String>,
    #[serde(default)]
    host_pg_port: Option<u16>,
    #[serde(default)]
    host_http_port: Option<u16>,
    #[serde(default)]
    internal_http_port: Option<u16>,
    #[serde(default)]
    endpoint_pageserver_id: Option<u64>,
    #[serde(default)]
    data_dir: Option<String>,
    config_path: String,
    #[serde(default)]
    postgresql_conf_path: Option<String>,
    #[serde(default)]
    last_lsn: Option<String>,
    #[serde(default)]
    static_lsn: Option<String>,
    #[serde(default)]
    hot_standby: bool,
    #[serde(default)]
    autoprewarm: bool,
    #[serde(default)]
    offload_lfc_interval_seconds: Option<u64>,
    #[serde(default = "default_pg_version")]
    pg_version: PgMajorVersion,
    #[serde(default)]
    grpc: bool,
    #[serde(default)]
    config_only: bool,
    #[serde(default)]
    enable_oggit: bool,
    #[serde(default = "default_oggit_database")]
    oggit_database: String,
    #[serde(default)]
    skip_pg_catalog_updates: bool,
    #[serde(default)]
    create_test_user: bool,
    #[serde(default)]
    remote_ext_base_url: Option<String>,
    #[serde(default)]
    privileged_role_name: Option<String>,
    #[serde(default)]
    safekeepers_generation: Option<u32>,
    #[serde(default)]
    safekeeper_connstrings: Option<Vec<String>>,
    #[serde(default)]
    extra_config: Option<Value>,
    #[serde(default)]
    auth_token: Option<String>,
    #[serde(default)]
    endpoint_storage_addr: Option<String>,
    #[serde(default)]
    endpoint_storage_token: Option<String>,
    status: String,
}

#[derive(Debug, Deserialize)]
struct OggitGcRequest {
    #[serde(default = "default_oggit_gc_retention_lsn_distance")]
    retention_lsn_distance: u64,
}

fn default_oggit_gc_retention_lsn_distance() -> u64 {
    16 * 1024 * 1024
}

fn default_oggit_database() -> String {
    "postgres".to_string()
}

#[derive(Debug, Serialize)]
struct NotifyResponse {
    reconfigured: Vec<String>,
    skipped: usize,
}

#[derive(Debug, Deserialize, Default)]
struct InitRequest {
    compose_project: Option<String>,
    og_version: Option<String>,
    storage_image: Option<String>,
    compute_image: Option<String>,
    num_pageservers: Option<u16>,
    num_safekeepers: Option<u16>,
}

#[derive(Debug, Serialize)]
struct PlanResponse {
    status: String,
    compose_files: Vec<String>,
    services: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EndpointCreateRequest {
    endpoint_id: String,
    tenant_id: Option<String>,
    timeline_id: Option<String>,
    branch_name: Option<String>,
    host_pg_port: Option<u16>,
    host_http_port: Option<u16>,
    internal_http_port: Option<u16>,
    endpoint_pageserver_id: Option<u64>,
    service_name: Option<String>,
    static_lsn: Option<String>,
    hot_standby: Option<bool>,
    autoprewarm: Option<bool>,
    offload_lfc_interval_seconds: Option<u64>,
    pg_version: Option<PgMajorVersion>,
    grpc: Option<bool>,
    config_only: Option<bool>,
    enable_oggit: Option<bool>,
    oggit_database: Option<String>,
    update_catalog: Option<bool>,
    create_test_user: Option<bool>,
    remote_ext_base_url: Option<String>,
    privileged_role_name: Option<String>,
    safekeepers_generation: Option<u32>,
    safekeeper_connstrings: Option<Vec<String>>,
    extra_config: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct EndpointStopRequest {
    last_lsn: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct EndpointGenerateJwtQuery {
    scope: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TenantCreateRequest {
    tenant_id: Option<String>,
    timeline_id: Option<String>,
    branch_name: Option<String>,
    set_default: Option<bool>,
    pg_version: Option<PgMajorVersion>,
    shard_count: Option<u8>,
    shard_stripe_size: Option<u32>,
    placement_policy: Option<PlacementPolicy>,
    config: Option<TenantConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct TenantConfigSetRequest {
    config: Option<TenantConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct TenantPolicySetRequest {
    placement: Option<String>,
    scheduling: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TenantShardSplitDockerRequest {
    shard_count: u8,
    stripe_size: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct TenantPreferredAzRequest {
    preferred_az: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TimelineListQuery {
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TimelineBranchRequest {
    tenant_id: Option<String>,
    branch_name: String,
    ancestor_timeline_id: Option<String>,
    ancestor_start_lsn: Option<String>,
    timeline_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TimelineCreateRequest {
    tenant_id: Option<String>,
    branch_name: String,
    timeline_id: Option<String>,
    pg_version: Option<PgMajorVersion>,
}

#[derive(Debug, Deserialize, Default)]
struct TimelineImportRequest {
    tenant_id: String,
    timeline_id: String,
    branch_name: String,
    end_lsn: String,
    pg_version: Option<PgMajorVersion>,
    safekeepers: Option<Vec<u64>>,
    safekeepers_generation: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
struct BranchRunRequest {
    source_endpoint: Option<String>,
    target_endpoint: Option<String>,
    database: String,
    source_schema: Option<String>,
    target_schema: Option<String>,
    strategy: Option<String>,
    incremental_oggit: Option<bool>,
    keep_fdw: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct BranchTargetRequest {
    tenant_id: Option<String>,
    target_branch: Option<String>,
    target_endpoint: Option<String>,
    target_connstr: Option<String>,
    resolution: Option<String>,
    conflict_id: Option<i64>,
    custom_sql: Option<String>,
    database: Option<String>,
    user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BranchTargetQuery {
    tenant_id: Option<String>,
    target_branch: Option<String>,
    target_endpoint: Option<String>,
    target_connstr: Option<String>,
    database: Option<String>,
    user: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerShardsQuery {
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerMigrateRequest {
    node_id: u64,
    origin_node_id: Option<u64>,
    prewarm: Option<bool>,
    override_scheduler: Option<bool>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerCancelRequest {
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerBulkMigrateRequest {
    nodes: Vec<u64>,
    concurrency: Option<usize>,
    max_shards: Option<usize>,
    dry_run: Option<bool>,
    override_scheduler: Option<bool>,
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerDeleteRequest {
    force: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct PageServerAddRequest {
    service_name: String,
    node_id: u64,
    host_http_port: u16,
    host_pg_port: u16,
    og_version: Option<String>,
    storage_image: Option<String>,
    storage_controller_http: Option<String>,
    broker_endpoint: Option<String>,
}

#[derive(Debug, Serialize)]
struct PageServerAddResponse {
    status: String,
    service_name: String,
    node_id: u64,
    host_http: String,
    host_pg: String,
    override_file: String,
    compose_files: Vec<String>,
    services: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SafekeeperSchedulingRequest {
    scheduling_policy: String,
}

#[derive(Debug, Deserialize, Default)]
struct SafekeeperTimelineMigrateRequest {
    tenant_id: String,
    timeline_id: String,
    new_sk_set: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen = std::env::var("DOCKER_CONTROL_PLANE_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse::<SocketAddr>()
        .context("invalid DOCKER_CONTROL_PLANE_LISTEN")?;
    let state_dir = PathBuf::from(
        std::env::var("DOCKER_CONTROL_PLANE_STATE_DIR")
            .unwrap_or_else(|_| "/data/.neon/control_plane".to_string()),
    );
    let storage_controller_url = std::env::var("STORAGE_CONTROLLER_HTTP")
        .unwrap_or_else(|_| "http://storage_controller:1234".to_string());
    let host_storage_controller_url = std::env::var("HOST_STORAGE_CONTROLLER_HTTP")
        .or_else(|_| std::env::var("STORAGE_CONTROLLER_HOST_HTTP"))
        .unwrap_or_else(|_| "http://127.0.0.1:1234".to_string());
    let compose_project =
        std::env::var("COMPOSE_PROJECT_NAME").unwrap_or_else(|_| "neon_poc".to_string());
    let og_version = std::env::var("OG_VERSION").unwrap_or_else(|_| "V702".to_string());
    let storage_image =
        std::env::var("NEON_IMAGE").unwrap_or_else(|_| "og_storage:latest".to_string());
    let compute_image =
        std::env::var("COMPUTE_IMAGE").unwrap_or_else(|_| "og_compute:latest".to_string());

    for dir in [
        state_dir.join("endpoints"),
        state_dir.join("overrides/endpoints"),
    ] {
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    }

    let state = Arc::new(AppState {
        state_dir,
        storage_controller_url,
        host_storage_controller_url,
        compose_project,
        og_version,
        storage_image,
        compute_image,
        client: Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("building HTTP client")?,
    });

    let app = Router::new()
        .route("/ready", get(ready))
        .route("/status", get(status))
        .route("/v1/init", post(init_env))
        .route("/v1/env", get(env))
        .route("/v1/status", get(status))
        .route("/v1/start", post(start_plan))
        .route("/v1/stop", post(stop_plan))
        .route("/v1/endpoint", get(endpoint_list).post(endpoint_create))
        .route(
            "/v1/endpoint/{endpoint_id}",
            get(endpoint_get).delete(endpoint_delete),
        )
        .route("/v1/endpoint/{endpoint_id}/start", post(endpoint_start))
        .route("/v1/endpoint/{endpoint_id}/stop", post(endpoint_stop))
        .route(
            "/v1/endpoint/{endpoint_id}/reconfigure",
            post(endpoint_reconfigure),
        )
        .route(
            "/v1/endpoint/{endpoint_id}/refresh-configuration",
            post(endpoint_reconfigure),
        )
        .route(
            "/v1/endpoint/{endpoint_id}/update-pageservers",
            post(endpoint_reconfigure),
        )
        .route(
            "/v1/endpoint/{endpoint_id}/generate-jwt",
            post(endpoint_generate_jwt),
        )
        .route("/v1/tenant", get(tenant_list).post(tenant_create))
        .route(
            "/v1/tenant/{tenant_id}",
            get(tenant_describe).delete(tenant_delete),
        )
        .route("/v1/tenant/{tenant_id}/locate", get(tenant_locate))
        .route(
            "/v1/tenant/{tenant_id}/set-default",
            post(tenant_set_default),
        )
        .route("/v1/tenant/{tenant_id}/config", put(tenant_config_set))
        .route("/v1/tenant/{tenant_id}/import", post(tenant_import))
        .route("/v1/tenant/{tenant_id}/policy", put(tenant_policy_set))
        .route(
            "/v1/tenant/{tenant_id}/shard-split",
            post(tenant_shard_split),
        )
        .route(
            "/v1/tenant/{tenant_id}/preferred-az",
            put(tenant_set_preferred_az),
        )
        .route("/v1/timeline", get(timeline_list).post(timeline_create))
        .route(
            "/v1/timeline/{tenant_id}/{timeline_id}",
            delete(timeline_delete),
        )
        .route("/v1/timeline/branch", post(timeline_branch))
        .route("/v1/timeline/import", post(timeline_import))
        .route("/v1/mappings", get(mappings_list))
        .route("/v1/mappings/map", post(mapping_map))
        .route("/v1/pageserver", get(pageserver_list).post(pageserver_add))
        .route("/v1/pageserver/shards", get(pageserver_shards))
        .route("/v1/pageserver/bulk-migrate", post(pageserver_bulk_migrate))
        .route("/v1/pageserver/{node_id}/drain", post(pageserver_drain))
        .route(
            "/v1/pageserver/{node_id}/cancel-drain",
            post(pageserver_cancel_drain),
        )
        .route("/v1/pageserver/{node_id}/fill", post(pageserver_fill))
        .route(
            "/v1/pageserver/{node_id}/cancel-fill",
            post(pageserver_cancel_fill),
        )
        .route("/v1/pageserver/{node_id}/delete", post(pageserver_delete))
        .route(
            "/v1/pageserver/{tenant_shard_id}/migrate",
            post(pageserver_migrate),
        )
        .route(
            "/v1/pageserver/{tenant_shard_id}/migrate-secondary",
            post(pageserver_migrate_secondary),
        )
        .route(
            "/v1/pageserver/{tenant_shard_id}/cancel-reconcile",
            post(pageserver_cancel_reconcile),
        )
        .route("/v1/safekeeper", get(safekeeper_list))
        .route(
            "/v1/safekeeper/{node_id}/scheduling",
            put(safekeeper_scheduling),
        )
        .route(
            "/v1/safekeeper/timeline-migrate",
            post(safekeeper_timeline_migrate),
        )
        .route("/v1/branch/diff", post(branch_diff))
        .route("/v1/oggit/gc", post(oggit_gc))
        .route("/v1/branch/merge", post(branch_merge))
        .route("/v1/branch/merge/{merge_id}/status", get(branch_status))
        .route(
            "/v1/branch/merge/{merge_id}/conflicts",
            get(branch_conflicts),
        )
        .route("/v1/branch/merge/{merge_id}/resolve", post(branch_resolve))
        .route(
            "/v1/branch/merge/{merge_id}/continue",
            post(branch_continue),
        )
        .route("/v1/branch/merge/{merge_id}/abort", post(branch_abort))
        .route("/notify-attach", put(notify_attach))
        .route("/notify-safekeepers", put(notify_safekeepers))
        .with_state(state);

    println!("docker_control_plane listening on {listen}");
    axum::serve(TcpListener::bind(listen).await?, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("registering SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("registering SIGINT handler");
    let mut sigquit = signal(SignalKind::quit()).expect("registering SIGQUIT handler");

    tokio::select! {
        _ = sigterm.recv() => eprintln!("docker_control_plane received SIGTERM, shutting down"),
        _ = sigint.recv() => eprintln!("docker_control_plane received SIGINT, shutting down"),
        _ = sigquit.recv() => eprintln!("docker_control_plane received SIGQUIT, shutting down"),
    }
}

async fn ready() -> StatusCode {
    StatusCode::OK
}

async fn status(State(state): State<Arc<AppState>>) -> Response {
    match load_status(&state).await {
        Ok(status) => Json(status).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn init_env(State(state): State<Arc<AppState>>, Json(req): Json<InitRequest>) -> Response {
    match write_env(&state, req) {
        Ok(env) => (StatusCode::OK, Json(env)).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn env(State(state): State<Arc<AppState>>) -> Response {
    match read_or_write_default_env(&state) {
        Ok(env) => Json(env).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn start_plan(State(state): State<Arc<AppState>>) -> Response {
    let env = match read_or_write_default_env(&state) {
        Ok(env) => env,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let mut services = vec![
        "storage_broker".to_string(),
        "storage_controller".to_string(),
        "pageserver".to_string(),
    ];
    let num_safekeepers = env_u64(&env, "num_safekeepers", 1).max(1);
    services.extend((1..=num_safekeepers).map(docker_safekeeper_service_name));
    services.push("endpoint_storage".to_string());
    Json(PlanResponse {
        status: "planned".to_string(),
        compose_files: vec![
            "docker-compose.yml".to_string(),
            ".neon/control_plane/overrides/runtime.yml".to_string(),
        ],
        services,
    })
    .into_response()
}

async fn stop_plan(State(state): State<Arc<AppState>>) -> Response {
    match load_endpoints(&state.state_dir) {
        Ok(endpoints) => {
            let mut services = endpoints
                .into_iter()
                .filter(|endpoint| endpoint.status == "Running")
                .map(|endpoint| endpoint.service_name)
                .collect::<Vec<_>>();
            let env = read_or_write_default_env(&state).unwrap_or_else(|_| json!({}));
            services.push("endpoint_storage".to_string());
            services.push("pageserver".to_string());
            let num_safekeepers = env_u64(&env, "num_safekeepers", 1).max(1);
            services.extend((1..=num_safekeepers).map(docker_safekeeper_service_name));
            services.push("storage_controller".to_string());
            services.push("storage_broker".to_string());
            services.push("storage_controller_db".to_string());
            Json(PlanResponse {
                status: "planned".to_string(),
                compose_files: vec!["docker-compose.yml".to_string()],
                services,
            })
            .into_response()
        }
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_list(State(state): State<Arc<AppState>>) -> Response {
    match load_endpoints(&state.state_dir) {
        Ok(endpoints) => Json(json!({ "endpoints": endpoints })).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_get(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
) -> Response {
    match load_endpoint(&state.state_dir, &endpoint_id) {
        Ok(endpoint) => Json(endpoint).into_response(),
        Err(err) => error_response(StatusCode::NOT_FOUND, err),
    }
}

async fn endpoint_delete(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
) -> Response {
    match delete_endpoint_record(&state.state_dir, &endpoint_id) {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<EndpointCreateRequest>,
) -> Response {
    match handle_endpoint_create(&state, req).await {
        Ok(endpoint) => (StatusCode::CREATED, Json(endpoint)).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_start(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
) -> Response {
    match handle_endpoint_start(&state, &endpoint_id).await {
        Ok(plan) => Json(plan).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_stop(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
    Json(req): Json<EndpointStopRequest>,
) -> Response {
    match update_endpoint_status(&state.state_dir, &endpoint_id, "Stopped", req.last_lsn) {
        Ok(endpoint) => Json(endpoint).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_reconfigure(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
) -> Response {
    match handle_endpoint_reconfigure(&state, &endpoint_id).await {
        Ok(endpoint) => Json(endpoint).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn endpoint_generate_jwt(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
    Query(query): Query<EndpointGenerateJwtQuery>,
) -> Response {
    match load_endpoint(&state.state_dir, &endpoint_id).and_then(|endpoint| {
        let scope = query
            .scope
            .as_deref()
            .map(parse_compute_claims_scope)
            .transpose()
            .context("parsing JWT scope")?;
        let compute_token = generate_compute_token(&state.state_dir, &endpoint, None)?;
        let compute_admin_token = generate_compute_token(
            &state.state_dir,
            &endpoint,
            Some(ComputeClaimsScope::Admin),
        )?;
        let storage_auth_token = generate_storage_auth_token(&state.state_dir, &endpoint)?;
        let endpoint_storage_token =
            generate_endpoint_storage_token(&state.state_dir, &endpoint, Duration::from_secs(86400))?;
        if query.scope.is_some() {
            let token = generate_compute_token(&state.state_dir, &endpoint, scope)?;
            return Ok(json!({
                "endpoint_id": endpoint_id,
                "scope": query.scope,
                "token_type": "EdDSA",
                "jwt": token
            }));
        }
        Ok(json!({
            "endpoint_id": endpoint_id,
            "token_type": "EdDSA",
            "compute_token": compute_token,
            "compute_admin_token": compute_admin_token,
            "storage_auth_token": storage_auth_token,
            "endpoint_storage_token": endpoint_storage_token,
            "endpoint_storage_addr": endpoint.endpoint_storage_addr.unwrap_or_else(|| "endpoint_storage:9993".to_string())
        }))
    }) {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

fn parse_compute_claims_scope(scope: &str) -> Result<ComputeClaimsScope> {
    match scope {
        "admin" => Ok(ComputeClaimsScope::Admin),
        other => ComputeClaimsScope::from_str(other),
    }
}

async fn tenant_list(State(state): State<Arc<AppState>>) -> Response {
    match proxy_get_json(&state, "/control/v1/tenant?limit=10000").await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TenantCreateRequest>,
) -> Response {
    match handle_tenant_create(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn tenant_describe(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller.tenant_describe(tenant_id).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_delete(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller
        .tenant_delete(tenant_id)
        .await
        .and_then(|_| cleanup_deleted_tenant_state(&state.state_dir, tenant_id))
    {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_locate(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
) -> Response {
    match proxy_get_json(&state, &format!("/debug/v1/tenant/{tenant_id}/locate")).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_set_default(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let mut store = DockerStateStore::new(&state.state_dir);
    match store
        .set_default_tenant(tenant_id)
        .and_then(|_| read_or_write_default_env(&state))
    {
        Ok(env) => Json(env).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn tenant_config_set(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
    Json(req): Json<TenantConfigSetRequest>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let config_req = TenantConfigRequest {
        tenant_id,
        config: req.config.unwrap_or_default(),
    };
    match storage_controller.set_tenant_config(&config_req).await {
        Ok(()) => {
            Json(json!({ "tenant_id": tenant_id, "config": config_req.config })).into_response()
        }
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_import(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller.tenant_import(tenant_id).await {
        Ok(response) => Json(json!({ "tenant_id": tenant_id, "import": response })).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_policy_set(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
    Json(req): Json<TenantPolicySetRequest>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let placement = match req
        .placement
        .as_deref()
        .map(parse_placement_policy)
        .transpose()
    {
        Ok(placement) => placement,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let scheduling = match req
        .scheduling
        .as_deref()
        .map(parse_shard_scheduling_policy)
        .transpose()
    {
        Ok(scheduling) => scheduling,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller
        .tenant_policy(
            tenant_id,
            TenantPolicyRequest {
                placement,
                scheduling,
            },
        )
        .await
    {
        Ok(()) => match storage_controller.tenant_describe(tenant_id).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_shard_split(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
    Json(req): Json<TenantShardSplitDockerRequest>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller
        .tenant_shard_split(
            tenant_id,
            TenantShardSplitRequest {
                new_shard_count: req.shard_count,
                new_stripe_size: req.stripe_size.map(ShardStripeSize),
            },
        )
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn tenant_set_preferred_az(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_id): AxumPath<String>,
    Json(req): Json<TenantPreferredAzRequest>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match set_tenant_preferred_az(&storage_controller, tenant_id, req.preferred_az).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn timeline_list(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TimelineListQuery>,
) -> Response {
    match handle_timeline_list(&state, query).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn timeline_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TimelineCreateRequest>,
) -> Response {
    match handle_timeline_create(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn timeline_delete(
    State(state): State<Arc<AppState>>,
    AxumPath((tenant_id, timeline_id)): AxumPath<(String, String)>,
) -> Response {
    let tenant_id = match TenantId::from_str(&tenant_id).context("parsing tenant_id") {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let timeline_id = match TimelineId::from_str(&timeline_id).context("parsing timeline_id") {
        Ok(timeline_id) => timeline_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    match handle_timeline_delete(&state, tenant_id, timeline_id).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn timeline_branch(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TimelineBranchRequest>,
) -> Response {
    match handle_timeline_branch(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn timeline_import(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TimelineImportRequest>,
) -> Response {
    match handle_timeline_import(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn mappings_list(State(state): State<Arc<AppState>>) -> Response {
    match load_mappings(&state.state_dir) {
        Ok(mappings) => Json(mappings).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn mapping_map(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TimelineBranchRequest>,
) -> Response {
    let tenant_id = match req
        .tenant_id
        .as_deref()
        .map(TenantId::from_str)
        .transpose()
        .context("parsing tenant_id")
    {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                anyhow!("tenant_id is required for mapping"),
            );
        }
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let timeline_id = match req
        .timeline_id
        .as_deref()
        .map(TimelineId::from_str)
        .transpose()
        .context("parsing timeline_id")
    {
        Ok(Some(timeline_id)) => timeline_id,
        Ok(None) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                anyhow!("timeline_id is required for mapping"),
            );
        }
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let mut store = DockerStateStore::new(&state.state_dir);
    match map_branch(&mut store, req.branch_name, tenant_id, timeline_id)
        .and_then(|_| load_mappings(&state.state_dir))
    {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn handle_timeline_list(state: &AppState, query: TimelineListQuery) -> Result<Value> {
    let mappings = load_mappings(&state.state_dir)?;
    let storage_controller = docker_storage_controller_api(state)?;
    let nodes = load_pageserver_nodes(state).await?;
    let tenants = storage_controller.tenant_list(Some(10000)).await?;
    let requested_tenant_id = query
        .tenant_id
        .as_deref()
        .map(TenantId::from_str)
        .transpose()
        .context("parsing tenant_id")?;
    let mut rows = Vec::new();

    for tenant in tenants {
        if requested_tenant_id.is_some_and(|wanted| wanted != tenant.tenant_id) {
            continue;
        }
        for shard in tenant.shards {
            let Some(node_id) = shard.node_attached else {
                continue;
            };
            let Some(node) = nodes.get(&node_id.0) else {
                continue;
            };
            let timelines = list_pageserver_timelines(state, node, shard.tenant_shard_id).await?;
            for timeline in timelines {
                let mut value = serde_json::to_value(&timeline)?;
                if let Some(branch_name) = mapping_name_for_timeline(
                    &mappings,
                    timeline.tenant_id.tenant_id,
                    timeline.timeline_id,
                ) {
                    value["branch_name"] = Value::String(branch_name);
                }
                value["pageserver_id"] = Value::String(node_id.to_string());
                rows.push(value);
            }
        }
    }

    Ok(json!({
        "timelines": rows,
        "mappings": mappings,
    }))
}

async fn list_pageserver_timelines(
    state: &AppState,
    node: &NodeDescribeResponse,
    tenant_shard_id: TenantShardId,
) -> Result<Vec<TimelineInfo>> {
    let url = format!(
        "http://{}:{}/v1/tenant/{}/timeline",
        node.listen_http_addr, node.listen_http_port, tenant_shard_id
    );
    state
        .client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<TimelineInfo>>()
        .await
        .with_context(|| format!("decoding pageserver timeline list from {url}"))
}

async fn delete_pageserver_timeline(
    state: &AppState,
    node: &NodeDescribeResponse,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
) -> Result<()> {
    let url = format!(
        "http://{}:{}/v1/tenant/{}/timeline/{}",
        node.listen_http_addr, node.listen_http_port, tenant_shard_id, timeline_id
    );
    state
        .client
        .delete(&url)
        .send()
        .await?
        .error_from_body()
        .await
        .with_context(|| format!("deleting pageserver timeline via {url}"))?;
    Ok(())
}

fn mapping_name_for_timeline(
    mappings: &Value,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Option<String> {
    let tenant_id = tenant_id.to_string();
    let timeline_id = timeline_id.to_string();
    mappings
        .as_object()?
        .iter()
        .find_map(|(branch_name, mapping)| {
            let mapping_tenant_id = mapping.get("tenant_id").and_then(Value::as_str)?;
            let mapping_timeline_id = mapping.get("timeline_id").and_then(Value::as_str)?;
            (mapping_tenant_id == tenant_id && mapping_timeline_id == timeline_id)
                .then(|| branch_name.clone())
        })
}

async fn pageserver_list(State(state): State<Arc<AppState>>) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match list_pageservers(&storage_controller).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_add(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PageServerAddRequest>,
) -> Response {
    match handle_pageserver_add(&state, req) {
        Ok(value) => (StatusCode::CREATED, Json(value)).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn pageserver_shards(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PageServerShardsQuery>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let tenant_id = match query
        .tenant_id
        .as_deref()
        .map(TenantId::from_str)
        .transpose()
        .context("parsing tenant_id")
    {
        Ok(tenant_id) => tenant_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    match list_tenant_shards(&storage_controller, tenant_id, Some(10000)).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_drain(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match drain_pageserver(&storage_controller, utils::id::NodeId(node_id)).await {
        Ok(()) => Json(json!({})).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_cancel_drain(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
    Json(req): Json<PageServerCancelRequest>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let node_id = utils::id::NodeId(node_id);
    let cancel_result = storage_controller.node_cancel_drain(node_id).await;
    let cancel_result = match cancel_result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("no drain in progress") => {
            storage_controller
                .node_configure(NodeConfigureRequest {
                    node_id,
                    availability: None,
                    scheduling: Some(NodeSchedulingPolicy::Active),
                })
                .await
        }
        Err(err) => Err(err),
    };
    match cancel_result {
        Ok(()) => match wait_node_scheduling_policy(
            &storage_controller,
            node_id,
            req.timeout_seconds.unwrap_or(120),
            |sched| {
                matches!(
                    sched,
                    NodeSchedulingPolicy::Active | NodeSchedulingPolicy::PauseForRestart
                )
            },
        )
        .await
        {
            Ok(policy) => Json(json!({ "node_id": node_id, "scheduling": policy })).into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_fill(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match fill_pageserver(&storage_controller, utils::id::NodeId(node_id)).await {
        Ok(()) => Json(json!({})).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_cancel_fill(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
    Json(req): Json<PageServerCancelRequest>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let node_id = utils::id::NodeId(node_id);
    let cancel_result = storage_controller.node_cancel_fill(node_id).await;
    let cancel_result = match cancel_result {
        Ok(()) => Ok(()),
        Err(err) if err.to_string().contains("no fill in progress") => {
            storage_controller
                .node_configure(NodeConfigureRequest {
                    node_id,
                    availability: None,
                    scheduling: Some(NodeSchedulingPolicy::Active),
                })
                .await
        }
        Err(err) => Err(err),
    };
    match cancel_result {
        Ok(()) => match wait_node_scheduling_policy(
            &storage_controller,
            node_id,
            req.timeout_seconds.unwrap_or(120),
            |sched| matches!(sched, NodeSchedulingPolicy::Active),
        )
        .await
        {
            Ok(policy) => Json(json!({ "node_id": node_id, "scheduling": policy })).into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_delete(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
    Json(req): Json<PageServerDeleteRequest>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match start_pageserver_delete(
        &storage_controller,
        utils::id::NodeId(node_id),
        req.force.unwrap_or(false),
    )
    .await
    {
        Ok(()) => Json(json!({})).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_bulk_migrate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PageServerBulkMigrateRequest>,
) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match bulk_migrate_pageservers(&storage_controller, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_migrate(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_shard_id): AxumPath<String>,
    Json(req): Json<PageServerMigrateRequest>,
) -> Response {
    let tenant_shard_id = match TenantShardId::from_str(&tenant_shard_id)
        .with_context(|| format!("parsing tenant_shard_id {tenant_shard_id}"))
    {
        Ok(tenant_shard_id) => tenant_shard_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match migrate_tenant_shard(
        &storage_controller,
        TenantShardMigrateOptions {
            tenant_shard_id,
            node_id: utils::id::NodeId(req.node_id),
            origin_node_id: req.origin_node_id.map(utils::id::NodeId),
            prewarm: req.prewarm,
            override_scheduler: req.override_scheduler.unwrap_or(false),
        },
    )
    .await
    {
        Ok(value) => match wait_tenant_shard_attached(
            &storage_controller,
            tenant_shard_id,
            utils::id::NodeId(req.node_id),
            req.timeout_seconds.unwrap_or(900),
        )
        .await
        {
            Ok(tenant) => Json(json!({
                "migration": value,
                "tenant_shard_id": tenant_shard_id,
                "node_attached": utils::id::NodeId(req.node_id),
                "tenant": tenant,
            }))
            .into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_migrate_secondary(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_shard_id): AxumPath<String>,
    Json(req): Json<PageServerMigrateRequest>,
) -> Response {
    let tenant_shard_id = match TenantShardId::from_str(&tenant_shard_id)
        .with_context(|| format!("parsing tenant_shard_id {tenant_shard_id}"))
    {
        Ok(tenant_shard_id) => tenant_shard_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let mut migration_config = MigrationConfig {
        override_scheduler: req.override_scheduler.unwrap_or(false),
        ..Default::default()
    };
    if let Some(prewarm) = req.prewarm {
        migration_config.prewarm = prewarm;
    }
    match storage_controller
        .tenant_shard_migrate_secondary(
            tenant_shard_id,
            TenantShardMigrateRequest {
                node_id: utils::id::NodeId(req.node_id),
                origin_node_id: req.origin_node_id.map(utils::id::NodeId),
                migration_config,
            },
        )
        .await
    {
        Ok(value) => match wait_tenant_shard_secondary(
            &storage_controller,
            tenant_shard_id,
            utils::id::NodeId(req.node_id),
            req.timeout_seconds.unwrap_or(900),
        )
        .await
        {
            Ok(tenant) => Json(json!({
                "migration": value,
                "tenant_shard_id": tenant_shard_id,
                "node_secondary": utils::id::NodeId(req.node_id),
                "tenant": tenant,
            }))
            .into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn pageserver_cancel_reconcile(
    State(state): State<Arc<AppState>>,
    AxumPath(tenant_shard_id): AxumPath<String>,
) -> Response {
    let tenant_shard_id = match TenantShardId::from_str(&tenant_shard_id)
        .with_context(|| format!("parsing tenant_shard_id {tenant_shard_id}"))
    {
        Ok(tenant_shard_id) => tenant_shard_id,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller
        .tenant_shard_cancel_reconcile(tenant_shard_id)
        .await
    {
        Ok(()) => Json(json!({ "tenant_shard_id": tenant_shard_id })).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn safekeeper_list(State(state): State<Arc<AppState>>) -> Response {
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    match storage_controller.safekeeper_list().await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn safekeeper_scheduling(
    State(state): State<Arc<AppState>>,
    AxumPath(node_id): AxumPath<u64>,
    Json(req): Json<SafekeeperSchedulingRequest>,
) -> Response {
    let scheduling_policy = match SkSchedulingPolicy::from_str(&req.scheduling_policy) {
        Ok(policy) => policy,
        Err(err) => return error_response(StatusCode::BAD_REQUEST, err),
    };
    let storage_controller = match docker_storage_controller_api(&state) {
        Ok(storage_controller) => storage_controller,
        Err(err) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    };
    let node_id = utils::id::NodeId(node_id);
    match storage_controller
        .safekeeper_scheduling(
            node_id,
            SafekeeperSchedulingPolicyRequest { scheduling_policy },
        )
        .await
    {
        Ok(()) => match storage_controller.safekeeper_list().await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
        },
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

async fn safekeeper_timeline_migrate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SafekeeperTimelineMigrateRequest>,
) -> Response {
    match handle_safekeeper_timeline_migrate(&state, req).await {
        Ok(value) => Json(value).into_response(),
        Err(err) => error_response(StatusCode::BAD_GATEWAY, err),
    }
}

fn parse_placement_policy(value: &str) -> Result<PlacementPolicy> {
    match value {
        "detached" => Ok(PlacementPolicy::Detached),
        "secondary" => Ok(PlacementPolicy::Secondary),
        _ if value.starts_with("attached:") => {
            let count = value
                .split_once(':')
                .and_then(|(_, count)| count.parse::<usize>().ok())
                .ok_or_else(|| {
                    anyhow!("invalid placement policy {value}, expected attached:<n>")
                })?;
            Ok(PlacementPolicy::Attached(count))
        }
        _ => {
            bail!("unknown placement policy {value}, expected detached, secondary, or attached:<n>")
        }
    }
}

fn parse_shard_scheduling_policy(value: &str) -> Result<ShardSchedulingPolicy> {
    match value {
        "active" => Ok(ShardSchedulingPolicy::Active),
        "essential" => Ok(ShardSchedulingPolicy::Essential),
        "pause" => Ok(ShardSchedulingPolicy::Pause),
        "stop" => Ok(ShardSchedulingPolicy::Stop),
        _ => bail!(
            "unknown shard scheduling policy {value}, expected active, essential, pause, or stop"
        ),
    }
}

fn cleanup_deleted_tenant_state(state_dir: &Path, tenant_id: TenantId) -> Result<Value> {
    let tenant_id_string = tenant_id.to_string();
    let mut removed_branches = Vec::new();
    let mut mappings = load_mappings(state_dir)?;
    if let Some(object) = mappings.as_object_mut() {
        object.retain(|branch_name, mapping| {
            let remove = mapping
                .get("tenant_id")
                .and_then(Value::as_str)
                .is_some_and(|mapped_tenant| mapped_tenant == tenant_id_string);
            if remove {
                removed_branches.push(branch_name.clone());
            }
            !remove
        });
        write_json_file(&state_dir.join("branches.json"), &mappings)?;
    }

    let mut removed_timeline_safekeepers = Vec::new();
    let dir = state_dir.join("timeline_safekeepers");
    if dir.exists() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?
        {
            let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&format!("{tenant_id}_")))
            {
                std::fs::remove_file(&path)
                    .with_context(|| format!("removing {}", path.display()))?;
                removed_timeline_safekeepers.push(path.display().to_string());
            }
        }
    }

    let env_path = state_dir.join("env.json");
    let mut cleared_default = false;
    if env_path.exists() {
        let mut env: Value = serde_json::from_reader(
            std::fs::File::open(&env_path)
                .with_context(|| format!("opening {}", env_path.display()))?,
        )
        .with_context(|| format!("decoding {}", env_path.display()))?;
        if env
            .get("default_tenant_id")
            .and_then(Value::as_str)
            .is_some_and(|default_tenant| default_tenant == tenant_id_string)
        {
            if let Some(object) = env.as_object_mut() {
                object.remove("default_tenant_id");
            }
            write_json_file(&env_path, &env)?;
            cleared_default = true;
        }
    }

    Ok(json!({
        "tenant_id": tenant_id,
        "deleted": true,
        "removed_branches": removed_branches,
        "removed_timeline_safekeepers": removed_timeline_safekeepers,
        "cleared_default_tenant": cleared_default,
    }))
}

fn cleanup_deleted_timeline_state(
    state_dir: &Path,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<Value> {
    let tenant_id_string = tenant_id.to_string();
    let timeline_id_string = timeline_id.to_string();
    let mut removed_branches = Vec::new();
    let mut mappings = load_mappings(state_dir)?;
    if let Some(object) = mappings.as_object_mut() {
        object.retain(|branch_name, mapping| {
            let remove = mapping
                .get("tenant_id")
                .and_then(Value::as_str)
                .is_some_and(|mapped_tenant| mapped_tenant == tenant_id_string)
                && mapping
                    .get("timeline_id")
                    .and_then(Value::as_str)
                    .is_some_and(|mapped_timeline| mapped_timeline == timeline_id_string);
            if remove {
                removed_branches.push(branch_name.clone());
            }
            !remove
        });
        write_json_file(&state_dir.join("branches.json"), &mappings)?;
    }

    let mut removed_endpoints = Vec::new();
    for endpoint in load_endpoints(state_dir)? {
        if endpoint.tenant_id == tenant_id_string && endpoint.timeline_id == timeline_id_string {
            let removed = delete_endpoint_record(state_dir, &endpoint.endpoint_id)?;
            removed_endpoints.push(removed);
        }
    }

    let safekeepers_path =
        timeline_safekeepers_path(state_dir, &tenant_id_string, &timeline_id_string);
    let removed_timeline_safekeepers = if safekeepers_path.exists() {
        std::fs::remove_file(&safekeepers_path)
            .with_context(|| format!("removing {}", safekeepers_path.display()))?;
        true
    } else {
        false
    };

    Ok(json!({
        "tenant_id": tenant_id,
        "timeline_id": timeline_id,
        "deleted": true,
        "removed_branches": removed_branches,
        "removed_endpoints": removed_endpoints,
        "removed_timeline_safekeepers": removed_timeline_safekeepers,
    }))
}

async fn set_tenant_preferred_az(
    storage_controller: &dyn StorageControllerApi,
    tenant_id: TenantId,
    preferred_az: Option<String>,
) -> Result<Value> {
    let tenant = storage_controller.tenant_describe(tenant_id).await?;
    if let Some(preferred_az) = &preferred_az {
        let known_azs = storage_controller
            .node_list()
            .await?
            .into_iter()
            .map(|node| node.availability_zone_id)
            .collect::<HashSet<_>>();
        if !known_azs.contains(preferred_az) {
            bail!("AZ {preferred_az} not found on any node: known AZs are {known_azs:?}");
        }
    }

    let preferred_az_ids = tenant
        .shards
        .into_iter()
        .map(|shard| {
            (
                shard.tenant_shard_id,
                preferred_az.clone().map(AvailabilityZone),
            )
        })
        .collect();
    let response = storage_controller
        .update_preferred_azs(ShardsPreferredAzsRequest { preferred_az_ids })
        .await?;
    let tenant = storage_controller.tenant_describe(tenant_id).await?;
    Ok(json!({
        "tenant_id": tenant_id,
        "preferred_az": preferred_az,
        "updated": response.updated,
        "tenant": tenant,
    }))
}

async fn wait_node_scheduling_policy<F>(
    storage_controller: &dyn StorageControllerApi,
    node_id: NodeId,
    timeout_seconds: u64,
    done: F,
) -> Result<NodeSchedulingPolicy>
where
    F: Fn(NodeSchedulingPolicy) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let nodes = storage_controller.node_list().await?;
        let node = nodes
            .into_iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| anyhow!("node {node_id} not found"))?;
        if done(node.scheduling) {
            return Ok(node.scheduling);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for node {node_id} scheduling policy, current {:?}",
                node.scheduling
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_tenant_shard<F>(
    storage_controller: &dyn StorageControllerApi,
    tenant_shard_id: TenantShardId,
    timeout_seconds: u64,
    done: F,
) -> Result<TenantDescribeResponse>
where
    F: Fn(&pageserver_api::controller_api::TenantDescribeResponseShard) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let tenant = storage_controller
            .tenant_describe(tenant_shard_id.tenant_id)
            .await?;
        let shard = tenant
            .shards
            .iter()
            .find(|shard| shard.tenant_shard_id == tenant_shard_id)
            .ok_or_else(|| anyhow!("tenant shard {tenant_shard_id} not found"))?;
        if done(shard) {
            return Ok(tenant);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for tenant shard {tenant_shard_id}, last state {}",
                serde_json::to_value(&tenant)?
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_tenant_shard_attached(
    storage_controller: &dyn StorageControllerApi,
    tenant_shard_id: TenantShardId,
    node_id: NodeId,
    timeout_seconds: u64,
) -> Result<TenantDescribeResponse> {
    wait_tenant_shard(
        storage_controller,
        tenant_shard_id,
        timeout_seconds,
        |shard| shard.node_attached == Some(node_id) && !shard.is_reconciling,
    )
    .await
}

async fn wait_tenant_shard_secondary(
    storage_controller: &dyn StorageControllerApi,
    tenant_shard_id: TenantShardId,
    node_id: NodeId,
    timeout_seconds: u64,
) -> Result<TenantDescribeResponse> {
    wait_tenant_shard(
        storage_controller,
        tenant_shard_id,
        timeout_seconds,
        |shard| shard.node_secondary.contains(&node_id) && !shard.is_reconciling,
    )
    .await
}

async fn bulk_migrate_pageservers(
    storage_controller: &dyn StorageControllerApi,
    req: PageServerBulkMigrateRequest,
) -> Result<Value> {
    let drain_ids = req
        .nodes
        .iter()
        .copied()
        .map(NodeId)
        .collect::<HashSet<_>>();
    if drain_ids.is_empty() {
        bail!("bulk migrate requires at least one source node");
    }

    let nodes = storage_controller.node_list().await?;
    let mut drain_nodes = Vec::new();
    let mut fill_nodes = Vec::new();
    for node in nodes {
        if drain_ids.contains(&node.id) {
            drain_nodes.push(node);
        } else if matches!(node.availability, NodeAvailabilityWrapper::Active)
            && matches!(
                node.scheduling,
                NodeSchedulingPolicy::Active | NodeSchedulingPolicy::Filling
            )
        {
            fill_nodes.push(node);
        }
    }
    if drain_nodes.len() != drain_ids.len() {
        bail!("bulk migration requested away from a node that does not exist");
    }
    if fill_nodes.is_empty() {
        bail!("there are no active destination nodes to migrate to");
    }

    for node in &drain_nodes {
        storage_controller
            .node_configure(NodeConfigureRequest {
                node_id: node.id,
                availability: None,
                scheduling: Some(NodeSchedulingPolicy::Draining),
            })
            .await?;
    }

    let tenants = storage_controller.tenant_list(Some(10000)).await?;
    let mut selected_node_idx = 0usize;
    let mut moves = Vec::new();
    for shard in tenants.into_iter().flat_map(|tenant| tenant.shards) {
        if req
            .max_shards
            .is_some_and(|max_shards| moves.len() >= max_shards)
        {
            break;
        }
        let Some(from) = shard.node_attached else {
            continue;
        };
        if !drain_ids.contains(&from) {
            continue;
        }
        let to = fill_nodes[selected_node_idx].id;
        selected_node_idx = (selected_node_idx + 1) % fill_nodes.len();
        moves.push((shard.tenant_shard_id, from, to));
    }

    if req.dry_run.unwrap_or(false) {
        return Ok(json!({
            "dry_run": true,
            "planned": moves.iter().map(|(tenant_shard_id, from, to)| {
                json!({ "tenant_shard_id": tenant_shard_id, "from": from, "to": to })
            }).collect::<Vec<_>>(),
            "total": moves.len(),
            "concurrency": req.concurrency.unwrap_or(8),
        }));
    }

    let concurrency = req.concurrency.unwrap_or(8);
    if concurrency == 0 {
        bail!("bulk migrate concurrency must be greater than zero");
    }
    let override_scheduler = req.override_scheduler.unwrap_or(false);
    let timeout_seconds = req.timeout_seconds.unwrap_or(900);
    let mut stream = futures::stream::iter(moves)
        .map(|(tenant_shard_id, from, to)| async move {
            let result = async {
                storage_controller
                    .tenant_shard_migrate(
                        tenant_shard_id,
                        TenantShardMigrateRequest {
                            node_id: to,
                            origin_node_id: Some(from),
                            migration_config: MigrationConfig {
                                override_scheduler,
                                ..Default::default()
                            },
                        },
                    )
                    .await?;
                wait_tenant_shard_attached(storage_controller, tenant_shard_id, to, timeout_seconds)
                    .await
            }
            .await;
            (tenant_shard_id, from, to, result)
        })
        .buffer_unordered(concurrency);

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    while let Some((tenant_shard_id, from, to, result)) = stream.next().await {
        match result {
            Ok(tenant) => succeeded.push(json!({
                "tenant_shard_id": tenant_shard_id,
                "from": from,
                "to": to,
                "tenant": tenant,
            })),
            Err(err) => failed.push(json!({
                "tenant_shard_id": tenant_shard_id,
                "from": from,
                "to": to,
                "error": err.to_string(),
            })),
        }
    }
    let success = succeeded.len();
    let failure = failed.len();

    Ok(json!({
        "dry_run": false,
        "succeeded": succeeded,
        "failed": failed,
        "success": success,
        "failure": failure,
        "concurrency": concurrency,
        "override_scheduler": override_scheduler,
    }))
}

async fn handle_safekeeper_timeline_migrate(
    state: &AppState,
    req: SafekeeperTimelineMigrateRequest,
) -> Result<Value> {
    let tenant_id = TenantId::from_str(&req.tenant_id).context("parsing tenant_id")?;
    let timeline_id = TimelineId::from_str(&req.timeline_id).context("parsing timeline_id")?;
    let new_sk_set = req.new_sk_set.into_iter().map(NodeId).collect::<Vec<_>>();
    let storage_controller = docker_storage_controller_api(state)?;
    storage_controller
        .timeline_safekeeper_migrate(
            tenant_id,
            timeline_id,
            TimelineSafekeeperMigrateRequest {
                new_sk_set: new_sk_set.clone(),
            },
        )
        .await?;

    let locate_value = proxy_get_json(
        state,
        &format!("/debug/v1/tenant/{tenant_id}/timeline/{timeline_id}/locate"),
    )
    .await?;
    let locate: SafekeeperTimelineLocateResponse = serde_json::from_value(locate_value.clone())
        .context("decoding storage-controller timeline locate response")?;
    let safekeepers = storage_controller.safekeeper_list().await?;
    let host_by_id = safekeepers
        .into_iter()
        .map(|sk| (sk.id, sk.host))
        .collect::<HashMap<_, _>>();
    let saved_safekeepers = locate
        .sk_set
        .iter()
        .map(|node_id| SafekeeperInfo {
            id: node_id.0,
            hostname: host_by_id.get(node_id).cloned(),
        })
        .collect::<Vec<_>>();
    save_timeline_safekeepers_value(
        &state.state_dir,
        &tenant_id.to_string(),
        &timeline_id.to_string(),
        locate.generation.into_inner(),
        &saved_safekeepers,
    )?;

    Ok(json!({
        "tenant_id": tenant_id,
        "timeline_id": timeline_id,
        "requested_new_sk_set": new_sk_set,
        "locate": locate_value,
        "saved_safekeepers": saved_safekeepers,
    }))
}

async fn oggit_gc(State(state): State<Arc<AppState>>, body: String) -> Response {
    let req = if body.trim().is_empty() {
        OggitGcRequest {
            retention_lsn_distance: default_oggit_gc_retention_lsn_distance(),
        }
    } else {
        match serde_json::from_str::<OggitGcRequest>(&body) {
            Ok(req) => req,
            Err(err) => return error_response(StatusCode::BAD_REQUEST, anyhow!(err)),
        }
    };
    match handle_oggit_gc(&state, req).await {
        Ok(results) => Json(json!({ "results": results })).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

fn min_child_ancestor_lsn(parent_id: TimelineId, timelines: &[TimelineInfo]) -> Option<String> {
    timelines
        .iter()
        .filter(|timeline| timeline.ancestor_timeline_id == Some(parent_id))
        .filter_map(|timeline| timeline.ancestor_lsn)
        .min()
        .map(|lsn| lsn.to_string())
}

async fn run_oggit_gc_on_connstr(
    connstr: &str,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    floor_lsn: String,
    retention_lsn_distance: u128,
) -> Result<OggitGcParentResult> {
    let (client, connection) = tokio_opengauss::connect(connstr, NoTls)
        .await
        .with_context(|| format!("failed to connect to oggit parent endpoint at {connstr}"))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    oggit_gc_parent(
        &client,
        OggitGcParentRequest {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            floor_lsn,
            retention_lsn_distance,
        },
    )
    .await
}

async fn handle_oggit_gc(
    state: &AppState,
    req: OggitGcRequest,
) -> Result<Vec<OggitGcParentResult>> {
    let storage_controller = docker_storage_controller_api(state)?;
    let nodes = load_pageserver_nodes(state).await?;
    let tenants = storage_controller.tenant_list(Some(10000)).await?;
    let endpoints = load_endpoints(&state.state_dir)?;
    let mut results = Vec::new();

    for tenant in tenants {
        for shard in tenant.shards {
            let Some(node_id) = shard.node_attached else {
                continue;
            };
            let Some(node) = nodes.get(&node_id.0) else {
                continue;
            };
            let timelines = list_pageserver_timelines(state, node, shard.tenant_shard_id).await?;
            for timeline in timelines
                .iter()
                .filter(|timeline| timeline.ancestor_timeline_id.is_none())
            {
                let tenant_id = timeline.tenant_id.tenant_id;
                let floor_lsn = min_child_ancestor_lsn(timeline.timeline_id, &timelines)
                    .unwrap_or_else(|| "0/0".to_string());
                let running = endpoints
                    .iter()
                    .filter(|endpoint| {
                        endpoint.tenant_id == tenant_id.to_string()
                            && endpoint.timeline_id == timeline.timeline_id.to_string()
                            && endpoint.status == "Running"
                    })
                    .collect::<Vec<_>>();

                if running.is_empty() {
                    results.push(OggitGcParentResult {
                        tenant_id: tenant_id.to_string(),
                        timeline_id: timeline.timeline_id.to_string(),
                        status: "skipped".to_string(),
                        floor_lsn,
                        deleted_change_log: 0,
                        deleted_object_change: 0,
                        skipped_reason: Some(
                            "no running endpoint for root parent timeline".to_string(),
                        ),
                    });
                    continue;
                }
                if running.len() > 1 {
                    results.push(OggitGcParentResult {
                        tenant_id: tenant_id.to_string(),
                        timeline_id: timeline.timeline_id.to_string(),
                        status: "skipped".to_string(),
                        floor_lsn,
                        deleted_change_log: 0,
                        deleted_object_change: 0,
                        skipped_reason: Some(
                            "multiple running endpoints for root parent timeline".to_string(),
                        ),
                    });
                    continue;
                }

                let endpoint = running[0];
                let connstr = branch_endpoint_connstr(endpoint, "branch_merge", "postgres")?;
                let mut result = run_oggit_gc_on_connstr(
                    &connstr,
                    tenant_id,
                    timeline.timeline_id,
                    floor_lsn,
                    u128::from(req.retention_lsn_distance),
                )
                .await
                .with_context(|| format!("failed to GC endpoint {}", endpoint.endpoint_id))?;
                if result.status == "deleted"
                    && result.deleted_change_log == 0
                    && result.deleted_object_change == 0
                {
                    result.status = "ok".to_string();
                }
                results.push(result);
            }
        }
    }

    Ok(results)
}

async fn branch_diff(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BranchRunRequest>,
) -> Response {
    match branch_run_endpoint_refs(&state, &req).and_then(|(source_endpoint, target_endpoint)| {
        Ok(BranchDiffOptions {
            source_endpoint,
            target_endpoint,
            source_schema: req.source_schema.unwrap_or_else(|| "public".to_string()),
            target_schema: req.target_schema.unwrap_or_else(|| "public".to_string()),
            fdw_server: "neon_merge_src".to_string(),
            keep_fdw: req.keep_fdw.unwrap_or(false),
            incremental_oggit: req.incremental_oggit.unwrap_or(false),
        })
    }) {
        Ok(opts) => match diff_branch(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_merge(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BranchRunRequest>,
) -> Response {
    match branch_run_endpoint_refs(&state, &req).and_then(|(source_endpoint, target_endpoint)| {
        let strategy = BranchMergeStrategy::from_str(
            &req.strategy.clone().unwrap_or_else(|| "fail".to_string()),
        )?;
        Ok(BranchMergeOptions {
            source_endpoint,
            target_endpoint,
            source_schema: req.source_schema.unwrap_or_else(|| "public".to_string()),
            target_schema: req.target_schema.unwrap_or_else(|| "public".to_string()),
            strategy,
            fdw_server: "neon_merge_src".to_string(),
            copy_source_only_tables: true,
            keep_fdw: req.keep_fdw.unwrap_or(false),
            incremental_oggit: req.incremental_oggit.unwrap_or(false),
        })
    }) {
        Ok(opts) => match merge_branch(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_status(
    State(state): State<Arc<AppState>>,
    AxumPath(merge_id): AxumPath<String>,
    Query(query): Query<BranchTargetQuery>,
) -> Response {
    match branch_target_endpoint_ref_from_selector(
        &state,
        query.tenant_id.as_deref(),
        query.target_branch.as_deref(),
        query.target_endpoint.as_deref(),
        query.target_connstr.as_deref(),
        query.user.as_deref(),
        query.database.as_deref(),
    )
    .map(|target_endpoint| BranchTargetOptions {
        target_endpoint,
        merge_id,
    }) {
        Ok(opts) => match merge_status(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_conflicts(
    State(state): State<Arc<AppState>>,
    AxumPath(merge_id): AxumPath<String>,
    Query(query): Query<BranchTargetQuery>,
) -> Response {
    match branch_target_endpoint_ref_from_selector(
        &state,
        query.tenant_id.as_deref(),
        query.target_branch.as_deref(),
        query.target_endpoint.as_deref(),
        query.target_connstr.as_deref(),
        query.user.as_deref(),
        query.database.as_deref(),
    )
    .map(|target_endpoint| BranchTargetOptions {
        target_endpoint,
        merge_id,
    }) {
        Ok(opts) => match conflicts(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_resolve(
    State(state): State<Arc<AppState>>,
    AxumPath(merge_id): AxumPath<String>,
    Json(req): Json<BranchTargetRequest>,
) -> Response {
    match branch_target_request_options(&state, merge_id, req) {
        Ok(opts) => match resolve_conflict(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_continue(
    State(state): State<Arc<AppState>>,
    AxumPath(merge_id): AxumPath<String>,
    Json(req): Json<BranchTargetRequest>,
) -> Response {
    match branch_target_endpoint_ref_from_request(&state, &req).map(|target_endpoint| {
        BranchTargetOptions {
            target_endpoint,
            merge_id,
        }
    }) {
        Ok(opts) => match continue_merge(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn branch_abort(
    State(state): State<Arc<AppState>>,
    AxumPath(merge_id): AxumPath<String>,
    Json(req): Json<BranchTargetRequest>,
) -> Response {
    match branch_target_endpoint_ref_from_request(&state, &req).map(|target_endpoint| {
        BranchTargetOptions {
            target_endpoint,
            merge_id,
        }
    }) {
        Ok(opts) => match abort_merge(opts).await {
            Ok(value) => Json(value).into_response(),
            Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
        },
        Err(err) => error_response(StatusCode::BAD_REQUEST, err),
    }
}

async fn notify_attach(
    State(state): State<Arc<AppState>>,
    Json(req): Json<NotifyAttachRequest>,
) -> Response {
    match handle_notify_attach(&state, req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn notify_safekeepers(
    State(state): State<Arc<AppState>>,
    Json(req): Json<NotifySafekeepersRequest>,
) -> Response {
    match handle_notify_safekeepers(&state, req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => error_response(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

async fn handle_notify_attach(
    state: &AppState,
    req: NotifyAttachRequest,
) -> Result<NotifyResponse> {
    let endpoints = running_endpoints_for_tenant(&state.state_dir, &req.tenant_id)?;
    if endpoints.is_empty() {
        return Ok(NotifyResponse {
            reconfigured: Vec::new(),
            skipped: 0,
        });
    }

    let nodes = load_pageserver_nodes(state).await?;
    let mut ordered_shards = req.shards;
    ordered_shards.sort_by_key(|shard| shard.shard_number);

    let mut pageserver_parts = Vec::with_capacity(ordered_shards.len());
    for shard in ordered_shards {
        let node = nodes
            .get(&shard.node_id)
            .ok_or_else(|| anyhow!("pageserver node {} not found", shard.node_id))?;
        pageserver_parts.push(format!(
            "host={} port={}",
            node.listen_pg_addr, node.listen_pg_port
        ));
    }
    let pageserver_connstring = pageserver_parts.join(",");

    let mut reconfigured = Vec::new();
    for endpoint in endpoints {
        update_endpoint_config(&state.state_dir, &endpoint, |spec| {
            spec["pageserver_connstring"] = Value::String(pageserver_connstring.clone());
            upsert_setting(
                spec,
                "neon.pageserver_connstring",
                &pageserver_connstring,
                "string",
            )?;
            if let Some(stripe_size) = req.stripe_size {
                spec["shard_stripe_size"] = json!(stripe_size);
            }
            Ok(())
        })?;

        post_configure(state, &endpoint).await?;
        reconfigured.push(endpoint.endpoint_id);
    }

    Ok(NotifyResponse {
        reconfigured,
        skipped: 0,
    })
}

async fn handle_notify_safekeepers(
    state: &AppState,
    req: NotifySafekeepersRequest,
) -> Result<NotifyResponse> {
    save_timeline_safekeepers_value(
        &state.state_dir,
        &req.tenant_id,
        &req.timeline_id,
        req.generation,
        &req.safekeepers,
    )?;

    let endpoints = running_endpoints_for_tenant(&state.state_dir, &req.tenant_id)?
        .into_iter()
        .filter(|endpoint| endpoint.timeline_id == req.timeline_id)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Ok(NotifyResponse {
            reconfigured: Vec::new(),
            skipped: 0,
        });
    }

    let connstrings = req
        .safekeepers
        .iter()
        .map(|sk| {
            let host = sk
                .hostname
                .clone()
                .unwrap_or_else(|| safekeeper_host_from_id(sk.id));
            format!("{host}:5454")
        })
        .collect::<Vec<_>>();
    let connstrings_setting = connstrings.join(",");

    let mut reconfigured = Vec::new();
    for endpoint in endpoints {
        update_endpoint_config(&state.state_dir, &endpoint, |spec| {
            spec["safekeeper_connstrings"] = json!(connstrings);
            spec["safekeepers_generation"] = json!(req.generation);
            upsert_setting(spec, "neon.safekeepers", &connstrings_setting, "string")?;
            Ok(())
        })?;

        post_configure(state, &endpoint).await?;
        reconfigured.push(endpoint.endpoint_id);
    }

    Ok(NotifyResponse {
        reconfigured,
        skipped: 0,
    })
}

async fn load_status(state: &AppState) -> Result<Value> {
    let endpoints = load_endpoints(&state.state_dir)?;
    let env = read_or_write_default_env(state)?;
    let pageservers = proxy_get_json(state, "/control/v1/node")
        .await
        .unwrap_or(json!([]));
    let tenants = proxy_get_json(state, "/control/v1/tenant?limit=10000")
        .await
        .unwrap_or(json!([]));
    Ok(json!({
        "status": "ok",
        "env": env,
        "endpoints": endpoints,
        "pageservers": pageservers,
        "tenants": tenants,
    }))
}
