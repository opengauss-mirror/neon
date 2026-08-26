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
