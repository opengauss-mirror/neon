use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router, body::Body};
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
use utils::auth::{Claims, JwtAuth, Scope, encode_from_key_file};
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
    oggit_worker_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
struct ControlPlaneAuth {
    jwt_auth: Option<Arc<JwtAuth>>,
}

impl ControlPlaneAuth {
    fn from_environment() -> Result<Self> {
        let Some(key_path) = std::env::var_os("DOCKER_CONTROL_PLANE_HTTP_AUTH_PUBLIC_KEY_PATH")
            .filter(|path| !path.is_empty())
        else {
            return Ok(Self { jwt_auth: None });
        };

        let key_path = camino::Utf8PathBuf::from_path_buf(PathBuf::from(key_path))
            .map_err(|path| anyhow!("authentication public key path is not valid UTF-8: {path:?}"))?;
        let jwt_auth = JwtAuth::from_key_path(&key_path).with_context(|| {
            format!(
                "loading docker control plane HTTP authentication public key from {key_path}"
            )
        })?;

        Ok(Self {
            jwt_auth: Some(Arc::new(jwt_auth)),
        })
    }

    fn is_enabled(&self) -> bool {
        self.jwt_auth.is_some()
    }
}

async fn authorize_control_plane_request(
    axum::extract::State(auth): axum::extract::State<ControlPlaneAuth>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let Some(jwt_auth) = auth.jwt_auth else {
        return next.run(request).await;
    };

    let Some(authorization_header) = request.headers().get(header::AUTHORIZATION) else {
        return (StatusCode::UNAUTHORIZED, "missing Authorization header").into_response();
    };
    let Ok(authorization_header) = authorization_header.to_str() else {
        return (StatusCode::UNAUTHORIZED, "invalid Authorization header").into_response();
    };
    let Some(token) = authorization_header.strip_prefix("Bearer ") else {
        return (StatusCode::UNAUTHORIZED, "Authorization header must use Bearer scheme")
            .into_response();
    };

    let claims = match validate_control_plane_token(&jwt_auth, token) {
        Ok(claims) => claims,
        Err((status, message)) => return (status, message).into_response(),
    };

    request.extensions_mut().insert(claims);
    next.run(request).await
}

fn validate_control_plane_token(
    jwt_auth: &JwtAuth,
    token: &str,
) -> std::result::Result<Claims, (StatusCode, &'static str)> {
    let token_data = jwt_auth.decode::<Claims>(token).map_err(|error| {
        tracing::debug!(%error, "control plane JWT validation failed");
        (StatusCode::UNAUTHORIZED, "invalid control plane JWT")
    })?;

    if token_data.claims.scope != Scope::Admin {
        return Err((
            StatusCode::FORBIDDEN,
            "control plane Admin scope is required",
        ));
    }

    Ok(token_data.claims)
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
struct EndpointStartRequest {
    #[serde(default)]
    enable_oggit: bool,
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
        oggit_worker_lock: Arc::new(tokio::sync::Mutex::new(())),
    });

    let auth = ControlPlaneAuth::from_environment()?;
    let protected_app = Router::new()
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
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            auth.clone(),
            authorize_control_plane_request,
        ));
    let app = Router::new().route("/ready", get(ready)).merge(protected_app);

    println!(
        "docker_control_plane listening on {listen} (http_auth={})",
        auth.is_enabled()
    );
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
    Json(req): Json<EndpointStartRequest>,
) -> Response {
    let _worker_lock = state.oggit_worker_lock.lock().await;
    match handle_endpoint_start(&state, &endpoint_id, req.enable_oggit).await {
        Ok(plan) => Json(plan).into_response(),
        Err(err) => {
            let status = if err
                .to_string()
                .contains("already has an active oggit worker")
            {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            error_response(status, err)
        }
    }
}

async fn endpoint_stop(
    State(state): State<Arc<AppState>>,
    AxumPath(endpoint_id): AxumPath<String>,
    Json(req): Json<EndpointStopRequest>,
) -> Response {
    let _worker_lock = state.oggit_worker_lock.lock().await;
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

fn default_branch_name() -> String {
    "main".to_string()
}

fn default_pg_version() -> PgMajorVersion {
    PgMajorVersion::PG14
}

fn docker_safekeeper_service_name(id: u64) -> String {
    if id == 1 {
        "safekeeper".to_string()
    } else {
        format!("safekeeper{id}")
    }
}

fn env_u64(env: &Value, key: &str, default: u64) -> u64 {
    env.get(key).and_then(Value::as_u64).unwrap_or(default)
}

fn write_env(state: &AppState, req: InitRequest) -> Result<Value> {
    ensure_auth_keys(&state.state_dir)?;
    let compose_project = req
        .compose_project
        .unwrap_or_else(|| state.compose_project.clone());
    let env = json!({
        "compose_project": compose_project,
        "compose_file": "docker-compose.yml",
        "runtime_override_file": ".neon/control_plane/overrides/runtime.yml",
        "network_name": format!("{compose_project}_default"),
        "og_version": req.og_version.unwrap_or_else(|| state.og_version.clone()),
        "storage_image": req.storage_image.unwrap_or_else(|| state.storage_image.clone()),
        "compute_image": req.compute_image.unwrap_or_else(|| state.compute_image.clone()),
        "num_pageservers": req.num_pageservers.unwrap_or(1),
        "num_safekeepers": req.num_safekeepers.unwrap_or(1),
        "default_tenant_id": Value::Null,
        "remote_storage": {
            "type": "local_fs",
            "path": ".neon/shared_remote_storage"
        },
        "storage_controller_url": state.storage_controller_url,
        "storage_controller_host_url": state.host_storage_controller_url,
        "endpoint_storage_addr": "endpoint_storage:9993",
        "auth_private_key_path": ".neon/control_plane/auth_private_key.pem",
        "auth_public_key_path": ".neon/control_plane/auth_public_key.pem"
    });
    write_json_file(&state.state_dir.join("env.json"), &env)?;
    render_runtime_override(state)?;
    Ok(env)
}

fn read_or_write_default_env(state: &AppState) -> Result<Value> {
    let path = state.state_dir.join("env.json");
    if path.exists() {
        let file =
            std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        return serde_json::from_reader(file)
            .with_context(|| format!("decoding {}", path.display()));
    }
    write_env(state, InitRequest::default())
}

async fn handle_endpoint_create(
    state: &AppState,
    req: EndpointCreateRequest,
) -> Result<EndpointRecord> {
    validate_id(&req.endpoint_id)?;
    let service_name = req
        .service_name
        .unwrap_or_else(|| sanitize_service_name(&req.endpoint_id));
    validate_id(&service_name)?;
    let static_lsn = req
        .static_lsn
        .as_deref()
        .map(utils::lsn::Lsn::from_str)
        .transpose()
        .context("parsing static_lsn")?;
    compute_mode_from_options(static_lsn, req.hot_standby.unwrap_or(false))?;

    let branch_name = req.branch_name.unwrap_or_else(default_branch_name);
    let mut store = DockerStateStore::new(&state.state_dir);
    let requested_tenant_id = req
        .tenant_id
        .as_deref()
        .map(TenantId::from_str)
        .transpose()
        .context("parsing tenant_id")?;
    let tenant_id = resolve_tenant(&store, requested_tenant_id)
        .or_else(|_| {
            docker_branch_tenant_id(&state.state_dir, &branch_name)?
                .context("tenant_id is required when branch mapping is absent")
        })
        .context("resolving tenant_id for endpoint")?;
    let timeline_id = req
        .timeline_id
        .as_deref()
        .map(TimelineId::from_str)
        .transpose()
        .context("parsing timeline_id")?
        .or_else(|| resolve_timeline(&store, &branch_name, tenant_id).ok())
        .context("timeline_id is required when branch mapping is absent")?;

    let endpoint_dir = state.state_dir.join("endpoints").join(&req.endpoint_id);
    std::fs::create_dir_all(&endpoint_dir)
        .with_context(|| format!("creating {}", endpoint_dir.display()))?;
    let neon_dir = neon_data_dir(state)?;
    let endpoint_data_dir = neon_dir.join(&service_name);
    std::fs::create_dir_all(&endpoint_data_dir)
        .with_context(|| format!("creating {}", endpoint_data_dir.display()))?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(endpoint_data_dir.join("postgresql_extend.conf"))
        .with_context(|| {
            format!(
                "creating {}",
                endpoint_data_dir.join("postgresql_extend.conf").display()
            )
        })?;
    let previous_endpoint = load_endpoint(&state.state_dir, &req.endpoint_id).ok();
    let host_pg_port = req.host_pg_port.unwrap_or(55433);
    let host_http_port = req.host_http_port.unwrap_or(3080);
    let internal_http_port = req.internal_http_port.unwrap_or(3080);
    let compose_project = read_or_write_default_env(state)?
        .get("compose_project")
        .and_then(Value::as_str)
        .unwrap_or(&state.compose_project)
        .to_string();
    let mut endpoint = EndpointRecord {
        endpoint_id: req.endpoint_id.clone(),
        tenant_id: tenant_id.to_string(),
        timeline_id: timeline_id.to_string(),
        branch_name,
        service_name: service_name.clone(),
        container_name: Some(format!("{compose_project}-{service_name}-1")),
        compute_http: format!("http://{service_name}:{internal_http_port}"),
        compute_pg: Some(format!("{service_name}:55433")),
        host_pg_port: Some(host_pg_port),
        host_http_port: Some(host_http_port),
        internal_http_port: Some(internal_http_port),
        endpoint_pageserver_id: req.endpoint_pageserver_id,
        data_dir: Some(format!(".neon/{service_name}")),
        config_path: format!("/control_plane/endpoints/{}/config.json", req.endpoint_id),
        postgresql_conf_path: Some(format!(
            ".neon/control_plane/endpoints/{}/postgresql.conf",
            req.endpoint_id
        )),
        last_lsn: None,
        static_lsn: req.static_lsn,
        hot_standby: req.hot_standby.unwrap_or(false),
        autoprewarm: req.autoprewarm.unwrap_or(false),
        offload_lfc_interval_seconds: req.offload_lfc_interval_seconds,
        pg_version: req.pg_version.unwrap_or(PgMajorVersion::PG14),
        grpc: req.grpc.unwrap_or(false),
        config_only: req.config_only.unwrap_or(false),
        enable_oggit: req.enable_oggit.unwrap_or(false),
        oggit_database: req.oggit_database.unwrap_or_else(default_oggit_database),
        skip_pg_catalog_updates: !req.update_catalog.unwrap_or(false),
        create_test_user: req.create_test_user.unwrap_or(false),
        remote_ext_base_url: req.remote_ext_base_url,
        privileged_role_name: req.privileged_role_name,
        safekeepers_generation: req.safekeepers_generation,
        safekeeper_connstrings: req.safekeeper_connstrings,
        extra_config: req.extra_config,
        auth_token: None,
        endpoint_storage_addr: Some("endpoint_storage:9993".to_string()),
        endpoint_storage_token: None,
        status: "Created".to_string(),
    };
    if let Some(previous_endpoint) = previous_endpoint {
        endpoint.status = previous_endpoint.status;
        endpoint.last_lsn = previous_endpoint.last_lsn;
        endpoint.auth_token = previous_endpoint.auth_token;
        endpoint.endpoint_storage_token = previous_endpoint.endpoint_storage_token;
    }

    map_branch(
        &mut store,
        endpoint.branch_name.clone(),
        tenant_id,
        timeline_id,
    )?;
    write_endpoint_record(&state.state_dir, &endpoint)?;
    render_endpoint_config(state, &endpoint).await?;
    render_endpoint_override(state, &endpoint)?;
    Ok(endpoint)
}

async fn handle_endpoint_start(
    state: &AppState,
    endpoint_id: &str,
    enable_oggit: bool,
) -> Result<PlanResponse> {
    let mut endpoint = load_endpoint(&state.state_dir, endpoint_id)?;
    endpoint.enable_oggit = enable_oggit;
    if enable_oggit {
        let conflicting_endpoint = load_endpoints(&state.state_dir)?.into_iter().find(|other| {
            other.endpoint_id != endpoint.endpoint_id
                && other.tenant_id == endpoint.tenant_id
                && other.timeline_id == endpoint.timeline_id
                && other.enable_oggit
                && matches!(other.status.as_str(), "Running" | "Starting")
        });
        if let Some(other) = conflicting_endpoint {
            bail!(
                "timeline {} already has an active oggit worker owned by endpoint {}",
                endpoint.timeline_id,
                other.endpoint_id
            );
        }
    }
    render_endpoint_config(state, &endpoint).await?;
    render_endpoint_override(state, &endpoint)?;
    endpoint.status = "Starting".to_string();
    write_endpoint_record(&state.state_dir, &endpoint)?;
    Ok(PlanResponse {
        status: "planned".to_string(),
        compose_files: vec![
            "docker-compose.yml".to_string(),
            format!(".neon/control_plane/overrides/endpoints/{endpoint_id}.yml"),
        ],
        services: vec![endpoint.service_name],
    })
}

async fn handle_endpoint_reconfigure(
    state: &AppState,
    endpoint_id: &str,
) -> Result<EndpointRecord> {
    let endpoint = load_endpoint(&state.state_dir, endpoint_id)?;
    render_endpoint_config(state, &endpoint).await?;
    if endpoint.status == "Running" {
        post_configure(state, &endpoint).await?;
    }
    Ok(endpoint)
}

async fn handle_tenant_create(state: &AppState, req: TenantCreateRequest) -> Result<Value> {
    let branch_name = req.branch_name.unwrap_or_else(default_branch_name);
    let tenant_id = req
        .tenant_id
        .as_deref()
        .map(TenantId::from_str)
        .transpose()
        .context("parsing tenant_id")?;
    let timeline_id = req
        .timeline_id
        .as_deref()
        .map(TimelineId::from_str)
        .transpose()
        .context("parsing timeline_id")?;
    let storage_controller = HttpStorageControllerApi::new(
        state
            .storage_controller_url
            .parse()
            .context("parsing storage_controller_url")?,
        state.client.clone(),
    );
    let mut store = DockerStateStore::new(&state.state_dir);
    let output = create_tenant(
        &storage_controller,
        &mut store,
        TenantCreateOptions {
            tenant_id,
            timeline_id,
            branch_name,
            set_default: req.set_default.unwrap_or(false),
            pg_version: req.pg_version.unwrap_or(PgMajorVersion::PG14),
            shard_count: req.shard_count.unwrap_or(0),
            shard_stripe_size: req.shard_stripe_size,
            placement_policy: req.placement_policy,
            config: req.config.unwrap_or_default(),
        },
    )
    .await?;
    if let Some(safekeepers) = &output.safekeepers {
        save_timeline_safekeepers_info(&state.state_dir, safekeepers)?;
    }
    let mappings = load_mappings(&state.state_dir)?;
    Ok(json!({
        "tenant_id": output.tenant_id,
        "timeline_id": output.timeline_id,
        "branch_name": output.branch_name,
        "last_record_lsn": output.timeline_info.last_record_lsn,
        "safekeepers": output.safekeepers,
        "mappings": mappings,
    }))
}

async fn handle_timeline_branch(state: &AppState, req: TimelineBranchRequest) -> Result<Value> {
    let mappings = load_mappings(&state.state_dir)?;
    let main = mappings.get("main");
    let tenant_id = req
        .tenant_id
        .or_else(|| {
            main.and_then(|v| {
                v.get("tenant_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
        })
        .context("tenant_id is required when main mapping is absent")?;
    let ancestor_timeline_id = req
        .ancestor_timeline_id
        .or_else(|| {
            main.and_then(|v| {
                v.get("timeline_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
        })
        .context("ancestor_timeline_id is required when main mapping is absent")?;
    let ancestor_start_lsn = match req.ancestor_start_lsn {
        Some(lsn) => lsn,
        None => {
            let storage_controller = docker_storage_controller_api(state)?;
            let tenant_id = TenantId::from_str(&tenant_id).context("parsing tenant_id")?;
            let ancestor_timeline_id = TimelineId::from_str(&ancestor_timeline_id)
                .context("parsing ancestor_timeline_id")?;
            timeline_last_record_lsn(&storage_controller, tenant_id, ancestor_timeline_id)
                .await?
                .to_string()
        }
    };
    let tenant_id = TenantId::from_str(&tenant_id).context("parsing tenant_id")?;
    let ancestor_timeline_id =
        TimelineId::from_str(&ancestor_timeline_id).context("parsing ancestor_timeline_id")?;
    let timeline_id = req
        .timeline_id
        .as_deref()
        .map(TimelineId::from_str)
        .transpose()
        .context("parsing timeline_id")?;
    let ancestor_start_lsn =
        utils::lsn::Lsn::from_str(&ancestor_start_lsn).context("parsing ancestor_start_lsn")?;
    let storage_controller = HttpStorageControllerApi::new(
        state
            .storage_controller_url
            .parse()
            .context("parsing storage_controller_url")?,
        state.client.clone(),
    );
    let mut store = DockerStateStore::new(&state.state_dir);
    let output = branch_timeline(
        &storage_controller,
        &mut store,
        TimelineBranchOptions {
            tenant_id,
            timeline_id,
            branch_name: req.branch_name,
            ancestor_timeline_id,
            ancestor_start_lsn: Some(ancestor_start_lsn),
        },
    )
    .await?;
    if let Some(safekeepers) = &output.safekeepers {
        save_timeline_safekeepers_info(&state.state_dir, safekeepers)?;
    }
    let mappings = load_mappings(&state.state_dir)?;
    Ok(json!({
        "tenant_id": output.tenant_id,
        "timeline_id": output.timeline_id,
        "branch_name": output.branch_name,
        "ancestor_timeline_id": ancestor_timeline_id,
        "ancestor_start_lsn": ancestor_start_lsn,
        "last_record_lsn": output.timeline_info.last_record_lsn,
        "safekeepers": output.safekeepers,
        "mappings": mappings,
    }))
}

async fn handle_timeline_create(state: &AppState, req: TimelineCreateRequest) -> Result<Value> {
    let mappings = load_mappings(&state.state_dir)?;
    let main = mappings.get("main");
    let tenant_id = req
        .tenant_id
        .or_else(|| {
            main.and_then(|v| {
                v.get("tenant_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
        })
        .context("tenant_id is required when main mapping is absent")?;
    let tenant_id = TenantId::from_str(&tenant_id).context("parsing tenant_id")?;
    let timeline_id = req
        .timeline_id
        .as_deref()
        .map(TimelineId::from_str)
        .transpose()
        .context("parsing timeline_id")?;
    let storage_controller = HttpStorageControllerApi::new(
        state
            .storage_controller_url
            .parse()
            .context("parsing storage_controller_url")?,
        state.client.clone(),
    );
    let mut store = DockerStateStore::new(&state.state_dir);
    let output = create_timeline(
        &storage_controller,
        &mut store,
        TimelineCreateOptions {
            tenant_id,
            timeline_id,
            branch_name: req.branch_name,
            pg_version: req.pg_version.unwrap_or(PgMajorVersion::PG14),
        },
    )
    .await?;
    if let Some(safekeepers) = &output.safekeepers {
        save_timeline_safekeepers_info(&state.state_dir, safekeepers)?;
    }
    let mappings = load_mappings(&state.state_dir)?;
    Ok(json!({
        "tenant_id": output.tenant_id,
        "timeline_id": output.timeline_id,
        "branch_name": output.branch_name,
        "last_record_lsn": output.timeline_info.last_record_lsn,
        "safekeepers": output.safekeepers,
        "mappings": mappings,
    }))
}

async fn handle_timeline_delete(
    state: &AppState,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<Value> {
    let storage_controller = docker_storage_controller_api(state)?;
    let nodes = load_pageserver_nodes(state).await?;
    let tenant = storage_controller.tenant_describe(tenant_id).await?;
    let mut deleted_shards = Vec::new();

    for shard in tenant.shards {
        let Some(node_id) = shard.node_attached else {
            continue;
        };
        let Some(node) = nodes.get(&node_id.0) else {
            continue;
        };
        let timelines = list_pageserver_timelines(state, node, shard.tenant_shard_id).await?;
        if timelines
            .iter()
            .any(|timeline| timeline.timeline_id == timeline_id)
        {
            delete_pageserver_timeline(state, node, shard.tenant_shard_id, timeline_id).await?;
            deleted_shards.push(json!({
                "tenant_shard_id": shard.tenant_shard_id.to_string(),
                "pageserver_id": node_id.to_string(),
            }));
        }
    }

    if deleted_shards.is_empty() {
        bail!("timeline {tenant_id}/{timeline_id} was not found on attached pageservers");
    }

    let cleanup = cleanup_deleted_timeline_state(&state.state_dir, tenant_id, timeline_id)?;
    Ok(json!({
        "tenant_id": tenant_id,
        "timeline_id": timeline_id,
        "deleted_shards": deleted_shards,
        "cleanup": cleanup,
    }))
}

async fn handle_timeline_import(state: &AppState, req: TimelineImportRequest) -> Result<Value> {
    let tenant_id = TenantId::from_str(&req.tenant_id).context("parsing tenant_id")?;
    let timeline_id = TimelineId::from_str(&req.timeline_id).context("parsing timeline_id")?;
    let end_lsn = Lsn::from_str(&req.end_lsn).context("parsing end_lsn")?;
    let pg_version = req.pg_version.unwrap_or(PgMajorVersion::PG14);
    let safekeeper_ids = req
        .safekeepers
        .unwrap_or_else(|| configured_safekeeper_ids(state));
    let generation = SafekeeperGeneration::new(req.safekeepers_generation.unwrap_or(1));
    let members = safekeeper_ids
        .iter()
        .map(|id| SafekeeperId {
            host: docker_safekeeper_service_name(*id),
            id: NodeId(*id),
            pg_port: 5454,
        })
        .collect::<Vec<_>>();
    let mconf = Configuration {
        generation,
        members: safekeeper_api::membership::MemberSet { m: members },
        new_members: None,
    };

    for id in &safekeeper_ids {
        create_safekeeper_timeline(
            state,
            *id,
            &SafekeeperTimelineCreateRequest {
                tenant_id,
                timeline_id,
                mconf: mconf.clone(),
                pg_version: PgVersionId::from(pg_version),
                system_id: None,
                wal_seg_size: None,
                start_lsn: end_lsn,
                commit_lsn: None,
            },
        )
        .await?;
    }
    let safekeeper_infos = safekeeper_ids
        .iter()
        .map(|id| SafekeeperInfo {
            id: *id,
            hostname: Some(docker_safekeeper_service_name(*id)),
        })
        .collect::<Vec<_>>();
    save_timeline_safekeepers_value(
        &state.state_dir,
        &req.tenant_id,
        &req.timeline_id,
        generation.into_inner(),
        &safekeeper_infos,
    )?;

    let mut store = DockerStateStore::new(&state.state_dir);
    map_branch(&mut store, req.branch_name.clone(), tenant_id, timeline_id)?;
    let mappings = load_mappings(&state.state_dir)?;
    Ok(json!({
        "tenant_id": tenant_id,
        "timeline_id": timeline_id,
        "branch_name": req.branch_name,
        "end_lsn": end_lsn,
        "safekeepers": safekeeper_ids,
        "safekeepers_generation": generation.into_inner(),
        "mappings": mappings,
    }))
}

fn configured_safekeeper_ids(state: &AppState) -> Vec<u64> {
    let env = read_or_write_default_env(state).unwrap_or_else(|_| json!({}));
    let num_safekeepers = env_u64(&env, "num_safekeepers", 1).max(1);
    (1..=num_safekeepers).collect()
}

async fn create_safekeeper_timeline(
    state: &AppState,
    safekeeper_id: u64,
    req: &SafekeeperTimelineCreateRequest,
) -> Result<()> {
    let service_name = docker_safekeeper_service_name(safekeeper_id);
    let url = format!("http://{service_name}:7676/v1/tenant/timeline");
    state
        .client
        .post(&url)
        .json(req)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("creating timeline on {service_name}"))?;
    Ok(())
}

fn neon_data_dir(state: &AppState) -> Result<PathBuf> {
    state
        .state_dir
        .parent()
        .map(Path::to_path_buf)
        .context("control-plane state_dir has no parent .neon directory")
}

fn handle_pageserver_add(
    state: &AppState,
    req: PageServerAddRequest,
) -> Result<PageServerAddResponse> {
    validate_id(&req.service_name)?;
    let og_version = req.og_version.unwrap_or_else(|| state.og_version.clone());
    let storage_image = req
        .storage_image
        .unwrap_or_else(|| state.storage_image.clone());
    let storage_controller_http = req
        .storage_controller_http
        .unwrap_or_else(|| state.storage_controller_url.clone());
    let broker_endpoint = req
        .broker_endpoint
        .unwrap_or_else(|| "http://storage_broker:50051".to_string());
    let neon_dir = neon_data_dir(state)?;
    let data_dir = neon_dir.join(&req.service_name);
    let config_dir = neon_dir.join(format!("{}_config", req.service_name));
    let override_path = state
        .state_dir
        .join("overrides/pageservers")
        .join(format!("{}.yml", req.service_name));

    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating {}", data_dir.display()))?;
    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("creating {}", config_dir.display()))?;
    std::fs::create_dir_all(neon_dir.join("shared_remote_storage")).with_context(|| {
        format!(
            "creating shared remote storage under {}",
            neon_dir.display()
        )
    })?;
    std::fs::create_dir_all(override_path.parent().unwrap())
        .with_context(|| format!("creating {}", override_path.parent().unwrap().display()))?;

    let rendered = render_pageserver_config(PageServerConfigOptions {
        service_name: req.service_name.clone(),
        node_id: utils::id::NodeId(req.node_id),
        og_version: og_version.clone(),
        storage_controller_http,
        broker_endpoint,
    })?;
    std::fs::write(config_dir.join("identity.toml"), rendered.identity_toml)
        .with_context(|| format!("writing {}/identity.toml", config_dir.display()))?;
    std::fs::write(config_dir.join("pageserver.toml"), rendered.pageserver_toml)
        .with_context(|| format!("writing {}/pageserver.toml", config_dir.display()))?;
    std::fs::write(config_dir.join("metadata.json"), rendered.metadata_json)
        .with_context(|| format!("writing {}/metadata.json", config_dir.display()))?;

    let service_name = &req.service_name;
    let content = format!(
        r#"services:
  {service_name}:
    restart: "no"
    image: ${{NEON_IMAGE:-{storage_image}}}
    pull_policy: never
    environment:
      - OG_VERSION=${{OG_VERSION:-{og_version}}}
      - PATH=/usr/local/${{OG_VERSION:-{og_version}}}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
    ports:
      - {host_http_port}:9898
      - {host_pg_port}:6400
    volumes:
      - ./.neon/{service_name}:/data/.neon/pageserver
      - ./.neon/shared_remote_storage:/data/.neon/shared_remote_storage
      - ./.neon/{service_name}_config/identity.toml:/data/.neon/pageserver/identity.toml:ro
      - ./.neon/{service_name}_config/pageserver.toml:/data/.neon/pageserver/pageserver.toml:ro
      - ./.neon/{service_name}_config/metadata.json:/data/.neon/pageserver/metadata.json:ro
    entrypoint: ["/bin/sh", "-ec"]
    command:
      - |
        exec pageserver -D /data/.neon/pageserver
    depends_on:
      - storage_broker
      - storage_controller
"#,
        host_http_port = req.host_http_port,
        host_pg_port = req.host_pg_port,
    );
    std::fs::write(&override_path, content)
        .with_context(|| format!("writing {}", override_path.display()))?;

    Ok(PageServerAddResponse {
        status: "planned".to_string(),
        service_name: req.service_name.clone(),
        node_id: req.node_id,
        host_http: format!("http://127.0.0.1:{}", req.host_http_port),
        host_pg: format!("127.0.0.1:{}", req.host_pg_port),
        override_file: format!(
            ".neon/control_plane/overrides/pageservers/{}.yml",
            req.service_name
        ),
        compose_files: vec![
            "docker-compose.yml".to_string(),
            format!(
                ".neon/control_plane/overrides/pageservers/{}.yml",
                req.service_name
            ),
        ],
        services: vec![req.service_name],
    })
}

async fn load_pageserver_nodes(state: &AppState) -> Result<HashMap<u64, NodeDescribeResponse>> {
    let nodes = state
        .client
        .get(format!("{}/control/v1/node", state.storage_controller_url))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<NodeDescribeResponse>>()
        .await
        .context("decoding storage_controller node list")?;

    Ok(nodes.into_iter().map(|node| (node.id, node)).collect())
}

async fn tenant_pageserver_connstring(
    state: &AppState,
    tenant_id: &str,
    endpoint_pageserver_id: Option<u64>,
    grpc: bool,
) -> Result<String> {
    if let Some(node_id) = endpoint_pageserver_id {
        let nodes = load_pageserver_nodes(state).await?;
        let node = nodes
            .get(&node_id)
            .ok_or_else(|| anyhow!("pageserver node {node_id} is not registered"))?;
        if grpc {
            let host = node
                .listen_grpc_addr
                .as_deref()
                .context("selected pageserver has no listen_grpc_addr")?;
            let port = node
                .listen_grpc_port
                .context("selected pageserver has no listen_grpc_port")?;
            return Ok(format!("grpc://no_user@{host}:{port}"));
        }
        return Ok(format!(
            "host={} port={}",
            node.listen_pg_addr, node.listen_pg_port
        ));
    }

    let value = proxy_get_json(state, &format!("/debug/v1/tenant/{tenant_id}/locate")).await?;
    let shards = value
        .get("shards")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("tenant {tenant_id} has no located shards"))?;
    if shards.is_empty() {
        bail!("tenant {tenant_id} has no located shards");
    }

    let mut parts = Vec::with_capacity(shards.len());
    for shard in shards {
        if grpc {
            let host = shard
                .get("listen_grpc_addr")
                .and_then(Value::as_str)
                .context("locate response missing listen_grpc_addr")?;
            let port = shard
                .get("listen_grpc_port")
                .and_then(Value::as_u64)
                .context("locate response missing listen_grpc_port")?;
            parts.push(format!("grpc://no_user@{host}:{port}"));
        } else {
            let host = shard
                .get("listen_pg_addr")
                .and_then(Value::as_str)
                .context("locate response missing listen_pg_addr")?;
            let port = shard
                .get("listen_pg_port")
                .and_then(Value::as_u64)
                .context("locate response missing listen_pg_port")?;
            parts.push(format!("host={host} port={port}"));
        }
    }

    Ok(parts.join(","))
}

async fn render_endpoint_config(state: &AppState, endpoint: &EndpointRecord) -> Result<()> {
    ensure_auth_keys(&state.state_dir)?;
    let config: Value =
        serde_json::from_str(BASE_COMPUTE_CONFIG).context("decoding base compute config")?;
    let pageserver_connstring = tenant_pageserver_connstring(
        state,
        &endpoint.tenant_id,
        endpoint.endpoint_pageserver_id,
        endpoint.grpc,
    )
    .await?;
    let mut endpoint = endpoint.clone();
    endpoint.auth_token = Some(generate_storage_auth_token(&state.state_dir, &endpoint)?);
    endpoint.endpoint_storage_addr = Some(
        endpoint
            .endpoint_storage_addr
            .clone()
            .unwrap_or_else(|| "endpoint_storage:9993".to_string()),
    );
    endpoint.endpoint_storage_token = Some(generate_endpoint_storage_token(
        &state.state_dir,
        &endpoint,
        Duration::from_secs(86400),
    )?);
    let mappings = load_mappings(&state.state_dir).unwrap_or_else(|_| json!({}));
    let branch_mapping = mappings.get(&endpoint.branch_name);
    let ancestor_timeline_id = branch_mapping
        .and_then(|mapping| mapping.get("ancestor_timeline_id"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let ancestor_start_lsn = branch_mapping
        .and_then(|mapping| mapping.get("ancestor_start_lsn"))
        .and_then(Value::as_str)
        .unwrap_or("0/0");
    let (safekeeper_connstrings, safekeepers_generation) =
        effective_safekeeper_connstrings(state, &endpoint)?;
    let oggit_safekeeper_http_urls = safekeeper_http_urls_for_connstrings(&safekeeper_connstrings);
    let config = render_endpoint_config_spec(EndpointRenderInput {
        base_config: config,
        tenant_id: endpoint.tenant_id.clone(),
        timeline_id: endpoint.timeline_id.clone(),
        endpoint_id: endpoint.endpoint_id.clone(),
        pageserver_connstring,
        safekeeper_connstrings,
        safekeepers_generation,
        storage_auth_token: endpoint.auth_token.clone(),
        endpoint_storage_addr: endpoint.endpoint_storage_addr.clone(),
        endpoint_storage_token: endpoint.endpoint_storage_token.clone(),
        autoprewarm: endpoint.autoprewarm,
        offload_lfc_interval_seconds: endpoint.offload_lfc_interval_seconds,
        pg_version: endpoint.pg_version,
        static_lsn: endpoint.static_lsn.clone(),
        hot_standby: endpoint.hot_standby,
        enable_oggit: endpoint.enable_oggit,
        oggit_database: endpoint.oggit_database.clone(),
        oggit_ancestor_timeline_id: ancestor_timeline_id.to_string(),
        oggit_branch_start_lsn: ancestor_start_lsn.to_string(),
        oggit_safekeeper_http_urls,
    })?;
    let mut config = config;
    apply_endpoint_record_options(&mut config, &endpoint)?;

    let path = resolve_state_path(&state.state_dir, &endpoint.config_path);
    write_json_file(&path, &config)?;
    write_endpoint_record(&state.state_dir, &endpoint)?;
    Ok(())
}

fn effective_safekeeper_connstrings(
    state: &AppState,
    endpoint: &EndpointRecord,
) -> Result<(Vec<String>, u32)> {
    if let Some(connstrings) = &endpoint.safekeeper_connstrings {
        return Ok((
            connstrings.clone(),
            endpoint.safekeepers_generation.unwrap_or(1),
        ));
    }

    if let Some(value) = load_timeline_safekeepers_value(
        &state.state_dir,
        &endpoint.tenant_id,
        &endpoint.timeline_id,
    )? {
        if let Some((connstrings, generation)) = timeline_safekeeper_connstrings_from_value(&value)
        {
            return Ok((connstrings, generation));
        }
    }

    Ok((default_safekeeper_connstrings(), 1))
}

fn default_safekeeper_connstrings() -> Vec<String> {
    vec!["safekeeper:5454".to_string()]
}

fn safekeeper_http_urls_for_connstrings(connstrings: &[String]) -> String {
    connstrings
        .iter()
        .map(|connstring| {
            let host = connstring
                .split_once(':')
                .map(|(host, _port)| host)
                .unwrap_or(connstring);
            format!("http://{host}:7676")
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn apply_endpoint_record_options(config: &mut Value, endpoint: &EndpointRecord) -> Result<()> {
    let spec = config
        .get_mut("spec")
        .ok_or_else(|| anyhow!("base compute config has no spec field"))?;
    spec["config_only"] = json!(endpoint.config_only);
    spec["grpc"] = json!(endpoint.grpc);
    if let Some(remote_ext_base_url) = &endpoint.remote_ext_base_url {
        spec["remote_ext_base_url"] = json!(remote_ext_base_url);
    }
    if let Some(privileged_role_name) = &endpoint.privileged_role_name {
        spec["privileged_role_name"] = json!(privileged_role_name);
    }
    if endpoint.create_test_user {
        let cluster = spec
            .get_mut("cluster")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("spec.cluster is missing or not an object"))?;
        let roles = cluster
            .entry("roles")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| anyhow!("spec.cluster.roles is not an array"))?;
        if !roles
            .iter()
            .any(|role| role.get("name").and_then(Value::as_str) == Some("test"))
        {
            roles.push(json!({
                "name": "test",
                "encrypted_password": null,
                "options": null
            }));
        }
        let databases = cluster
            .entry("databases")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| anyhow!("spec.cluster.databases is not an array"))?;
        if !databases
            .iter()
            .any(|database| database.get("name").and_then(Value::as_str) == Some("neondb"))
        {
            databases.push(json!({
                "name": "neondb",
                "owner": "test",
                "options": null,
                "restrict_conn": false,
                "invalid": false
            }));
        }
    }
    if let Some(extra_config) = &endpoint.extra_config {
        apply_extra_config(config, extra_config)?;
    }
    let spec = config
        .get_mut("spec")
        .ok_or_else(|| anyhow!("compute config has no spec field after applying extra_config"))?;
    let final_skip_pg_catalog_updates =
        skip_pg_catalog_updates(spec, endpoint.skip_pg_catalog_updates);
    spec["skip_pg_catalog_updates"] = json!(final_skip_pg_catalog_updates);
    Ok(())
}

fn skip_pg_catalog_updates(spec: &Value, endpoint_skip_pg_catalog_updates: bool) -> bool {
    if oggit_enabled(spec) {
        false
    } else {
        endpoint_skip_pg_catalog_updates
    }
}

fn oggit_enabled(spec: &Value) -> bool {
    spec.get("cluster")
        .and_then(|cluster| cluster.get("settings"))
        .and_then(Value::as_array)
        .and_then(|settings| {
            settings.iter().rev().find(|setting| {
                setting.get("name").and_then(Value::as_str) == Some("neon.oggit_enabled")
            })
        })
        .and_then(|setting| setting.get("value"))
        .map_or(false, setting_value_enabled)
}

fn setting_value_enabled(value: &Value) -> bool {
    match value {
        Value::Bool(enabled) => *enabled,
        Value::String(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "on" | "true" | "1" | "yes"
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control_plane_jwt_auth() -> JwtAuth {
        JwtAuth::from_key(DOCKER_DEV_PUBLIC_KEY.to_string()).unwrap()
    }

    fn control_plane_jwt(scope: Scope) -> String {
        let private_key = pem::parse(DOCKER_DEV_PRIVATE_KEY).unwrap();
        encode_from_key_file(&Claims::new(None, scope), &private_key).unwrap()
    }

    #[test]
    fn control_plane_auth_accepts_admin_token() {
        let claims = validate_control_plane_token(
            &control_plane_jwt_auth(),
            &control_plane_jwt(Scope::Admin),
        )
        .unwrap();

        assert_eq!(claims.scope, Scope::Admin);
    }

    #[test]
    fn control_plane_auth_rejects_non_admin_token() {
        let error = validate_control_plane_token(
            &control_plane_jwt_auth(),
            &control_plane_jwt(Scope::Tenant),
        )
        .unwrap_err();

        assert_eq!(error.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn control_plane_auth_rejects_invalid_token() {
        let error = validate_control_plane_token(&control_plane_jwt_auth(), "not-a-jwt").unwrap_err();

        assert_eq!(error.0, StatusCode::UNAUTHORIZED);
    }

    fn spec_with_oggit_setting(value: Value) -> Value {
        json!({
            "cluster": {
                "settings": [{
                    "name": "neon.oggit_enabled",
                    "value": value,
                    "vartype": "bool"
                }]
            }
        })
    }

    #[test]
    fn oggit_on_forces_catalog_updates() {
        let spec = spec_with_oggit_setting(json!("on"));

        assert!(!skip_pg_catalog_updates(&spec, true));
    }

    #[test]
    fn oggit_off_preserves_endpoint_skip_catalog_updates() {
        let spec = spec_with_oggit_setting(json!("off"));

        assert!(skip_pg_catalog_updates(&spec, true));
    }

    #[test]
    fn oggit_enabled_accepts_supported_string_values_ignoring_case() {
        for value in ["ON", "True", "1", "YeS"] {
            let spec = spec_with_oggit_setting(json!(value));

            assert!(oggit_enabled(&spec), "expected {value:?} to enable oggit");
        }
    }

    #[test]
    fn oggit_enabled_accepts_json_boolean_true() {
        let spec = spec_with_oggit_setting(json!(true));

        assert!(oggit_enabled(&spec));
    }

    #[test]
    fn extra_config_is_applied_before_catalog_update_decision() {
        let mut config = json!({
            "spec": {
                "cluster": {
                    "settings": []
                }
            }
        });
        let extra_config = json!({
            "spec.cluster.settings": [{
                "name": "neon.oggit_enabled",
                "value": true,
                "vartype": "bool"
            }]
        });

        apply_extra_config(&mut config, &extra_config).unwrap();

        assert!(!skip_pg_catalog_updates(&config["spec"], true));
    }
}

fn apply_extra_config(config: &mut Value, extra_config: &Value) -> Result<()> {
    let Some(items) = extra_config.as_object() else {
        bail!("extra_config must be a JSON object");
    };
    for (path, value) in items {
        set_json_path(config, path, value.clone())?;
    }
    Ok(())
}

fn set_json_path(root: &mut Value, path: &str, value: Value) -> Result<()> {
    let mut current = root;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if part.is_empty() {
            bail!("empty path segment in extra config key {path:?}");
        }
        if parts.peek().is_none() {
            let object = current
                .as_object_mut()
                .ok_or_else(|| anyhow!("extra config path {path:?} traverses non-object"))?;
            object.insert(part.to_string(), value);
            return Ok(());
        }
        let object = current
            .as_object_mut()
            .ok_or_else(|| anyhow!("extra config path {path:?} traverses non-object"))?;
        current = object.entry(part).or_insert_with(|| json!({}));
    }
    bail!("empty extra config path")
}

fn render_endpoint_override(state: &AppState, endpoint: &EndpointRecord) -> Result<()> {
    let service_name = &endpoint.service_name;
    let endpoint_id = &endpoint.endpoint_id;
    let safekeeper_depends = endpoint_safekeeper_depends(state, endpoint)?
        .into_iter()
        .map(|service| format!("      - {service}\n"))
        .collect::<String>();
    let data_dir = endpoint
        .data_dir
        .clone()
        .unwrap_or_else(|| format!(".neon/{service_name}"));
    let host_pg_port = endpoint.host_pg_port.unwrap_or(55433);
    let host_http_port = endpoint.host_http_port.unwrap_or(3080);
    let internal_http_port = endpoint.internal_http_port.unwrap_or(3080);
    let override_path = state
        .state_dir
        .join("overrides/endpoints")
        .join(format!("{endpoint_id}.yml"));
    let content = format!(
        r#"services:
  {service_name}:
    restart: "no"
    image: ${{COMPUTE_IMAGE:-{compute_image}}}
    pull_policy: never
    environment:
      - OG_VERSION=${{OG_VERSION:-{og_version}}}
      - PATH=/usr/local/${{OG_VERSION:-{og_version}}}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      - OTEL_SDK_DISABLED=true
      - DOCKER_CONTROL_PLANE_MODE=true
      - COMPUTE_CONFIG_FILE=/control_plane/endpoints/{endpoint_id}/config.json
      - ENDPOINT_ID={endpoint_id}
      - ENDPOINT_SERVICE_NAME={service_name}
      - ENDPOINT_STATUS_DIR=/control_plane/endpoints/{endpoint_id}
      - INSTANCE_ID=docker-local-{endpoint_id}
      - COMPUTE_REMOTE_HBA_CIDR=${{COMPUTE_REMOTE_HBA_CIDR:-172.30.0.0/20}}
      - BRANCH_MERGE_USER_ENABLED=${{BRANCH_MERGE_USER_ENABLED:-true}}
      - BRANCH_MERGE_USER=${{BRANCH_MERGE_USER:-branch_merge}}
      - BRANCH_MERGE_PASSWORD=${{BRANCH_MERGE_PASSWORD:-Branch_merge@123}}
      - OGGIT_FDW_USERMAPPING_KEY=${{OGGIT_FDW_USERMAPPING_KEY:-MapKey@123}}
      - STORAGE_CONTROLLER_HTTP=${{STORAGE_CONTROLLER_HTTP:-http://storage_controller:1234}}
    volumes:
      - ./{data_dir}:/var/db/gaussdb
      - ./.neon/control_plane:/control_plane
      - ../compute/shell/compute.sh:/shell/compute.sh:ro
    ports:
      - {host_pg_port}:55433
      - {host_http_port}:{internal_http_port}
    entrypoint: ["/shell/compute.sh"]
    networks:
      default:
        aliases:
          - {service_name}
    depends_on:
{safekeeper_depends}      - pageserver
      - endpoint_storage
"#,
        compute_image = state.compute_image,
        og_version = state.og_version,
    );
    std::fs::create_dir_all(override_path.parent().unwrap())?;
    std::fs::write(&override_path, content)
        .with_context(|| format!("writing {}", override_path.display()))?;
    Ok(())
}

fn endpoint_safekeeper_depends(state: &AppState, endpoint: &EndpointRecord) -> Result<Vec<String>> {
    let (connstrings, _generation) = effective_safekeeper_connstrings(state, endpoint)?;
    let mut services = Vec::new();
    for connstring in connstrings {
        let host = connstring
            .split_once(':')
            .map(|(host, _port)| host)
            .unwrap_or(connstring.as_str());
        let is_local_safekeeper = host == "safekeeper"
            || host
                .strip_prefix("safekeeper")
                .map(|value| !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit()))
                .unwrap_or(false);
        if is_local_safekeeper && !services.iter().any(|service| service == host) {
            services.push(host.to_string());
        }
    }
    if services.is_empty() {
        services.push("safekeeper".to_string());
    }
    Ok(services)
}

fn render_runtime_override(state: &AppState) -> Result<()> {
    let path = state.state_dir.join("overrides/runtime.yml");
    let content = format!(
        "# Runtime services are defined in docker-compose.yml for the current PoC.\n# docker_local includes this file so dynamically generated endpoint overrides\n# can share the same invocation shape.\nservices: {{}}\n# storage_image={}\n# compute_image={}\n",
        state.storage_image, state.compute_image
    );
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, content).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn running_endpoints_for_tenant(state_dir: &Path, tenant_id: &str) -> Result<Vec<EndpointRecord>> {
    let endpoints = load_endpoints(state_dir)?
        .into_iter()
        .filter(|endpoint| endpoint.tenant_id == tenant_id && endpoint.status == "Running")
        .collect();
    Ok(endpoints)
}

fn load_endpoint(state_dir: &Path, endpoint_id: &str) -> Result<EndpointRecord> {
    let path = state_dir
        .join("endpoints")
        .join(endpoint_id)
        .join("endpoint.json");
    let file = std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("decoding {}", path.display()))
}

fn ensure_auth_keys(state_dir: &Path) -> Result<()> {
    let private_key_path = state_dir.join("auth_private_key.pem");
    let public_key_path = state_dir.join("auth_public_key.pem");
    if let Some(parent) = private_key_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    if !private_key_path.exists() {
        std::fs::write(&private_key_path, DOCKER_DEV_PRIVATE_KEY)
            .with_context(|| format!("writing {}", private_key_path.display()))?;
    }
    if !public_key_path.exists() {
        std::fs::write(&public_key_path, DOCKER_DEV_PUBLIC_KEY)
            .with_context(|| format!("writing {}", public_key_path.display()))?;
    }
    DecodingKey::from_ed_pem(
        &std::fs::read(&public_key_path)
            .with_context(|| format!("reading {}", public_key_path.display()))?,
    )
    .context("validating docker dev public key")?;
    let _ = Validation::new(jsonwebtoken::Algorithm::EdDSA);
    Ok(())
}

fn auth_private_pem(state_dir: &Path) -> Result<pem::Pem> {
    ensure_auth_keys(state_dir)?;
    let private_key_path = state_dir.join("auth_private_key.pem");
    pem::parse(
        std::fs::read(&private_key_path)
            .with_context(|| format!("reading {}", private_key_path.display()))?,
    )
    .context("parsing docker dev private key")
}

fn generate_compute_token(
    state_dir: &Path,
    endpoint: &EndpointRecord,
    scope: Option<ComputeClaimsScope>,
) -> Result<String> {
    let claims = ComputeClaims {
        audience: match scope {
            Some(ComputeClaimsScope::Admin) => Some(vec![COMPUTE_AUDIENCE.to_string()]),
            _ => None,
        },
        compute_id: match scope {
            Some(ComputeClaimsScope::Admin) => None,
            _ => Some(endpoint.endpoint_id.clone()),
        },
        scope,
    };
    encode_from_key_file(&claims, &auth_private_pem(state_dir)?)
}

fn generate_storage_auth_token(state_dir: &Path, endpoint: &EndpointRecord) -> Result<String> {
    let tenant_id = TenantId::from_str(&endpoint.tenant_id)
        .with_context(|| format!("parsing tenant id {}", endpoint.tenant_id))?;
    let claims = Claims::new(Some(tenant_id), Scope::Tenant);
    encode_from_key_file(&claims, &auth_private_pem(state_dir)?)
}

fn generate_endpoint_storage_token(
    state_dir: &Path,
    endpoint: &EndpointRecord,
    ttl: Duration,
) -> Result<String> {
    let exp = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)? + ttl).as_secs();
    let claims = EndpointStorageClaims {
        tenant_id: TenantId::from_str(&endpoint.tenant_id)
            .with_context(|| format!("parsing tenant id {}", endpoint.tenant_id))?,
        timeline_id: TimelineId::from_str(&endpoint.timeline_id)
            .with_context(|| format!("parsing timeline id {}", endpoint.timeline_id))?,
        endpoint_id: EndpointId::from(endpoint.endpoint_id.clone()),
        exp,
    };
    encode_from_key_file(&claims, &auth_private_pem(state_dir)?)
}

fn branch_endpoint_connstr(
    endpoint: &EndpointRecord,
    user: &str,
    database: &str,
) -> Result<String> {
    let password = if user == "branch_merge" {
        std::env::var("BRANCH_MERGE_PASSWORD").unwrap_or_else(|_| "Branch_merge@123".to_string())
    } else {
        std::env::var("BRANCH_TARGET_PASSWORD").unwrap_or_default()
    };
    let escaped_password = urlencoding::encode(&password);
    let compute_pg = endpoint
        .compute_pg
        .clone()
        .unwrap_or_else(|| format!("{}:55433", endpoint.service_name));
    let auth = if password.is_empty() {
        user.to_string()
    } else {
        format!("{user}:{escaped_password}")
    };
    Ok(format!(
        "postgresql://{auth}@{compute_pg}/{database}?sslmode=disable"
    ))
}

fn branch_target_endpoint_ref_by_endpoint(
    state: &AppState,
    endpoint_id: &str,
    user: &str,
    database: &str,
) -> Result<BranchEndpointRef> {
    let endpoint = load_endpoint(&state.state_dir, endpoint_id)?;
    if endpoint.status != "Running" {
        bail!("endpoint {endpoint_id} is not Running");
    }
    let connstr = branch_endpoint_connstr(&endpoint, user, database)?;
    BranchEndpointRef::from_connstr(
        &connstr,
        Some(&endpoint.branch_name),
        endpoint_id,
        user,
        database,
    )
}

fn branch_target_endpoint_ref_by_branch(
    state: &AppState,
    branch_name: &str,
    tenant_id: Option<&str>,
    user: &str,
    database: &str,
) -> Result<BranchEndpointRef> {
    let endpoints = load_endpoints(&state.state_dir)?;
    let matches = endpoints
        .iter()
        .filter(|endpoint| {
            endpoint.branch_name == branch_name
                && endpoint.status == "Running"
                && tenant_id.map_or(true, |tenant_id| endpoint.tenant_id == tenant_id)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [endpoint] => {
            let connstr = branch_endpoint_connstr(endpoint, user, database)?;
            BranchEndpointRef::from_connstr(
                &connstr,
                Some(&endpoint.branch_name),
                &endpoint.endpoint_id,
                user,
                database,
            )
        }
        [] => bail!("no Running endpoint found for branch {branch_name}"),
        _ => bail!(
            "multiple Running endpoints found for branch {branch_name}; use --target-endpoint"
        ),
    }
}

fn branch_target_endpoint_ref_from_selector(
    state: &AppState,
    tenant_id: Option<&str>,
    target_branch: Option<&str>,
    target_endpoint: Option<&str>,
    target_connstr: Option<&str>,
    user: Option<&str>,
    database: Option<&str>,
) -> Result<BranchEndpointRef> {
    let user = user.unwrap_or("branch_merge");
    let database = database.context("database is required")?;
    let selector_count = [
        target_branch.is_some(),
        target_endpoint.is_some(),
        target_connstr.is_some(),
    ]
    .into_iter()
    .filter(|selected| *selected)
    .count();
    if selector_count != 1 {
        bail!("specify exactly one of --target-branch, --target-endpoint, or --target-connstr");
    }

    if let Some(connstr) = target_connstr {
        let branch_name = target_branch.map(str::to_string);
        return BranchEndpointRef::from_connstr(
            connstr,
            branch_name.as_ref(),
            "target",
            user,
            database,
        );
    }
    if let Some(endpoint_id) = target_endpoint {
        let endpoint = load_endpoint(&state.state_dir, endpoint_id)?;
        if let Some(tenant_id) = tenant_id {
            if endpoint.tenant_id != tenant_id {
                bail!(
                    "endpoint {endpoint_id} belongs to tenant {}, not {tenant_id}",
                    endpoint.tenant_id
                );
            }
        }
        if let Some(branch_name) = target_branch {
            if endpoint.branch_name != branch_name {
                bail!(
                    "endpoint {endpoint_id} runs branch {}, not {branch_name}",
                    endpoint.branch_name
                );
            }
        }
        return branch_target_endpoint_ref_by_endpoint(state, endpoint_id, user, database);
    }
    branch_target_endpoint_ref_by_branch(
        state,
        target_branch.context("target_branch is required")?,
        tenant_id,
        user,
        database,
    )
}

fn branch_target_endpoint_ref_from_request(
    state: &AppState,
    req: &BranchTargetRequest,
) -> Result<BranchEndpointRef> {
    branch_target_endpoint_ref_from_selector(
        state,
        req.tenant_id.as_deref(),
        req.target_branch.as_deref(),
        req.target_endpoint.as_deref(),
        req.target_connstr.as_deref(),
        req.user.as_deref(),
        req.database.as_deref(),
    )
}

fn branch_run_endpoint_refs(
    state: &AppState,
    req: &BranchRunRequest,
) -> Result<(BranchEndpointRef, BranchEndpointRef)> {
    let source_endpoint_id = req
        .source_endpoint
        .as_deref()
        .context("source_endpoint is required")?;
    let target_endpoint_id = req
        .target_endpoint
        .as_deref()
        .context("target_endpoint is required")?;
    Ok((
        branch_target_endpoint_ref_by_endpoint(
            state,
            source_endpoint_id,
            "branch_merge",
            &req.database,
        )?,
        branch_target_endpoint_ref_by_endpoint(
            state,
            target_endpoint_id,
            "branch_merge",
            &req.database,
        )?,
    ))
}

fn branch_target_request_options(
    state: &AppState,
    merge_id: String,
    req: BranchTargetRequest,
) -> Result<BranchResolveOptions> {
    let target_endpoint = branch_target_endpoint_ref_from_request(state, &req)?;
    let conflict_id = req.conflict_id.context("conflict_id is required")?;
    let resolution = req
        .resolution
        .as_deref()
        .map(BranchConflictResolution::from_str)
        .transpose()?;
    Ok(BranchResolveOptions {
        target_endpoint,
        merge_id,
        conflict_id,
        resolution,
        custom_sql: req.custom_sql,
    })
}

fn write_endpoint_record(state_dir: &Path, endpoint: &EndpointRecord) -> Result<()> {
    let dir = state_dir.join("endpoints").join(&endpoint.endpoint_id);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    write_json_file(&dir.join("endpoint.json"), endpoint)
}

fn delete_endpoint_record(state_dir: &Path, endpoint_id: &str) -> Result<Value> {
    validate_id(endpoint_id)?;
    let endpoint = load_endpoint(state_dir, endpoint_id)?;
    let endpoint_dir = state_dir.join("endpoints").join(endpoint_id);
    let override_path = state_dir
        .join("overrides/endpoints")
        .join(format!("{endpoint_id}.yml"));
    if endpoint_dir.exists() {
        std::fs::remove_dir_all(&endpoint_dir)
            .with_context(|| format!("removing {}", endpoint_dir.display()))?;
    }
    if override_path.exists() {
        std::fs::remove_file(&override_path)
            .with_context(|| format!("removing {}", override_path.display()))?;
    }
    Ok(json!({
        "endpoint_id": endpoint_id,
        "service_name": endpoint.service_name,
        "removed": true
    }))
}

fn update_endpoint_status(
    state_dir: &Path,
    endpoint_id: &str,
    status: &str,
    last_lsn: Option<String>,
) -> Result<EndpointRecord> {
    let mut endpoint = load_endpoint(state_dir, endpoint_id)?;
    endpoint.status = status.to_string();
    if last_lsn.is_some() {
        endpoint.last_lsn = last_lsn;
    }
    write_endpoint_record(state_dir, &endpoint)?;
    Ok(endpoint)
}

fn load_mappings(state_dir: &Path) -> Result<Value> {
    let path = state_dir.join("branches.json");
    if !path.exists() {
        return Ok(json!({}));
    }
    let file = std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("decoding {}", path.display()))
}

fn upsert_mapping(
    state_dir: &Path,
    branch_name: &str,
    tenant_id: &str,
    timeline_id: &str,
) -> Result<Value> {
    upsert_mapping_with_extra(state_dir, branch_name, tenant_id, timeline_id, json!({}))
}

fn upsert_mapping_with_extra(
    state_dir: &Path,
    branch_name: &str,
    tenant_id: &str,
    timeline_id: &str,
    extra: Value,
) -> Result<Value> {
    let mut mappings = load_mappings(state_dir)?;
    let object = mappings
        .as_object_mut()
        .ok_or_else(|| anyhow!("branches.json root is not an object"))?;
    let mut mapping = object
        .get(branch_name)
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some(mapping_obj) = mapping.as_object_mut() {
        if let (Some(existing_tenant_id), Some(existing_timeline_id)) = (
            mapping_obj.get("tenant_id").and_then(Value::as_str),
            mapping_obj.get("timeline_id").and_then(Value::as_str),
        ) {
            if existing_tenant_id != tenant_id {
                bail!(
                    "branch '{branch_name}' is already mapped for tenant {existing_tenant_id}, cannot map it for another tenant {tenant_id}"
                );
            }
            if existing_timeline_id != timeline_id {
                bail!(
                    "branch '{branch_name}' is already mapped to timeline {existing_timeline_id}, cannot map to another timeline {timeline_id}"
                );
            }
        }
        mapping_obj.insert(
            "tenant_id".to_string(),
            Value::String(tenant_id.to_string()),
        );
        mapping_obj.insert(
            "timeline_id".to_string(),
            Value::String(timeline_id.to_string()),
        );
        if let Some(extra_obj) = extra.as_object() {
            for (key, value) in extra_obj {
                mapping_obj.insert(key.clone(), value.clone());
            }
        }
    }
    object.insert(branch_name.to_string(), mapping);
    write_json_file(&state_dir.join("branches.json"), &mappings)?;
    Ok(mappings)
}

fn timeline_safekeepers_path(state_dir: &Path, tenant_id: &str, timeline_id: &str) -> PathBuf {
    state_dir
        .join("timeline_safekeepers")
        .join(format!("{tenant_id}_{timeline_id}.json"))
}

fn save_timeline_safekeepers_info(state_dir: &Path, info: &SafekeepersInfo) -> Result<()> {
    write_json_file(
        &timeline_safekeepers_path(
            state_dir,
            &info.tenant_id.to_string(),
            &info.timeline_id.to_string(),
        ),
        info,
    )
}

fn save_timeline_safekeepers_value(
    state_dir: &Path,
    tenant_id: &str,
    timeline_id: &str,
    generation: u32,
    safekeepers: &[SafekeeperInfo],
) -> Result<()> {
    write_json_file(
        &timeline_safekeepers_path(state_dir, tenant_id, timeline_id),
        &json!({
            "tenant_id": tenant_id,
            "timeline_id": timeline_id,
            "generation": generation,
            "safekeepers": safekeepers,
        }),
    )
}

fn load_timeline_safekeepers_value(
    state_dir: &Path,
    tenant_id: &str,
    timeline_id: &str,
) -> Result<Option<Value>> {
    let path = timeline_safekeepers_path(state_dir, tenant_id, timeline_id);
    if !path.exists() {
        return Ok(None);
    }
    let file = std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file)
        .map(Some)
        .with_context(|| format!("decoding {}", path.display()))
}

fn safekeeper_host_from_id(id: u64) -> String {
    docker_safekeeper_service_name(id)
}

fn timeline_safekeeper_connstrings_from_value(value: &Value) -> Option<(Vec<String>, u32)> {
    let generation = value.get("generation").and_then(Value::as_u64).unwrap_or(1) as u32;
    let safekeepers = value.get("safekeepers")?.as_array()?;
    let connstrings = safekeepers
        .iter()
        .filter_map(|sk| {
            let id = sk.get("id").and_then(Value::as_u64)?;
            let host = sk
                .get("hostname")
                .and_then(Value::as_str)
                .filter(|host| !host.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| safekeeper_host_from_id(id));
            Some(format!("{host}:5454"))
        })
        .collect::<Vec<_>>();
    (!connstrings.is_empty()).then_some((connstrings, generation))
}

async fn proxy_get_json(state: &AppState, path: &str) -> Result<Value> {
    state
        .client
        .get(format!("{}{}", state.storage_controller_url, path))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decoding storage_controller GET {path}"))
}

fn write_json_file<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("writing {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        bail!("invalid id {id:?}; use only ASCII letters, digits, '-' and '_'");
    }
    Ok(())
}

fn sanitize_service_name(id: &str) -> String {
    id.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b == b'_' {
                b as char
            } else {
                '_'
            }
        })
        .collect()
}

fn load_endpoints(state_dir: &Path) -> Result<Vec<EndpointRecord>> {
    let endpoints_dir = state_dir.join("endpoints");
    if !endpoints_dir.exists() {
        return Ok(Vec::new());
    }

    let mut endpoints = Vec::new();
    for entry in std::fs::read_dir(&endpoints_dir)
        .with_context(|| format!("reading {}", endpoints_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path().join("endpoint.json");
        if !path.exists() {
            continue;
        }
        let file =
            std::fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let endpoint: EndpointRecord = serde_json::from_reader(file)
            .with_context(|| format!("decoding {}", path.display()))?;
        endpoints.push(endpoint);
    }
    Ok(endpoints)
}

fn update_endpoint_config<F>(
    state_dir: &Path,
    endpoint: &EndpointRecord,
    mut update: F,
) -> Result<()>
where
    F: FnMut(&mut Value) -> Result<()>,
{
    let config_path = resolve_state_path(state_dir, &endpoint.config_path);
    let file = std::fs::File::open(&config_path)
        .with_context(|| format!("opening {}", config_path.display()))?;
    let mut config: Value = serde_json::from_reader(file)
        .with_context(|| format!("decoding {}", config_path.display()))?;
    let spec = config
        .get_mut("spec")
        .ok_or_else(|| anyhow!("{} has no spec field", config_path.display()))?;
    update(spec)?;

    let tmp_path = config_path.with_extension("json.tmp");
    std::fs::write(&tmp_path, serde_json::to_vec_pretty(&config)?)
        .with_context(|| format!("writing {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &config_path).with_context(|| {
        format!(
            "renaming {} to {}",
            tmp_path.display(),
            config_path.display()
        )
    })?;
    Ok(())
}

fn resolve_state_path(state_dir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        if let Ok(stripped) = path.strip_prefix("/control_plane") {
            return state_dir.join(stripped);
        }
        if let Ok(stripped) = path.strip_prefix("/data/.neon/control_plane") {
            return state_dir.join(stripped);
        }
        path
    } else if let Ok(stripped) = path.strip_prefix(".neon/control_plane") {
        state_dir.join(stripped)
    } else {
        state_dir.join(path)
    }
}

fn upsert_setting(spec: &mut Value, name: &str, value: &str, vartype: &str) -> Result<()> {
    let settings = spec
        .get_mut("cluster")
        .and_then(|cluster| cluster.get_mut("settings"))
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow!("spec.cluster.settings is missing or not an array"))?;

    settings.retain(|setting| setting.get("name").and_then(Value::as_str) != Some(name));
    settings.push(json!({
        "name": name,
        "value": value,
        "vartype": vartype,
    }));
    Ok(())
}

async fn post_configure(state: &AppState, endpoint: &EndpointRecord) -> Result<()> {
    let config_path = resolve_state_path(&state.state_dir, &endpoint.config_path);
    let config: Value = serde_json::from_reader(
        std::fs::File::open(&config_path)
            .with_context(|| format!("opening {}", config_path.display()))?,
    )
    .with_context(|| format!("decoding {}", config_path.display()))?;

    let response = state
        .client
        .post(format!(
            "{}/configure",
            endpoint.compute_http.trim_end_matches('/')
        ))
        .json(&config)
        .send()
        .await
        .with_context(|| format!("posting /configure to {}", endpoint.compute_http))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!(
            "compute {} rejected /configure with {}: {}",
            endpoint.endpoint_id,
            status,
            body
        );
    }
    Ok(())
}

fn error_response(status: StatusCode, err: anyhow::Error) -> Response {
    let causes = err.chain().map(ToString::to_string).collect::<Vec<_>>();
    let error = causes
        .first()
        .cloned()
        .unwrap_or_else(|| "unknown error".to_string());
    (status, Json(json!({ "error": error, "causes": causes }))).into_response()
}
