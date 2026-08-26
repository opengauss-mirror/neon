//!
//! `neon_local` is an executable that can be used to create a local
//! Neon environment, for testing purposes. The local environment is
//! quite different from the cloud environment with Kubernetes, but it
//! easier to work with locally. The python tests in `test_runner`
//! rely on `neon_local` to set up the environment for each test.
//!
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use compute_api::requests::ComputeClaimsScope;
use compute_api::spec::{ComputeMode, PageserverProtocol};
use control_plane::broker::StorageBroker;
use control_plane::endpoint::{
    ComputeControlPlane, Endpoint, EndpointStatus, EndpointTerminateMode,
};
use control_plane::endpoint_storage::{ENDPOINT_STORAGE_DEFAULT_ADDR, EndpointStorage};
use control_plane::local_env;
use control_plane::local_env::{
    EndpointStorageConf, InitForceMode, LocalEnv, NeonBroker, NeonLocalInitConf,
    NeonLocalInitPageserverConf, SafekeeperConf,
};
use control_plane::merge_oggit::{OggitGcParentRequest, OggitGcParentResult, oggit_gc_parent};
use control_plane::ops::endpoint::compute_mode_from_options;
use control_plane::ops::tenant::{TenantCreateOptions, create_tenant};
use control_plane::ops::timeline::{
    TimelineBranchOptions, TimelineCreateOptions, branch_timeline, create_timeline,
};
use control_plane::ops::{
    branch as branch_ops,
    branch::{
        BranchCommandOutput, BranchDiffOptions, BranchMergeOptions, BranchResolveOptions,
        BranchTargetOptions,
    },
};
use control_plane::pageserver::PageServerNode;
use control_plane::safekeeper::SafekeeperNode;
use control_plane::storage_controller::{
    NeonStorageControllerStartArgs, NeonStorageControllerStopArgs, StorageController,
};
use nix::fcntl::{Flock, FlockArg};
use pageserver_api::config::{
    DEFAULT_GRPC_LISTEN_PORT as DEFAULT_PAGESERVER_GRPC_PORT,
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_PAGESERVER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_PAGESERVER_PG_PORT,
};
use pageserver_api::controller_api::{NodeAvailabilityWrapper, PlacementPolicy};
use pageserver_api::models::{TenantConfigRequest, TenantWaitLsnRequest, TimelineInfo};
use pageserver_api::shard::{DEFAULT_STRIPE_SIZE, TenantShardId};
use postgres_backend::AuthType;
use postgres_connection::parse_host_port;
use safekeeper_api::membership::{SafekeeperGeneration, SafekeeperId};
use safekeeper_api::{
    DEFAULT_HTTP_LISTEN_PORT as DEFAULT_SAFEKEEPER_HTTP_PORT,
    DEFAULT_PG_LISTEN_PORT as DEFAULT_SAFEKEEPER_PG_PORT, PgMajorVersion, PgVersionId,
};
use storage_broker::DEFAULT_LISTEN_ADDR as DEFAULT_BROKER_ADDR;
use tokio::task::JoinSet;
use tokio_opengauss::NoTls;
use url::Host;
use utils::auth::{Claims, Scope};
use utils::id::{NodeId, TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;
use utils::project_git_version;

// Default id of a safekeeper node, if not specified on the command line.
const DEFAULT_SAFEKEEPER_ID: NodeId = NodeId(1);
const DEFAULT_PAGESERVER_ID: NodeId = NodeId(1);
const DEFAULT_BRANCH_NAME: &str = "main";
project_git_version!(GIT_VERSION);

#[allow(dead_code)]
const DEFAULT_PG_VERSION: PgMajorVersion = PgMajorVersion::PG14;
const DEFAULT_PG_VERSION_NUM: &str = "14";

const DEFAULT_PAGESERVER_CONTROL_PLANE_API: &str = "http://127.0.0.1:1234/upcall/v1/";

#[derive(clap::Parser)]
#[command(version = GIT_VERSION, about, name = "Neon CLI")]
struct Cli {
    #[command(subcommand)]
    command: NeonLocalCmd,
}

#[derive(clap::Subcommand)]
enum NeonLocalCmd {
    Init(InitCmdArgs),

    #[command(subcommand)]
    Tenant(TenantCmd),
    #[command(subcommand)]
    Timeline(TimelineCmd),
    #[command(subcommand)]
    Pageserver(PageserverCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_controller")]
    StorageController(StorageControllerCmd),
    #[command(subcommand)]
    #[clap(alias = "storage_broker")]
    StorageBroker(StorageBrokerCmd),
    #[command(subcommand)]
    Safekeeper(SafekeeperCmd),
    #[command(subcommand)]
    EndpointStorage(EndpointStorageCmd),
    #[command(subcommand)]
    Endpoint(EndpointCmd),
    #[command(subcommand)]
    Mappings(MappingsCmd),
    #[command(subcommand)]
    Branch(BranchCmd),
    #[command(subcommand)]
    Oggit(OggitCmd),

    Start(StartCmdArgs),
    Stop(StopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Initialize a new Neon repository, preparing configs for services to start with")]
struct InitCmdArgs {
    #[clap(long, help("How many pageservers to create (default 1)"))]
    num_pageservers: Option<u16>,

    #[clap(long)]
    config: Option<PathBuf>,

    #[clap(long, help("Force initialization even if the repository is not empty"))]
    #[arg(value_parser)]
    #[clap(default_value = "must-not-exist")]
    force: InitForceMode,
}

#[derive(clap::Args)]
#[clap(about = "Start pageserver and safekeepers")]
struct StartCmdArgs {
    #[clap(long = "start-timeout", default_value = "10s")]
    timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop pageserver and safekeepers")]
struct StopCmdArgs {
    #[arg(value_enum)]
    #[clap(long, default_value_t = StopMode::Fast)]
    mode: StopMode,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum StopMode {
    Fast,
    Immediate,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage tenants")]
enum TenantCmd {
    List,
    Create(TenantCreateCmdArgs),
    SetDefault(TenantSetDefaultCmdArgs),
    Config(TenantConfigCmdArgs),
    Import(TenantImportCmdArgs),
}

#[derive(clap::Args)]
struct TenantCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(
        long,
        help = "Use a specific timeline id when creating a tenant and its initial timeline"
    )]
    timeline_id: Option<TimelineId>,

    #[clap(short = 'c')]
    config: Vec<String>,

    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long, help = "Postgres version to use for the initial timeline")]
    pg_version: PgMajorVersion,

    #[clap(
        long,
        help = "Use this tenant in future CLI commands where tenant_id is needed, but not specified"
    )]
    set_default: bool,

    #[clap(long, help = "Number of shards in the new tenant")]
    #[arg(default_value_t = 0)]
    shard_count: u8,
    #[clap(long, help = "Sharding stripe size in pages")]
    shard_stripe_size: Option<u32>,

    #[clap(long, help = "Placement policy shards in this tenant")]
    #[arg(value_parser = parse_placement_policy)]
    placement_policy: Option<PlacementPolicy>,
}

fn parse_placement_policy(s: &str) -> anyhow::Result<PlacementPolicy> {
    Ok(serde_json::from_str::<PlacementPolicy>(s)?)
}

#[derive(clap::Args)]
#[clap(
    about = "Set a particular tenant as default in future CLI commands where tenant_id is needed, but not specified"
)]
struct TenantSetDefaultCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
}

#[derive(clap::Args)]
struct TenantConfigCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(short = 'c')]
    config: Vec<String>,
}

#[derive(clap::Args)]
#[clap(
    about = "Import a tenant that is present in remote storage, and create branches for its timelines"
)]
struct TenantImportCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage timelines")]
enum TimelineCmd {
    List(TimelineListCmdArgs),
    Branch(TimelineBranchCmdArgs),
    Create(TimelineCreateCmdArgs),
    Import(TimelineImportCmdArgs),
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage oggit metadata")]
enum OggitCmd {
    Gc(OggitGcCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Garbage collect old oggit change_log/object_change rows on root parent timelines")]
struct OggitGcCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(
        long,
        default_value_t = 16 * 1024 * 1024,
        help = "LSN bytes retained on root parents without live direct children"
    )]
    retention_lsn_distance: u64,
}

#[derive(clap::Subcommand)]
#[clap(about = "Diff and merge local Neon branches through running endpoints")]
enum BranchCmd {
    Diff(BranchDiffCmdArgs),
    Merge(BranchMergeCmdArgs),
    MergeStatus(BranchMergeStatusCmdArgs),
    Conflicts(BranchConflictsCmdArgs),
    Resolve(BranchResolveCmdArgs),
    Continue(BranchContinueCmdArgs),
    Abort(BranchAbortCmdArgs),
}

impl BranchCmd {
    fn uses_only_connstrs(&self) -> bool {
        match self {
            BranchCmd::Diff(args) => args.source_connstr.is_some() && args.target_connstr.is_some(),
            BranchCmd::Merge(args) => {
                args.source_connstr.is_some() && args.target_connstr.is_some()
            }
            BranchCmd::MergeStatus(args) => args.target_connstr.is_some(),
            BranchCmd::Conflicts(args) => args.target_connstr.is_some(),
            BranchCmd::Resolve(args) => args.target_connstr.is_some(),
            BranchCmd::Continue(args) => args.target_connstr.is_some(),
            BranchCmd::Abort(args) => args.target_connstr.is_some(),
        }
    }
}

#[derive(clap::Args)]
#[clap(about = "Diff a source branch against a target branch")]
struct BranchDiffCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Source branch name")]
    source_branch: Option<String>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the source branch")]
    source_endpoint: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external source endpoint")]
    source_connstr: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(
        long,
        default_value = "public",
        help = "Remote schema to import from source branch"
    )]
    source_schema: String,

    #[clap(long, default_value = "public", help = "Target schema to compare")]
    target_schema: String,

    #[clap(long, help = "Database name on both endpoints")]
    database: String,

    #[clap(
        long,
        default_value = "cloud_admin",
        help = "Database user on both endpoints"
    )]
    user: String,

    #[clap(
        long,
        default_value = "neon_merge_src",
        help = "Temporary FDW server on target"
    )]
    fdw_server: String,

    #[clap(
        long,
        help = "Keep the temporary FDW schema and server after the command"
    )]
    keep_fdw: bool,

    #[clap(
        long,
        help = "Use oggit incremental metadata instead of full-table FDW diff"
    )]
    incremental_oggit: bool,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub(crate) enum BranchMergeStrategy {
    Fail,
    Ours,
    Theirs,
    Manual,
}

#[derive(clap::Args)]
#[clap(about = "Merge a source branch into a target branch")]
struct BranchMergeCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Source branch name")]
    source_branch: Option<String>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the source branch")]
    source_endpoint: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external source endpoint")]
    source_connstr: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(
        long,
        default_value = "public",
        help = "Remote schema to import from source branch"
    )]
    source_schema: String,

    #[clap(long, default_value = "public", help = "Target schema to merge into")]
    target_schema: String,

    #[clap(long, help = "Database name on both endpoints")]
    database: String,

    #[clap(
        long,
        default_value = "cloud_admin",
        help = "Database user on both endpoints"
    )]
    user: String,

    #[clap(
        long,
        value_enum,
        default_value = "fail",
        help = "Conflict strategy: fail, ours, theirs, or manual"
    )]
    strategy: BranchMergeStrategy,

    #[clap(
        long,
        default_value = "neon_merge_src",
        help = "Temporary FDW server on target"
    )]
    fdw_server: String,

    #[clap(long, help = "Do not copy tables that exist only on the source branch")]
    no_copy_source_only_tables: bool,

    #[clap(
        long,
        help = "Keep the temporary FDW schema and server after the command"
    )]
    keep_fdw: bool,

    #[clap(
        long,
        help = "Use oggit incremental metadata instead of full-table FDW merge"
    )]
    incremental_oggit: bool,
}

#[derive(clap::Args)]
#[clap(about = "Show status for an oggit incremental merge")]
struct BranchMergeStatusCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(long, help = "Merge id returned by branch merge")]
    merge_id: String,

    #[clap(long, help = "Database name")]
    database: String,

    #[clap(long, default_value = "cloud_admin", help = "Database user")]
    user: String,
}

#[derive(clap::Args)]
#[clap(about = "List conflicts for a blocked oggit incremental merge")]
struct BranchConflictsCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(long, help = "Merge id returned by branch merge")]
    merge_id: String,

    #[clap(long, help = "Database name")]
    database: String,

    #[clap(long, default_value = "cloud_admin", help = "Database user")]
    user: String,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum BranchConflictResolution {
    Ours,
    Theirs,
    Skip,
}

#[derive(clap::Args)]
#[clap(about = "Resolve one conflict in a blocked oggit incremental merge")]
struct BranchResolveCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(long, help = "Merge id returned by branch merge")]
    merge_id: String,

    #[clap(long, help = "Conflict id from branch conflicts")]
    conflict_id: i64,

    #[clap(long, value_enum, help = "Resolution: ours, theirs, or skip")]
    resolution: Option<BranchConflictResolution>,

    #[clap(
        long,
        conflicts_with = "resolution",
        help = "Custom SQL to run during continue"
    )]
    custom_sql: Option<String>,

    #[clap(long, help = "Database name")]
    database: String,

    #[clap(long, default_value = "cloud_admin", help = "Database user")]
    user: String,
}

#[derive(clap::Args)]
#[clap(about = "Continue a blocked oggit incremental merge after conflicts are resolved")]
struct BranchContinueCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(long, help = "Merge id returned by branch merge")]
    merge_id: String,

    #[clap(long, help = "Database name")]
    database: String,

    #[clap(long, default_value = "cloud_admin", help = "Database user")]
    user: String,
}

#[derive(clap::Args)]
#[clap(about = "Abort a blocked oggit incremental merge")]
struct BranchAbortCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "Target branch name")]
    target_branch: Option<String>,

    #[clap(long, help = "Running endpoint id for the target branch")]
    target_endpoint: Option<String>,

    #[clap(long, help = "Connection string for an external target endpoint")]
    target_connstr: Option<String>,

    #[clap(long, help = "Merge id returned by branch merge")]
    merge_id: String,

    #[clap(long, help = "Database name")]
    database: String,

    #[clap(long, default_value = "cloud_admin", help = "Database user")]
    user: String,
}

#[derive(clap::Args)]
#[clap(about = "List all timelines available to this pageserver")]
struct TimelineListCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_shard_id: Option<TenantShardId>,
}

#[derive(clap::Args)]
#[clap(about = "Create a new timeline, branching off from another timeline")]
struct TimelineBranchCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: Option<TimelineId>,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[clap(
        long,
        help = "Use last Lsn of another timeline (and its data) as base when creating the new timeline. The timeline gets resolved by its branch name."
    )]
    ancestor_branch_name: Option<String>,

    #[clap(
        long,
        help = "When using another timeline as base, use a specific Lsn in it instead of the latest one"
    )]
    ancestor_start_lsn: Option<Lsn>,
}

#[derive(clap::Args)]
#[clap(about = "Create a new blank timeline")]
struct TimelineCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: Option<TimelineId>,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long, help = "Postgres version")]
    pg_version: PgMajorVersion,
}

#[derive(clap::Args)]
#[clap(about = "Import timeline from a basebackup directory")]
struct TimelineImportCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(long, help = "New timeline's ID")]
    timeline_id: TimelineId,

    #[clap(long, help = "Human-readable alias for the new timeline")]
    branch_name: String,

    #[clap(long, help = "Basebackup tarfile to import")]
    base_tarfile: PathBuf,

    #[clap(long, help = "Lsn the basebackup starts at")]
    base_lsn: Lsn,

    #[clap(long, help = "Wal to add after base")]
    wal_tarfile: Option<PathBuf>,

    #[clap(long, help = "Lsn the basebackup ends at")]
    end_lsn: Option<Lsn>,

    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long, help = "Postgres version of the backup being imported")]
    pg_version: PgMajorVersion,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage pageservers")]
enum PageserverCmd {
    Status(PageserverStatusCmdArgs),
    Start(PageserverStartCmdArgs),
    Stop(PageserverStopCmdArgs),
    Restart(PageserverRestartCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Show status of a local pageserver")]
struct PageserverStatusCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,
}

#[derive(clap::Args)]
#[clap(about = "Start local pageserver")]
struct PageserverStartCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop local pageserver")]
struct PageserverStopCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

#[derive(clap::Args)]
#[clap(about = "Restart local pageserver")]
struct PageserverRestartCmdArgs {
    #[clap(long = "id", help = "pageserver id")]
    pageserver_id: Option<NodeId>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage storage controller")]
enum StorageControllerCmd {
    Start(StorageControllerStartCmdArgs),
    Stop(StorageControllerStopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Start storage controller")]
struct StorageControllerStartCmdArgs {
    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,

    #[clap(
        long,
        help = "Identifier used to distinguish storage controller instances"
    )]
    #[arg(default_value_t = 1)]
    instance_id: u8,

    #[clap(
        long,
        help = "Base port for the storage controller instance idenfified by instance-id (defaults to pageserver cplane api)"
    )]
    base_port: Option<u16>,

    #[clap(
        long,
        help = "Whether the storage controller should handle pageserver-reported local disk loss events."
    )]
    handle_ps_local_disk_loss: Option<bool>,
}

#[derive(clap::Args)]
#[clap(about = "Stop storage controller")]
struct StorageControllerStopCmdArgs {
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,

    #[clap(
        long,
        help = "Identifier used to distinguish storage controller instances"
    )]
    #[arg(default_value_t = 1)]
    instance_id: u8,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage storage broker")]
enum StorageBrokerCmd {
    Start(StorageBrokerStartCmdArgs),
    Stop(StorageBrokerStopCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Start broker")]
struct StorageBrokerStartCmdArgs {
    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "stop broker")]
struct StorageBrokerStopCmdArgs {
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    #[arg(value_enum, default_value = "fast")]
    stop_mode: StopMode,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage safekeepers")]
enum SafekeeperCmd {
    Start(SafekeeperStartCmdArgs),
    Stop(SafekeeperStopCmdArgs),
    Restart(SafekeeperRestartCmdArgs),
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage object storage")]
enum EndpointStorageCmd {
    Start(EndpointStorageStartCmd),
    Stop(EndpointStorageStopCmd),
}

#[derive(clap::Args)]
#[clap(about = "Start object storage")]
struct EndpointStorageStartCmd {
    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop object storage")]
struct EndpointStorageStopCmd {
    #[arg(value_enum, default_value = "fast")]
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    stop_mode: StopMode,
}

#[derive(clap::Args)]
#[clap(about = "Start local safekeeper")]
struct SafekeeperStartCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[clap(
        short = 'e',
        long = "safekeeper-extra-opt",
        help = "Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo"
    )]
    extra_opt: Vec<String>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Args)]
#[clap(about = "Stop local safekeeper")]
struct SafekeeperStopCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[arg(value_enum, default_value = "fast")]
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    stop_mode: StopMode,
}

#[derive(clap::Args)]
#[clap(about = "Restart local safekeeper")]
struct SafekeeperRestartCmdArgs {
    #[clap(help = "safekeeper id")]
    #[arg(default_value_t = NodeId(1))]
    id: NodeId,

    #[arg(value_enum, default_value = "fast")]
    #[clap(
        short = 'm',
        help = "If 'immediate', don't flush repository data at shutdown"
    )]
    stop_mode: StopMode,

    #[clap(
        short = 'e',
        long = "safekeeper-extra-opt",
        help = "Additional safekeeper invocation options, e.g. -e=--http-auth-public-key-path=foo"
    )]
    extra_opt: Vec<String>,

    #[clap(short = 't', long, help = "timeout until we fail the command")]
    #[arg(default_value = "10s")]
    start_timeout: humantime::Duration,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage Postgres instances")]
enum EndpointCmd {
    List(EndpointListCmdArgs),
    Create(EndpointCreateCmdArgs),
    Start(EndpointStartCmdArgs),
    Reconfigure(EndpointReconfigureCmdArgs),
    RefreshConfiguration(EndpointRefreshConfigurationArgs),
    Stop(EndpointStopCmdArgs),
    UpdatePageservers(EndpointUpdatePageserversCmdArgs),
    GenerateJwt(EndpointGenerateJwtCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "List endpoints")]
struct EndpointListCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_shard_id: Option<TenantShardId>,
}

#[derive(clap::Args)]
#[clap(about = "Create a compute endpoint")]
struct EndpointCreateCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(help = "Postgres endpoint id")]
    endpoint_id: Option<String>,
    #[clap(long, help = "Name of the branch the endpoint will run on")]
    branch_name: Option<String>,
    #[clap(
        long,
        help = "Specify Lsn on the timeline to start from. By default, end of the timeline would be used"
    )]
    lsn: Option<Lsn>,
    #[clap(long)]
    pg_port: Option<u16>,
    #[clap(long, alias = "http-port")]
    external_http_port: Option<u16>,
    #[clap(long)]
    internal_http_port: Option<u16>,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(
        long,
        help = "Don't do basebackup, create endpoint directory with only config files",
        action = clap::ArgAction::Set,
        default_value_t = false
    )]
    config_only: bool,

    #[arg(default_value = DEFAULT_PG_VERSION_NUM)]
    #[clap(long, help = "Postgres version")]
    pg_version: PgMajorVersion,

    /// Use gRPC to communicate with Pageservers, by generating grpc:// connstrings.
    ///
    /// Specified on creation such that it's retained across reconfiguration and restarts.
    ///
    /// NB: not yet supported by computes.
    #[clap(long)]
    grpc: bool,

    #[clap(
        long,
        help = "If set, the node will be a hot replica on the specified timeline",
        action = clap::ArgAction::Set,
        default_value_t = false
    )]
    hot_standby: bool,

    #[clap(long, help = "If set, will set up the catalog for neon_superuser")]
    update_catalog: bool,

    #[clap(
        long,
        help = "Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but useful for tests."
    )]
    allow_multiple: bool,

    /// Only allow changing it on creation
    #[clap(long, help = "Name of the privileged role for the endpoint")]
    privileged_role_name: Option<String>,
}

#[derive(clap::Args)]
#[clap(about = "Start postgres. If the endpoint doesn't exist yet, it is created.")]
struct EndpointStartCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(
        long,
        help = "Safekeepers membership generation to prefix neon.safekeepers with. Normally neon_local sets it on its own, but this option allows to override. Non zero value forces endpoint to use membership configurations."
    )]
    safekeepers_generation: Option<u32>,
    #[clap(
        long,
        help = "List of safekeepers endpoint will talk to. Normally neon_local chooses them on its own, but this option allows to override."
    )]
    safekeepers: Option<String>,

    #[clap(
        long,
        help = "Configure the remote extensions storage proxy gateway URL to request for extensions.",
        alias = "remote-ext-config"
    )]
    remote_ext_base_url: Option<String>,

    #[clap(
        long,
        help = "If set, will create test user `user` and `neondb` database. Requires `update-catalog = true`"
    )]
    create_test_user: bool,

    #[clap(
        long,
        help = "Allow multiple primary endpoints running on the same branch. Shouldn't be used normally, but useful for tests."
    )]
    allow_multiple: bool,

    #[clap(short = 't', long, value_parser= humantime::parse_duration, help = "timeout until we fail the command")]
    #[arg(default_value = "90s")]
    start_timeout: Duration,

    #[clap(
        long,
        help = "Download LFC cache from endpoint storage on endpoint startup",
        default_value = "false"
    )]
    autoprewarm: bool,

    #[clap(long, help = "Upload LFC cache to endpoint storage periodically")]
    offload_lfc_interval_seconds: Option<std::num::NonZeroU64>,

    #[clap(
        long,
        help = "Enable the compute-side oggit logical decoding worker for this endpoint"
    )]
    enable_oggit: bool,

    #[clap(
        long,
        requires = "enable_oggit",
        help = "Database monitored by the oggit worker; defaults to the endpoint's previous selection or postgres"
    )]
    oggit_database: Option<String>,

    #[clap(
        long,
        help = "Run in development mode, skipping VM-specific operations like process termination",
        action = clap::ArgAction::SetTrue
    )]
    dev: bool,
}

#[derive(clap::Args)]
#[clap(about = "Reconfigure an endpoint")]
struct EndpointReconfigureCmdArgs {
    #[clap(
        long = "tenant-id",
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: Option<TenantId>,

    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,
    #[clap(long = "pageserver-id")]
    endpoint_pageserver_id: Option<NodeId>,

    #[clap(long)]
    safekeepers: Option<String>,
}

#[derive(clap::Args)]
#[clap(about = "Refresh the endpoint's configuration by forcing it reload it's spec")]
struct EndpointRefreshConfigurationArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,
}

#[derive(clap::Args)]
#[clap(about = "Stop an endpoint")]
struct EndpointStopCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,

    #[clap(
        long,
        help = "Also delete data directory (now optional, should be default in future)"
    )]
    destroy: bool,

    #[clap(long, help = "Postgres shutdown mode")]
    #[clap(default_value = "fast")]
    mode: EndpointTerminateMode,
}

#[derive(clap::Args)]
#[clap(about = "Update the pageservers in the spec file of the compute endpoint")]
struct EndpointUpdatePageserversCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,

    #[clap(short = 'p', long, help = "Specified pageserver id")]
    pageserver_id: Option<NodeId>,
}

#[derive(clap::Args)]
#[clap(about = "Generate a JWT for an endpoint")]
struct EndpointGenerateJwtCmdArgs {
    #[clap(help = "Postgres endpoint id")]
    endpoint_id: String,

    #[clap(short = 's', long, help = "Scope to generate the JWT with", value_parser = ComputeClaimsScope::from_str)]
    scope: Option<ComputeClaimsScope>,
}

#[derive(clap::Subcommand)]
#[clap(about = "Manage neon_local branch name mappings")]
enum MappingsCmd {
    Map(MappingsMapCmdArgs),
}

#[derive(clap::Args)]
#[clap(about = "Create new mapping which cannot exist already")]
struct MappingsMapCmdArgs {
    #[clap(
        long,
        help = "Tenant id. Represented as a hexadecimal string 32 symbols length"
    )]
    tenant_id: TenantId,
    #[clap(
        long,
        help = "Timeline id. Represented as a hexadecimal string 32 symbols length"
    )]
    timeline_id: TimelineId,
    #[clap(long, help = "Branch name to give to the timeline")]
    branch_name: String,
}

///
/// Timelines tree element used as a value in the HashMap.
///
struct TimelineTreeEl {
    /// `TimelineInfo` received from the `pageserver` via the `timeline_list` http API call.
    pub info: TimelineInfo,
    /// Name, recovered from neon config mappings
    pub name: Option<String>,
    /// Holds all direct children of this timeline referenced using `timeline_id`.
    pub children: BTreeSet<TimelineId>,
}

/// A flock-based guard over the neon_local repository directory
struct RepoLock {
    _file: Flock<File>,
}

impl RepoLock {
    fn new() -> Result<Self> {
        let repo_dir = File::open(local_env::base_path())?;
        match Flock::lock(repo_dir, FlockArg::LockExclusive) {
            Ok(f) => Ok(Self { _file: f }),
            Err((_, e)) => Err(e).context("flock error"),
        }
    }
}

// Main entry point for the 'neon_local' CLI utility
//
// This utility helps to manage neon installation. That includes following:
//   * Management of local postgres installations running on top of the
//     pageserver.
//   * Providing CLI api to the pageserver
//   * TODO: export/import to/from usual postgres
fn main() -> Result<()> {
    let cli = Cli::parse();

    if let NeonLocalCmd::Branch(subcmd) = &cli.command {
        if subcmd.uses_only_connstrs() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            return rt.block_on(handle_branch(subcmd, None));
        }
    }

    // Check for 'neon init' command first.
    let (subcommand_result, _lock) = if let NeonLocalCmd::Init(args) = cli.command {
        (handle_init(&args).map(|env| Some(Cow::Owned(env))), None)
    } else {
        // This tool uses a collection of simple files to store its state, and consequently
        // it is not generally safe to run multiple commands concurrently.  Rather than expect
        // all callers to know this, use a lock file to protect against concurrent execution.
        let _repo_lock = Some(RepoLock::new().unwrap());

        // all other commands need an existing config
        let env = LocalEnv::load_config(&local_env::base_path()).context("Error loading config")?;
        let original_env = env.clone();
        let env = Box::leak(Box::new(env));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let subcommand_result = match cli.command {
            NeonLocalCmd::Init(_) => unreachable!("init was handled earlier already"),
            NeonLocalCmd::Start(args) => rt.block_on(handle_start_all(&args, env)),
            NeonLocalCmd::Stop(args) => rt.block_on(handle_stop_all(&args, env)),
            NeonLocalCmd::Tenant(subcmd) => rt.block_on(handle_tenant(&subcmd, env)),
            NeonLocalCmd::Timeline(subcmd) => rt.block_on(handle_timeline(&subcmd, env)),
            NeonLocalCmd::Pageserver(subcmd) => rt.block_on(handle_pageserver(&subcmd, env)),
            NeonLocalCmd::StorageController(subcmd) => {
                rt.block_on(handle_storage_controller(&subcmd, env))
            }
            NeonLocalCmd::StorageBroker(subcmd) => rt.block_on(handle_storage_broker(&subcmd, env)),
            NeonLocalCmd::Safekeeper(subcmd) => rt.block_on(handle_safekeeper(&subcmd, env)),
            NeonLocalCmd::EndpointStorage(subcmd) => {
                rt.block_on(handle_endpoint_storage(&subcmd, env))
            }
            NeonLocalCmd::Endpoint(subcmd) => rt.block_on(handle_endpoint(&subcmd, env)),
            NeonLocalCmd::Mappings(subcmd) => handle_mappings(&subcmd, env),
            NeonLocalCmd::Branch(subcmd) => rt.block_on(handle_branch(&subcmd, Some(env))),
            NeonLocalCmd::Oggit(subcmd) => rt.block_on(handle_oggit(&subcmd, env)),
        };

        let subcommand_result = if &original_env != env {
            subcommand_result.map(|()| Some(Cow::Borrowed(env)))
        } else {
            subcommand_result.map(|()| None)
        };
        (subcommand_result, Some(_repo_lock))
    };

    match subcommand_result {
        Ok(Some(updated_env)) => updated_env.persist_config()?,
        Ok(None) => (),
        Err(e) => {
            eprintln!("command failed: {e:?}");
            exit(1);
        }
    }
    Ok(())
}

///
/// Prints timelines list as a tree-like structure.
///
fn print_timelines_tree(
    timelines: Vec<TimelineInfo>,
    mut timeline_name_mappings: HashMap<TenantTimelineId, String>,
) -> Result<()> {
    let mut timelines_hash = timelines
        .iter()
        .map(|t| {
            (
                t.timeline_id,
                TimelineTreeEl {
                    info: t.clone(),
                    children: BTreeSet::new(),
                    name: timeline_name_mappings
                        .remove(&TenantTimelineId::new(t.tenant_id.tenant_id, t.timeline_id)),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    // Memorize all direct children of each timeline.
    for timeline in timelines.iter() {
        if let Some(ancestor_timeline_id) = timeline.ancestor_timeline_id {
            timelines_hash
                .get_mut(&ancestor_timeline_id)
                .context("missing timeline info in the HashMap")?
                .children
                .insert(timeline.timeline_id);
        }
    }

    for timeline in timelines_hash.values() {
        // Start with root local timelines (no ancestors) first.
        if timeline.info.ancestor_timeline_id.is_none() {
            print_timeline(0, &Vec::from([true]), timeline, &timelines_hash)?;
        }
    }

    Ok(())
}

///
/// Recursively prints timeline info with all its children.
///
fn print_timeline(
    nesting_level: usize,
    is_last: &[bool],
    timeline: &TimelineTreeEl,
    timelines: &HashMap<TimelineId, TimelineTreeEl>,
) -> Result<()> {
    if nesting_level > 0 {
        let ancestor_lsn = match timeline.info.ancestor_lsn {
            Some(lsn) => lsn.to_string(),
            None => "Unknown Lsn".to_string(),
        };

        let mut br_sym = "┣━";

        // Draw each nesting padding with proper style
        // depending on whether its timeline ended or not.
        if nesting_level > 1 {
            for l in &is_last[1..is_last.len() - 1] {
                if *l {
                    print!("   ");
                } else {
                    print!("┃  ");
                }
            }
        }

        // We are the last in this sub-timeline
        if *is_last.last().unwrap() {
            br_sym = "┗━";
        }

        print!("{br_sym} @{ancestor_lsn}: ");
    }

    // Finally print a timeline id and name with new line
    println!(
        "{} [{}]",
        timeline.name.as_deref().unwrap_or("_no_name_"),
        timeline.info.timeline_id
    );

    let len = timeline.children.len();
    let mut i: usize = 0;
    let mut is_last_new = Vec::from(is_last);
    is_last_new.push(false);

    for child in &timeline.children {
        i += 1;

        // Mark that the last padding is the end of the timeline
        if i == len {
            if let Some(last) = is_last_new.last_mut() {
                *last = true;
            }
        }

        print_timeline(
            nesting_level + 1,
            &is_last_new,
            timelines
                .get(child)
                .context("missing timeline info in the HashMap")?,
            timelines,
        )?;
    }

    Ok(())
}

/// Helper function to get tenant id from an optional --tenant_id option or from the config file
fn get_tenant_id(
    tenant_id_arg: Option<TenantId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantId> {
    if let Some(tenant_id_from_arguments) = tenant_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(default_id)
    } else {
        anyhow::bail!("No tenant id. Use --tenant-id, or set a default tenant");
    }
}

/// Helper function to get tenant-shard ID from an optional --tenant_id option or from the config file,
/// for commands that accept a shard suffix
fn get_tenant_shard_id(
    tenant_shard_id_arg: Option<TenantShardId>,
    env: &local_env::LocalEnv,
) -> anyhow::Result<TenantShardId> {
    if let Some(tenant_id_from_arguments) = tenant_shard_id_arg {
        Ok(tenant_id_from_arguments)
    } else if let Some(default_id) = env.default_tenant_id {
        Ok(TenantShardId::unsharded(default_id))
    } else {
        anyhow::bail!("No tenant shard id. Use --tenant-id, or set a default tenant");
    }
}

fn handle_init(args: &InitCmdArgs) -> anyhow::Result<LocalEnv> {
    // Create the in-memory `LocalEnv` that we'd normally load from disk in `load_config`.
    let init_conf: NeonLocalInitConf = if let Some(config_path) = &args.config {
        // User (likely the Python test suite) provided a description of the environment.
        if args.num_pageservers.is_some() {
            bail!(
                "Cannot specify both --num-pageservers and --config, use key `pageservers` in the --config file instead"
            );
        }
        // load and parse the file
        let contents = std::fs::read_to_string(config_path).with_context(|| {
            format!(
                "Could not read configuration file '{}'",
                config_path.display()
            )
        })?;
        toml_edit::de::from_str(&contents)?
    } else {
        // User (likely interactive) did not provide a description of the environment, give them the default
        NeonLocalInitConf {
            control_plane_api: Some(DEFAULT_PAGESERVER_CONTROL_PLANE_API.parse().unwrap()),
            broker: NeonBroker {
                listen_addr: Some(DEFAULT_BROKER_ADDR.parse().unwrap()),
                listen_https_addr: None,
            },
            safekeepers: vec![SafekeeperConf {
                id: DEFAULT_SAFEKEEPER_ID,
                pg_port: DEFAULT_SAFEKEEPER_PG_PORT,
                http_port: DEFAULT_SAFEKEEPER_HTTP_PORT,
                ..Default::default()
            }],
            pageservers: (0..args.num_pageservers.unwrap_or(1))
                .map(|i| {
                    let pageserver_id = NodeId(DEFAULT_PAGESERVER_ID.0 + i as u64);
                    let pg_port = DEFAULT_PAGESERVER_PG_PORT + i;
                    let http_port = DEFAULT_PAGESERVER_HTTP_PORT + i;
                    let grpc_port = DEFAULT_PAGESERVER_GRPC_PORT + i;
                    NeonLocalInitPageserverConf {
                        id: pageserver_id,
                        listen_pg_addr: format!("127.0.0.1:{pg_port}"),
                        listen_http_addr: format!("127.0.0.1:{http_port}"),
                        listen_https_addr: None,
                        listen_grpc_addr: Some(format!("127.0.0.1:{grpc_port}")),
                        pg_auth_type: AuthType::Trust,
                        http_auth_type: AuthType::Trust,
                        grpc_auth_type: AuthType::Trust,
                        other: Default::default(),
                        // Typical developer machines use disks with slow fsync, and we don't care
                        // about data integrity: disable disk syncs.
                        no_sync: true,
                    }
                })
                .collect(),
            endpoint_storage: EndpointStorageConf {
                listen_addr: ENDPOINT_STORAGE_DEFAULT_ADDR,
            },
            pg_distrib_dir: None,
            neon_distrib_dir: None,
            default_tenant_id: TenantId::from_array(std::array::from_fn(|_| 0)),
            storage_controller: None,
            control_plane_hooks_api: None,
            generate_local_ssl_certs: false,
        }
    };

    LocalEnv::init(init_conf, &args.force)
        .context("materialize initial neon_local environment on disk")?;
    Ok(LocalEnv::load_config(&local_env::base_path())
        .expect("freshly written config should be loadable"))
}

/// The default pageserver is the one where CLI tenant/timeline operations are sent by default.
/// For typical interactive use, one would just run with a single pageserver.  Scenarios with
/// tenant/timeline placement across multiple pageservers are managed by python test code rather
/// than this CLI.
fn get_default_pageserver(env: &local_env::LocalEnv) -> PageServerNode {
    let ps_conf = env
        .pageservers
        .first()
        .expect("Config is validated to contain at least one pageserver");
    PageServerNode::from_env(env, ps_conf)
}

async fn handle_tenant(subcmd: &TenantCmd, env: &mut local_env::LocalEnv) -> anyhow::Result<()> {
    let pageserver = get_default_pageserver(env);
    match subcmd {
        TenantCmd::List => {
            for t in pageserver.tenant_list().await? {
                println!("{} {:?}", t.id, t.state);
            }
        }
        TenantCmd::Import(args) => {
            let tenant_id = args.tenant_id;

            let storage_controller = StorageController::from_env(env);
            let create_response = storage_controller.tenant_import(tenant_id).await?;

            let shard_zero = create_response
                .shards
                .first()
                .expect("Import response omitted shards");

            let attached_pageserver_id = shard_zero.node_id;
            let pageserver =
                PageServerNode::from_env(env, env.get_pageserver_conf(attached_pageserver_id)?);

            println!(
                "Imported tenant {tenant_id}, attached to pageserver {attached_pageserver_id}"
            );

            let timelines = pageserver
                .http_client
                .list_timelines(shard_zero.shard_id)
                .await?;

            // Pick a 'main' timeline that has no ancestors, the rest will get arbitrary names
            let main_timeline = timelines
                .iter()
                .find(|t| t.ancestor_timeline_id.is_none())
                .expect("No timelines found")
                .timeline_id;

            let mut branch_i = 0;
            for timeline in timelines.iter() {
                let branch_name = if timeline.timeline_id == main_timeline {
                    "main".to_string()
                } else {
                    branch_i += 1;
                    format!("branch_{branch_i}")
                };

                println!(
                    "Importing timeline {tenant_id}/{} as branch {branch_name}",
                    timeline.timeline_id
                );

                env.register_branch_mapping(branch_name, tenant_id, timeline.timeline_id)?;
            }
        }
        TenantCmd::Create(args) => {
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();

            let tenant_conf = PageServerNode::parse_config(tenant_conf)?;

            let storage_controller = StorageController::from_env(env);
            let output = create_tenant(
                &storage_controller,
                env,
                TenantCreateOptions {
                    tenant_id: args.tenant_id,
                    timeline_id: args.timeline_id,
                    branch_name: DEFAULT_BRANCH_NAME.to_string(),
                    set_default: args.set_default,
                    pg_version: args.pg_version,
                    shard_count: args.shard_count,
                    shard_stripe_size: args.shard_stripe_size,
                    placement_policy: args.placement_policy.clone(),
                    config: tenant_conf,
                },
            )
            .await?;
            let tenant_id = output.tenant_id;
            let new_timeline_id = output.timeline_id;
            println!("tenant {tenant_id} successfully created on the pageserver");

            println!("Created an initial timeline '{new_timeline_id}' for tenant: {tenant_id}",);

            if args.set_default {
                println!("Setting tenant {tenant_id} as a default one");
            }
        }
        TenantCmd::SetDefault(args) => {
            println!("Setting tenant {} as a default one", args.tenant_id);
            env.default_tenant_id = Some(args.tenant_id);
        }
        TenantCmd::Config(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let tenant_conf: HashMap<_, _> =
                args.config.iter().flat_map(|c| c.split_once(':')).collect();
            let config = PageServerNode::parse_config(tenant_conf)?;

            let req = TenantConfigRequest { tenant_id, config };

            let storage_controller = StorageController::from_env(env);
            storage_controller
                .set_tenant_config(&req)
                .await
                .with_context(|| format!("Tenant config failed for tenant with id {tenant_id}"))?;
            println!("tenant {tenant_id} successfully configured via storcon");
        }
    }
    Ok(())
}

async fn handle_timeline(cmd: &TimelineCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    let pageserver = get_default_pageserver(env);

    match cmd {
        TimelineCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;
            let timelines = pageserver.timeline_list(&tenant_shard_id).await?;
            print_timelines_tree(timelines, env.timeline_name_mappings())?;
        }
        TimelineCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let storage_controller = StorageController::from_env(env);
            let output = create_timeline(
                &storage_controller,
                env,
                TimelineCreateOptions {
                    tenant_id,
                    timeline_id: args.timeline_id,
                    branch_name: args.branch_name.clone(),
                    pg_version: args.pg_version,
                },
            )
            .await?;

            let last_record_lsn = output.timeline_info.last_record_lsn;

            println!(
                "Created timeline '{}' at Lsn {last_record_lsn} for tenant: {tenant_id}",
                output.timeline_info.timeline_id
            );
        }
        // TODO: rename to import-basebackup-plus-wal
        TimelineCmd::Import(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let timeline_id = args.timeline_id;
            let branch_name = &args.branch_name;

            // Parse base inputs
            let base = (args.base_lsn, args.base_tarfile.clone());

            // Parse pg_wal inputs
            let wal_tarfile = args.wal_tarfile.clone();
            let end_lsn = args.end_lsn;
            // TODO validate both or none are provided
            let pg_wal = end_lsn.zip(wal_tarfile);

            println!("Importing timeline into pageserver ...");
            pageserver
                .timeline_import(tenant_id, timeline_id, base, pg_wal, args.pg_version)
                .await?;
            if env.storage_controller.timelines_onto_safekeepers {
                println!("Creating timeline on safekeeper ...");
                let timeline_info = pageserver
                    .timeline_info(
                        TenantShardId::unsharded(tenant_id),
                        timeline_id,
                        pageserver_client::mgmt_api::ForceAwaitLogicalSize::No,
                    )
                    .await?;
                let default_sk = SafekeeperNode::from_env(env, env.safekeepers.first().unwrap());
                let default_host = default_sk
                    .conf
                    .listen_addr
                    .clone()
                    .unwrap_or_else(|| "localhost".to_string());
                let mconf = safekeeper_api::membership::Configuration {
                    generation: SafekeeperGeneration::new(1),
                    members: safekeeper_api::membership::MemberSet {
                        m: vec![SafekeeperId {
                            host: default_host,
                            id: default_sk.conf.id,
                            pg_port: default_sk.conf.pg_port,
                        }],
                    },
                    new_members: None,
                };
                let pg_version = PgVersionId::from(args.pg_version);
                let req = safekeeper_api::models::TimelineCreateRequest {
                    tenant_id,
                    timeline_id,
                    mconf,
                    pg_version,
                    system_id: None,
                    wal_seg_size: None,
                    start_lsn: timeline_info.last_record_lsn,
                    commit_lsn: None,
                };
                default_sk.create_timeline(&req).await?;
            }
            env.register_branch_mapping(branch_name.to_string(), tenant_id, timeline_id)?;
            println!("Done");
        }
        TimelineCmd::Branch(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let new_timeline_id = args.timeline_id.unwrap_or(TimelineId::generate());
            let new_branch_name = &args.branch_name;
            let ancestor_branch_name = args
                .ancestor_branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let ancestor_timeline_id = env
                .get_branch_timeline_id(&ancestor_branch_name, tenant_id)
                .ok_or_else(|| {
                    anyhow!("Found no timeline id for branch name '{ancestor_branch_name}'")
                })?;

            let storage_controller = StorageController::from_env(env);

            // If no explicit start LSN is provided, try to sync with the running endpoint's current LSN
            let start_lsn = if args.ancestor_start_lsn.is_some() {
                args.ancestor_start_lsn
            } else {
                // Try to find a running endpoint on the ancestor timeline to get its current LSN
                let cplane = ComputeControlPlane::load(env.clone())?;
                let mut endpoint_lsn: Option<Lsn> = None;

                for endpoint in cplane.endpoints.values() {
                    if endpoint.tenant_id == tenant_id
                        && endpoint.timeline_id == ancestor_timeline_id
                        && endpoint.status() == EndpointStatus::Running
                    {
                        // Found a running endpoint, try to get its flush LSN
                        let connstr = format!(
                            "host=127.0.0.1 port={} user=cloud_admin dbname=postgres",
                            endpoint.pg_address.port()
                        );
                        match tokio_opengauss::connect(&connstr, NoTls).await {
                            Ok((client, connection)) => {
                                // Spawn connection handler
                                tokio::spawn(async move {
                                    if let Err(e) = connection.await {
                                        eprintln!("connection error: {}", e);
                                    }
                                });

                                // Query current flush LSN (openGauss uses pg_get_flush_lsn())
                                match client.query_one("SELECT pg_get_flush_lsn()", &[]).await {
                                    Ok(row) => {
                                        let lsn_str: String = row.get(0);
                                        // Parse LSN format like "00000000/04BEFAA0" or "0/4BEFAA0"
                                        if let Ok(lsn) =
                                            Lsn::from_str(&lsn_str.replace("00000000/", "0/"))
                                        {
                                            println!(
                                                "Syncing branch point with endpoint flush LSN: {}",
                                                lsn
                                            );
                                            endpoint_lsn = Some(lsn);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "Warning: Failed to get flush LSN from endpoint: {}",
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Warning: Failed to connect to endpoint: {}", e);
                            }
                        }
                        break;
                    }
                }

                // If we got an LSN from the endpoint, wait for pageserver to sync
                if let Some(lsn) = endpoint_lsn {
                    println!("Waiting for pageserver to sync WAL to {}...", lsn);
                    println!("(This may take a while for large transactions)");

                    let pageserver = get_default_pageserver(env);
                    let tenant_shard_id = TenantShardId::unsharded(tenant_id);

                    // Use longer timeout (5 minutes) and retry mechanism for large WAL volumes
                    let max_retries = 3;
                    let timeout_per_attempt = Duration::from_secs(120); // 2 minutes per attempt
                    let mut success = false;

                    for attempt in 1..=max_retries {
                        let mut timelines = HashMap::new();
                        timelines.insert(ancestor_timeline_id, lsn);
                        let wait_req = TenantWaitLsnRequest {
                            timelines,
                            timeout: timeout_per_attempt,
                        };

                        match pageserver
                            .http_client
                            .wait_lsn(tenant_shard_id, wait_req)
                            .await
                        {
                            Ok(_) => {
                                println!("WAL sync complete.");
                                success = true;
                                break;
                            }
                            Err(e) => {
                                if attempt < max_retries {
                                    println!(
                                        "Attempt {}/{}: still waiting... ({})",
                                        attempt, max_retries, e
                                    );
                                } else {
                                    eprintln!(
                                        "Warning: wait_lsn failed after {} attempts: {}",
                                        max_retries, e
                                    );
                                }
                            }
                        }
                    }

                    if !success {
                        eprintln!(
                            "Proceeding with branch creation anyway. Data may be incomplete."
                        );
                    }
                }

                // Use the endpoint LSN as the branch point to ensure data consistency
                endpoint_lsn
            };

            let output = branch_timeline(
                &storage_controller,
                env,
                TimelineBranchOptions {
                    tenant_id,
                    timeline_id: Some(new_timeline_id),
                    branch_name: new_branch_name.to_string(),
                    ancestor_timeline_id,
                    ancestor_start_lsn: start_lsn,
                },
            )
            .await?;

            println!(
                "Created timeline '{}' at Lsn {} for tenant: {tenant_id}. Ancestor timeline: '{ancestor_branch_name}'",
                output.timeline_info.timeline_id, output.timeline_info.last_record_lsn
            );
        }
    }

    Ok(())
}

fn resolve_branch_endpoint(
    cplane: &ComputeControlPlane,
    env: &local_env::LocalEnv,
    tenant_id: TenantId,
    branch_name: &str,
    endpoint_id: &Option<String>,
    endpoint_arg_name: &str,
) -> Result<(String, Arc<Endpoint>)> {
    let timeline_id = env
        .get_branch_timeline_id(branch_name, tenant_id)
        .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

    if let Some(endpoint_id) = endpoint_id {
        let endpoint = cplane
            .endpoints
            .get(endpoint_id)
            .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;

        if endpoint.tenant_id != tenant_id || endpoint.timeline_id != timeline_id {
            bail!(
                "endpoint {endpoint_id} does not belong to branch {branch_name} in tenant {tenant_id}"
            );
        }

        if endpoint.status() != EndpointStatus::Running {
            bail!("endpoint {endpoint_id} for branch {branch_name} is not running");
        }

        return Ok((endpoint_id.clone(), Arc::clone(endpoint)));
    }

    let mut matches = cplane
        .endpoints
        .iter()
        .filter(|(_, endpoint)| {
            endpoint.tenant_id == tenant_id
                && endpoint.timeline_id == timeline_id
                && endpoint.status() == EndpointStatus::Running
        })
        .map(|(endpoint_id, endpoint)| (endpoint_id.clone(), Arc::clone(endpoint)))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        bail!(
            "no running endpoint found for branch {branch_name}; start one or pass --{}-endpoint",
            endpoint_arg_name
        );
    }

    if matches.len() > 1 {
        let endpoint_ids = matches
            .iter()
            .map(|(endpoint_id, _)| endpoint_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "multiple running endpoints found for branch {branch_name}: {endpoint_ids}; pass an explicit endpoint id"
        );
    }

    Ok(matches.remove(0))
}

type BranchEndpointRef = branch_ops::BranchEndpointRef;

fn require_branch_env<'a>(env: Option<&'a local_env::LocalEnv>) -> Result<&'a local_env::LocalEnv> {
    env.context("local branch endpoint mode requires an initialized NEON_REPO_DIR; use --source-connstr/--target-connstr for Docker endpoints")
}

fn resolve_branch_endpoint_ref(
    cplane: Option<&ComputeControlPlane>,
    env: Option<&local_env::LocalEnv>,
    tenant_id: Option<TenantId>,
    branch_name: &Option<String>,
    endpoint_id: &Option<String>,
    connstr: &Option<String>,
    endpoint_arg_name: &str,
    fallback_name: &str,
    user: &str,
    database: &str,
) -> Result<BranchEndpointRef> {
    if let Some(connstr) = connstr {
        if endpoint_id.is_some() {
            bail!(
                "pass either --{endpoint_arg_name}-endpoint or --{endpoint_arg_name}-connstr, not both"
            );
        }
        return BranchEndpointRef::from_connstr(
            connstr,
            branch_name.as_ref(),
            fallback_name,
            user,
            database,
        );
    }

    let branch_name = branch_name.as_ref().with_context(|| {
        format!("--{endpoint_arg_name}-branch is required without --{endpoint_arg_name}-connstr")
    })?;
    let tenant_id = tenant_id.with_context(|| {
        format!("--tenant-id or default tenant is required without --{endpoint_arg_name}-connstr")
    })?;
    let cplane = cplane.with_context(|| {
        format!("local --{endpoint_arg_name}-branch lookup requires endpoint metadata")
    })?;
    let env = require_branch_env(env)?;
    let (endpoint_id, endpoint) = resolve_branch_endpoint(
        cplane,
        env,
        tenant_id,
        branch_name,
        endpoint_id,
        endpoint_arg_name,
    )?;

    Ok(BranchEndpointRef::from_local(
        endpoint_id,
        endpoint,
        branch_name.clone(),
        user,
        database,
    ))
}

/// Build the compute-side oggit worker configuration for a Primary openGauss
/// endpoint. Returns None for non-Primary endpoints or when gaussdb is not the
/// backing binary. Inference of branch_start_lsn / ancestor_timeline_id mirrors
/// the former external worker: it queries the pageserver's timeline_info.
async fn build_oggit_endpoint_config(
    env: &local_env::LocalEnv,
    endpoint: &Endpoint,
    safekeepers: &[NodeId],
    database: &str,
) -> Option<control_plane::endpoint::OggitEndpointConfig> {
    if !matches!(endpoint.mode, ComputeMode::Primary) {
        return None;
    }
    if !env
        .pg_bin_dir(endpoint.pg_version())
        .ok()?
        .join("gaussdb")
        .exists()
    {
        return None;
    }

    let tenant_id = endpoint.tenant_id;
    let timeline_id = endpoint.timeline_id;

    // Best-effort query of ancestor info. On failure we still start the worker
    // with a 0/0 base; the worker records what it can and diff/merge validates.
    let timeline_info = get_default_pageserver(env)
        .timeline_info(
            TenantShardId::unsharded(tenant_id),
            timeline_id,
            pageserver_client::mgmt_api::ForceAwaitLogicalSize::No,
        )
        .await
        .ok();
    let branch_start_lsn = timeline_info
        .as_ref()
        .and_then(|info| info.ancestor_lsn)
        .map(|lsn| lsn.to_string())
        .unwrap_or_else(|| "0/0".to_string());
    let ancestor_timeline_id = timeline_info
        .as_ref()
        .and_then(|info| info.ancestor_timeline_id)
        .map(|id| id.to_string());
    let safekeeper_http_urls = safekeepers
        .iter()
        .filter_map(|id| env.safekeepers.iter().find(|sk| sk.id == *id))
        .map(|sk| {
            let listen_addr = sk
                .listen_addr
                .clone()
                .unwrap_or_else(|| "127.0.0.1".to_string());
            format!("http://{}:{}", listen_addr, sk.http_port)
        })
        .collect::<Vec<_>>();

    Some(control_plane::endpoint::OggitEndpointConfig {
        tenant_id: tenant_id.to_string(),
        timeline_id: timeline_id.to_string(),
        ancestor_timeline_id,
        branch_start_lsn,
        database: database.to_string(),
        safekeeper_http_urls,
    })
}

fn branch_output_print(output: BranchCommandOutput) {
    for line in output.lines {
        println!("{line}");
    }
}

fn local_branch_strategy(strategy: BranchMergeStrategy) -> control_plane::BranchMergeStrategy {
    match strategy {
        BranchMergeStrategy::Fail => control_plane::BranchMergeStrategy::Fail,
        BranchMergeStrategy::Ours => control_plane::BranchMergeStrategy::Ours,
        BranchMergeStrategy::Theirs => control_plane::BranchMergeStrategy::Theirs,
        BranchMergeStrategy::Manual => control_plane::BranchMergeStrategy::Manual,
    }
}

fn retarget_index_definition(
    indexdef: &str,
    target_schema: &str,
    table_name: &str,
) -> Result<String> {
    let on_pos = indexdef
        .find(" ON ")
        .ok_or_else(|| anyhow!("could not parse index definition: {indexdef}"))?;
    let rel_start = on_pos + " ON ".len();
    let tail = &indexdef[rel_start..];
    let rel_end = tail
        .find(" USING ")
        .or_else(|| tail.find(" ("))
        .ok_or_else(|| anyhow!("could not parse index relation in definition: {indexdef}"))?;
    let target_relation = format!(
        "{}.{}",
        quote_sql_ident(target_schema),
        quote_sql_ident(table_name)
    );

    Ok(format!(
        "{}{}{}",
        &indexdef[..rel_start],
        target_relation,
        &tail[rel_end..]
    ))
}

async fn load_source_indexes(
    source_client: &tokio_opengauss::Client,
    source_schema: &str,
    target_schema: &str,
    table_name: &str,
) -> Result<Vec<SourceIndex>> {
    let rows = source_client
        .query(
            "SELECT pg_catalog.pg_get_indexdef(i.indexrelid)::text
             FROM pg_index i
             JOIN pg_class t ON t.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = $1
               AND t.relname = $2
               AND NOT EXISTS (
                   SELECT 1
                   FROM pg_constraint con
                   WHERE con.conindid = i.indexrelid
               )
             ORDER BY i.indexrelid::text",
            &[&source_schema, &table_name],
        )
        .await
        .with_context(|| format!("failed to read indexes for {source_schema}.{table_name}"))?;

    rows.into_iter()
        .map(|row| {
            let definition: String = row.get(0);
            Ok(SourceIndex {
                definition: retarget_index_definition(&definition, target_schema, table_name)?,
            })
        })
        .collect()
}

async fn copy_source_only_tables_with_schema(
    target_client: &mut tokio_opengauss::Client,
    source_endpoint: &BranchEndpointRef,
    source_schema: &str,
    target_schema: &str,
    fdw_schema: &str,
    copy_source_only_tables: bool,
) -> Result<(Vec<(String, i64)>, Vec<String>)> {
    let source_client = connect_to_branch_endpoint(source_endpoint).await?;
    reject_unsupported_source_schema_objects(&source_client, source_schema).await?;

    target_client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_sql_ident(target_schema)
        ))
        .await
        .with_context(|| format!("failed to create target schema {target_schema}"))?;

    let source_tables = list_schema_tables(&source_client, source_schema).await?;
    let target_tables = list_schema_tables(target_client, target_schema).await?;
    let source_only_tables = source_tables
        .difference(&target_tables)
        .cloned()
        .collect::<Vec<_>>();
    let common_tables = source_tables
        .intersection(&target_tables)
        .cloned()
        .collect::<Vec<_>>();

    if !copy_source_only_tables && !source_only_tables.is_empty() {
        bail!(
            "target table {}.{} does not exist",
            target_schema,
            source_only_tables[0]
        );
    }

    let mut copied_tables = Vec::new();
    for table_name in source_only_tables {
        reject_unsupported_source_table_features(&source_client, source_schema, &table_name)
            .await?;
        let columns = load_source_columns(&source_client, source_schema, &table_name).await?;
        let constraints =
            load_source_constraints(&source_client, source_schema, &table_name).await?;
        let indexes =
            load_source_indexes(&source_client, source_schema, target_schema, &table_name).await?;

        let column_defs = columns
            .iter()
            .map(|column| {
                let default_expr = column
                    .default_expr
                    .as_ref()
                    .map(|expr| format!(" DEFAULT {expr}"))
                    .unwrap_or_default();
                let not_null = if column.not_null { " NOT NULL" } else { "" };
                format!(
                    "{} {}{}{}",
                    quote_sql_ident(&column.name),
                    column.data_type,
                    default_expr,
                    not_null
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let create_table_sql = format!(
            "CREATE TABLE {}.{} ({})",
            quote_sql_ident(target_schema),
            quote_sql_ident(&table_name),
            column_defs
        );
        target_client
            .batch_execute(&create_table_sql)
            .await
            .with_context(|| {
                format!("failed to create target table {target_schema}.{table_name}")
            })?;

        for constraint in constraints {
            let add_constraint_sql = format!(
                "ALTER TABLE {}.{} ADD CONSTRAINT {} {}",
                quote_sql_ident(target_schema),
                quote_sql_ident(&table_name),
                quote_sql_ident(&constraint.name),
                constraint.definition
            );
            target_client
                .batch_execute(&add_constraint_sql)
                .await
                .with_context(|| {
                    format!(
                        "failed to add constraint {} on {target_schema}.{table_name}",
                        constraint.name
                    )
                })?;
        }

        for index in indexes {
            target_client
                .batch_execute(&index.definition)
                .await
                .with_context(|| {
                    format!("failed to create index on {target_schema}.{table_name}")
                })?;
        }

        let column_list = columns
            .iter()
            .map(|column| quote_sql_ident(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let copy_sql = format!(
            "INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}",
            quote_sql_ident(target_schema),
            quote_sql_ident(&table_name),
            column_list,
            column_list,
            quote_sql_ident(fdw_schema),
            quote_sql_ident(&table_name)
        );
        let copied_count = target_client
            .execute(copy_sql.as_str(), &[])
            .await
            .with_context(|| format!("failed to copy rows into {target_schema}.{table_name}"))?;
        copied_tables.push((table_name, copied_count as i64));
    }

    Ok((copied_tables, common_tables))
}

fn name_array_literal(names: &[String]) -> String {
    let values = names
        .iter()
        .map(|name| quote_sql_literal(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{values}]::name[]")
}

async fn prepare_branch_source_fdw(
    client: &mut tokio_opengauss::Client,
    args_source_schema: &str,
    fdw_schema: &str,
    fdw_server: &str,
    source_endpoint: &BranchEndpointRef,
) -> Result<()> {
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS neon")
        .await
        .context("failed to create neon extension on target endpoint")?;

    let source_port = i32::from(source_endpoint.fdw_port);

    let user_mapping_options = match &source_endpoint.password {
        Some(password) => format!(
            "user {}, password {}",
            quote_sql_literal(&source_endpoint.user),
            quote_sql_literal(password)
        ),
        None => format!("user {}", quote_sql_literal(&source_endpoint.user)),
    };

    let prepare_sql = format!(
        "DROP SERVER IF EXISTS {} CASCADE;
         DROP SCHEMA IF EXISTS {} CASCADE;
         CREATE SCHEMA {};
         CREATE SERVER {} FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host {}, port {}, dbname {});
         CREATE USER MAPPING FOR CURRENT_USER SERVER {} OPTIONS ({});",
        quote_sql_ident(fdw_server),
        quote_sql_ident(fdw_schema),
        quote_sql_ident(fdw_schema),
        quote_sql_ident(fdw_server),
        quote_sql_literal(&source_endpoint.fdw_host),
        quote_sql_literal(&source_port.to_string()),
        quote_sql_literal(&source_endpoint.database),
        quote_sql_ident(fdw_server),
        user_mapping_options,
    );

    client
        .batch_execute(&prepare_sql)
        .await
        .context("failed to prepare postgres_fdw source schema")?;

    import_branch_source_foreign_tables(
        client,
        source_endpoint,
        args_source_schema,
        fdw_schema,
        fdw_server,
    )
    .await
    .context("failed to create source foreign tables")?;

    Ok(())
}

async fn cleanup_branch_source_fdw(
    client: &mut tokio_opengauss::Client,
    fdw_schema: &str,
    fdw_server: &str,
) -> Result<()> {
    let cleanup_sql = format!(
        "DROP SERVER IF EXISTS {} CASCADE;
         DROP SCHEMA IF EXISTS {} CASCADE",
        quote_sql_ident(fdw_server),
        quote_sql_ident(fdw_schema),
    );

    client
        .batch_execute(&cleanup_sql)
        .await
        .context("failed to clean up postgres_fdw source schema")?;

    Ok(())
}

async fn handle_branch(subcmd: &BranchCmd, env: Option<&local_env::LocalEnv>) -> Result<()> {
    match subcmd {
        BranchCmd::Diff(args) => {
            let needs_local = args.source_connstr.is_none() || args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let source_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.source_branch,
                &args.source_endpoint,
                &args.source_connstr,
                "source",
                "source",
                &args.user,
                &args.database,
            )?;
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            println!(
                "Diffing source branch '{}' ({}) against target branch '{}' ({})",
                source_endpoint.branch_name,
                source_endpoint.endpoint_id,
                target_endpoint.branch_name,
                target_endpoint.endpoint_id
            );

            let mut client = connect_to_branch_endpoint(&target_endpoint).await?;
            lock_branch_fdw_workspace(&client).await?;
            let parent_command_lsn = if args.incremental_oggit {
                Some(current_endpoint_lsn(&client).await?)
            } else {
                None
            };
            let child_command_lsn = if args.incremental_oggit {
                let source_client = connect_to_branch_endpoint(&source_endpoint).await?;
                Some(current_endpoint_lsn(&source_client).await?)
            } else {
                None
            };
            ensure_branch_database_compatibility(&client, &source_endpoint).await?;
            prepare_branch_source_fdw(
                &mut client,
                &args.source_schema,
                &OGGIT_FDW_SCHEMA,
                &args.fdw_server,
                &source_endpoint,
            )
            .await?;

            if args.incremental_oggit {
                import_source_oggit_foreign_tables(
                    &mut client,
                    &OGGIT_FDW_SCHEMA,
                    &args.fdw_server,
                )
                .await?;
                let diff_result = oggit_diff_from_meta(
                    &client,
                    &OGGIT_FDW_SCHEMA,
                    None,
                    None,
                    child_command_lsn.as_deref(),
                    parent_command_lsn.as_deref(),
                )
                .await
                .context("branch diff failed")?;

                if !args.keep_fdw {
                    if let Err(e) =
                        cleanup_branch_source_fdw(&mut client, &OGGIT_FDW_SCHEMA, &args.fdw_server)
                            .await
                    {
                        eprintln!("Warning: {e:#}");
                    }
                }

                println!(
                    "oggit.diff\tbase_lsn={}\tchild_to_lsn={}\tparent_to_lsn={}",
                    diff_result.base_lsn, diff_result.child_to_lsn, diff_result.parent_to_lsn
                );

                for row in diff_result.rows {
                    println!(
                        "{}\t{}.{}\t{}\tkey={}\tours={}\ttheirs={}\t{}",
                        row.diff_scope,
                        row.schema_name,
                        row.table_name,
                        row.diff_type,
                        oggit_json_text(&row.key_json),
                        oggit_json_text(&row.ours_json),
                        oggit_json_text(&row.theirs_json),
                        row.detail.unwrap_or_default()
                    );
                }
            } else {
                let diff_result = client
                    .query(
                        "SELECT 'row'::text, schema_name, table_name, diff_type,
                                COALESCE(row_data, ''), ''::text, ''::text, ''::text
                           FROM neon_branch_diff($1::name, $2::name, NULL::name[])",
                        &[&OGGIT_FDW_SCHEMA, &args.target_schema],
                    )
                    .await;

                if !args.keep_fdw {
                    if let Err(e) =
                        cleanup_branch_source_fdw(&mut client, &OGGIT_FDW_SCHEMA, &args.fdw_server)
                            .await
                    {
                        eprintln!("Warning: {e:#}");
                    }
                }

                for row in diff_result.context("branch diff failed")? {
                    let diff_scope: String = row.get(0);
                    let schema_name: String = row.get(1);
                    let table_name: String = row.get(2);
                    let diff_type: String = row.get(3);
                    let key_json: String = row.get(4);
                    let ours_json: String = row.get(5);
                    let theirs_json: String = row.get(6);
                    let detail: String = row.get(7);

                    println!(
                        "{diff_scope}\t{schema_name}.{table_name}\t{diff_type}\tkey={key_json}\tours={ours_json}\ttheirs={theirs_json}\t{detail}"
                    );
                }
            }
        }
        BranchCmd::Merge(args) => {
            let needs_local = args.source_connstr.is_none() || args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let source_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.source_branch,
                &args.source_endpoint,
                &args.source_connstr,
                "source",
                "source",
                &args.user,
                &args.database,
            )?;
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            println!(
                "Merging source branch '{}' ({}) into target branch '{}' ({}) with strategy '{}'",
                source_endpoint.branch_name,
                source_endpoint.endpoint_id,
                target_endpoint.branch_name,
                target_endpoint.endpoint_id,
                args.strategy.as_str()
            );

            let mut client = connect_to_branch_endpoint(&target_endpoint).await?;
            lock_branch_fdw_workspace(&client).await?;
            let parent_command_lsn = if args.incremental_oggit {
                Some(current_endpoint_lsn(&client).await?)
            } else {
                None
            };
            let child_command_lsn = if args.incremental_oggit {
                let source_client = connect_to_branch_endpoint(&source_endpoint).await?;
                let requested_lsn = current_endpoint_lsn(&source_client).await?;
                oggit_freeze_metadata_lsn(&source_client, &requested_lsn, "child")
                    .await
                    .context("failed to freeze child oggit metadata before merge")?;
                Some(requested_lsn)
            } else {
                None
            };
            if let Some(requested_lsn) = parent_command_lsn.as_deref() {
                oggit_freeze_metadata_lsn(&client, requested_lsn, "parent")
                    .await
                    .context("failed to freeze parent oggit metadata before merge")?;
            }
            ensure_branch_database_compatibility(&client, &source_endpoint).await?;
            prepare_branch_source_fdw(
                &mut client,
                &args.source_schema,
                &OGGIT_FDW_SCHEMA,
                &args.fdw_server,
                &source_endpoint,
            )
            .await?;

            if !args.incremental_oggit && matches!(args.strategy, BranchMergeStrategy::Manual) {
                bail!("manual merge strategy requires --incremental-oggit");
            }

            if args.incremental_oggit {
                import_source_oggit_foreign_tables(
                    &mut client,
                    &OGGIT_FDW_SCHEMA,
                    &args.fdw_server,
                )
                .await?;
                client
                    .batch_execute("BEGIN")
                    .await
                    .context("failed to start branch merge transaction")?;

                let merge_result = oggit_merge_from_meta(
                    &client,
                    &OGGIT_FDW_SCHEMA,
                    args.strategy,
                    None,
                    None,
                    child_command_lsn.as_deref(),
                    parent_command_lsn.as_deref(),
                )
                .await;

                if merge_result.is_err() {
                    if let Err(e) = client.batch_execute("ROLLBACK").await {
                        eprintln!("Warning: failed to roll back branch merge transaction: {e:#}");
                    }
                    if !args.keep_fdw {
                        if let Err(e) = cleanup_branch_source_fdw(
                            &mut client,
                            &OGGIT_FDW_SCHEMA,
                            &args.fdw_server,
                        )
                        .await
                        {
                            eprintln!("Warning: {e:#}");
                        }
                    }
                }

                let merge_result = merge_result.context("branch merge failed")?;
                client
                    .batch_execute("COMMIT")
                    .await
                    .context("failed to commit branch merge transaction")?;
                if merge_result.status == "applied" {
                    oggit_finalize_merge_commit_lsn(&client, &merge_result.merge_id)
                        .await
                        .context("failed to finalize oggit merge commit LSN")?;
                }

                if merge_result.status == "blocked" && !args.keep_fdw {
                    eprintln!(
                        "Keeping FDW schema '{}' and server '{}' so branch continue can reread child oggit metadata",
                        OGGIT_FDW_SCHEMA, args.fdw_server
                    );
                } else if !args.keep_fdw {
                    if let Err(e) =
                        cleanup_branch_source_fdw(&mut client, &OGGIT_FDW_SCHEMA, &args.fdw_server)
                            .await
                    {
                        eprintln!("Warning: {e:#}");
                    }
                }

                println!(
                    "oggit.merge\tmerge_id={}\tstatus={}\tconflicts={}\tapplied={}",
                    merge_result.merge_id,
                    merge_result.status,
                    merge_result.conflict_count,
                    merge_result.applied_count
                );
                for detail in &merge_result.skipped_details {
                    println!("oggit.skipped\t{detail}");
                }
                if merge_result.status == "failed" {
                    bail!("branch merge failed");
                }
                return Ok(());
            }

            let copy_source_only_tables = !args.no_copy_source_only_tables;
            client
                .batch_execute("BEGIN")
                .await
                .context("failed to start branch merge transaction")?;

            let merge_result: Result<(Vec<(String, i64)>, Vec<tokio_opengauss::Row>)> = async {
                let (copied_tables, common_tables) = copy_source_only_tables_with_schema(
                    &mut client,
                    &source_endpoint,
                    &args.source_schema,
                    &args.target_schema,
                    &OGGIT_FDW_SCHEMA,
                    copy_source_only_tables,
                )
                .await?;

                let include_tables_sql = name_array_literal(&common_tables);
                let merge_sql = format!(
                    "SELECT schema_name, table_name, inserted_count, updated_count
                     FROM neon_branch_merge($1::name, $2::name, $3, {include_tables_sql}, false)"
                );
                let merged_tables = client
                    .query(
                        merge_sql.as_str(),
                        &[
                            &OGGIT_FDW_SCHEMA,
                            &args.target_schema,
                            &args.strategy.as_str(),
                        ],
                    )
                    .await?;

                client
                    .batch_execute("COMMIT")
                    .await
                    .context("failed to commit branch merge transaction")?;

                Ok((copied_tables, merged_tables))
            }
            .await;

            if merge_result.is_err() {
                if let Err(e) = client.batch_execute("ROLLBACK").await {
                    eprintln!("Warning: failed to roll back branch merge transaction: {e:#}");
                }
                if !args.keep_fdw {
                    if let Err(e) =
                        cleanup_branch_source_fdw(&mut client, &OGGIT_FDW_SCHEMA, &args.fdw_server)
                            .await
                    {
                        eprintln!("Warning: {e:#}");
                    }
                }
            }

            let (copied_tables, merged_tables) = merge_result.context("branch merge failed")?;

            if !args.keep_fdw {
                if let Err(e) =
                    cleanup_branch_source_fdw(&mut client, &OGGIT_FDW_SCHEMA, &args.fdw_server)
                        .await
                {
                    eprintln!("Warning: {e:#}");
                }
            }

            for (table_name, inserted_count) in copied_tables {
                println!(
                    "{}.{}\tinserted={}\tupdated=0",
                    args.target_schema, table_name, inserted_count
                );
            }

            for row in merged_tables {
                let schema_name: String = row.get(0);
                let table_name: String = row.get(1);
                let inserted_count: i64 = row.get(2);
                let updated_count: i64 = row.get(3);

                println!(
                    "{schema_name}.{table_name}\tinserted={inserted_count}\tupdated={updated_count}"
                );
            }
        }
        BranchCmd::MergeStatus(args) => {
            let needs_local = args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            let client = connect_to_branch_endpoint(&target_endpoint).await?;
            let (history, pending_conflict_count) = oggit_merge_status(&client, &args.merge_id)
                .await
                .context("failed to read oggit merge status")?
                .with_context(|| format!("merge {} not found", args.merge_id))?;

            println!(
                "oggit.merge\tmerge_id={}\tbranch={} ({})\tchild_timeline={}\tparent_timeline={}\tbase_lsn={}\tchild_to_lsn={}\tparent_to_lsn={}\tstrategy={}\tstatus={}\tconflicts={}\tpending={}",
                history.merge_id,
                target_endpoint.branch_name,
                target_endpoint.endpoint_id,
                history.child_timeline_id,
                history.parent_timeline_id,
                history.base_lsn,
                history.child_to_lsn,
                history.parent_to_lsn,
                history.strategy,
                history.status,
                history.conflict_count,
                pending_conflict_count
            );
        }
        BranchCmd::Conflicts(args) => {
            let needs_local = args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            println!(
                "Listing oggit merge conflicts on target branch '{}' ({}) for merge {}",
                target_endpoint.branch_name, target_endpoint.endpoint_id, args.merge_id
            );

            let client = connect_to_branch_endpoint(&target_endpoint).await?;
            for conflict in oggit_read_conflicts(&client, &args.merge_id)
                .await
                .context("failed to list oggit merge conflicts")?
            {
                println!(
                    "{}\t{}\t{}\t{}.{}\tobject={}\tkey={}\tours={}\ttheirs={}\tresolution={}\tstatus={}\t{}",
                    conflict.conflict_id,
                    conflict.conflict_scope,
                    conflict.conflict_type,
                    conflict.schema_name.unwrap_or_default(),
                    conflict.table_name.unwrap_or_default(),
                    conflict.object_name.unwrap_or_default(),
                    oggit_json_text(&conflict.key_json),
                    oggit_json_text(&conflict.ours_json),
                    oggit_json_text(&conflict.theirs_json),
                    conflict.resolution.unwrap_or_default(),
                    conflict.status,
                    conflict.reason
                );
            }
        }
        BranchCmd::Resolve(args) => {
            let needs_local = args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            let client = connect_to_branch_endpoint(&target_endpoint).await?;
            if let Some(custom_sql) = &args.custom_sql {
                oggit_resolve_conflict_sql(&client, &args.merge_id, args.conflict_id, custom_sql)
                    .await
                    .context("failed to store custom oggit conflict SQL")?;
                println!(
                    "Resolved conflict {} on target branch '{}' ({}) with custom SQL",
                    args.conflict_id, target_endpoint.branch_name, target_endpoint.endpoint_id
                );
            } else {
                let resolution = args
                    .resolution
                    .context("pass either --resolution or --custom-sql")?;
                oggit_resolve_conflict(
                    &client,
                    &args.merge_id,
                    args.conflict_id,
                    resolution.as_str(),
                )
                .await
                .context("failed to resolve oggit conflict")?;
                println!(
                    "Resolved conflict {} on target branch '{}' ({}) as {}",
                    args.conflict_id,
                    target_endpoint.branch_name,
                    target_endpoint.endpoint_id,
                    resolution.as_str()
                );
            }
        }
        BranchCmd::Continue(args) => {
            let needs_local = args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            println!(
                "Continuing oggit merge {} on target branch '{}' ({})",
                args.merge_id, target_endpoint.branch_name, target_endpoint.endpoint_id
            );

            let client = connect_to_branch_endpoint(&target_endpoint).await?;
            client
                .batch_execute("BEGIN")
                .await
                .context("failed to start oggit continue transaction")?;
            let result = oggit_continue_merge(&client, &args.merge_id).await;
            if result.is_err() {
                if let Err(e) = client.batch_execute("ROLLBACK").await {
                    eprintln!("Warning: failed to roll back oggit continue transaction: {e:#}");
                }
            }
            let result = result.context("failed to continue oggit merge")?;
            client
                .batch_execute("COMMIT")
                .await
                .context("failed to commit oggit continue transaction")?;
            if result.status == "applied" {
                oggit_finalize_merge_commit_lsn(&client, &result.merge_id)
                    .await
                    .context("failed to finalize oggit merge commit LSN")?;
            }
            println!(
                "oggit.merge\tmerge_id={}\tstatus={}\tconflicts={}\tapplied={}",
                result.merge_id, result.status, result.conflict_count, result.applied_count
            );
            for detail in &result.skipped_details {
                println!("oggit.skipped\t{detail}");
            }
        }
        BranchCmd::Abort(args) => {
            let needs_local = args.target_connstr.is_none();
            let tenant_id = if needs_local {
                Some(get_tenant_id(args.tenant_id, require_branch_env(env)?)?)
            } else {
                None
            };
            let cplane = if needs_local {
                Some(ComputeControlPlane::load(require_branch_env(env)?.clone())?)
            } else {
                None
            };
            let target_endpoint = resolve_branch_endpoint_ref(
                cplane.as_ref(),
                env,
                tenant_id,
                &args.target_branch,
                &args.target_endpoint,
                &args.target_connstr,
                "target",
                "target",
                &args.user,
                &args.database,
            )?;

            let client = connect_to_branch_endpoint(&target_endpoint).await?;
            client
                .batch_execute("BEGIN")
                .await
                .context("failed to start oggit abort transaction")?;
            let result = oggit_abort_merge(&client, &args.merge_id).await;
            if result.is_err() {
                if let Err(e) = client.batch_execute("ROLLBACK").await {
                    eprintln!("Warning: failed to roll back oggit abort transaction: {e:#}");
                }
            }
            result.context("failed to abort oggit merge")?;
            client
                .batch_execute("COMMIT")
                .await
                .context("failed to commit oggit abort transaction")?;
            println!(
                "Aborted oggit merge {} on target branch '{}' ({})",
                args.merge_id, target_endpoint.branch_name, target_endpoint.endpoint_id
            );
        }
    }

    Ok(())
}

async fn handle_endpoint(subcmd: &EndpointCmd, env: &local_env::LocalEnv) -> Result<()> {
    let mut cplane = ComputeControlPlane::load(env.clone())?;

    match subcmd {
        EndpointCmd::List(args) => {
            // TODO(sharding): this command shouldn't have to specify a shard ID: we should ask the storage controller
            // where shard 0 is attached, and query there.
            let tenant_shard_id = get_tenant_shard_id(args.tenant_shard_id, env)?;

            let timeline_name_mappings = env.timeline_name_mappings();

            let mut table = comfy_table::Table::new();

            table.load_preset(comfy_table::presets::NOTHING);

            table.set_header([
                "ENDPOINT",
                "ADDRESS",
                "TIMELINE",
                "BRANCH NAME",
                "LSN",
                "STATUS",
            ]);

            for (endpoint_id, endpoint) in cplane
                .endpoints
                .iter()
                .filter(|(_, endpoint)| endpoint.tenant_id == tenant_shard_id.tenant_id)
            {
                let lsn_str = match endpoint.mode {
                    ComputeMode::Static(lsn) => {
                        // -> read-only endpoint
                        // Use the node's LSN.
                        lsn.to_string()
                    }
                    _ => {
                        // As the LSN here refers to the one that the compute is started with,
                        // we display nothing as it is a primary/hot standby compute.
                        "---".to_string()
                    }
                };

                let branch_name = timeline_name_mappings
                    .get(&TenantTimelineId::new(
                        tenant_shard_id.tenant_id,
                        endpoint.timeline_id,
                    ))
                    .map(|name| name.as_str())
                    .unwrap_or("?");

                table.add_row([
                    endpoint_id.as_str(),
                    &endpoint.pg_address.to_string(),
                    &endpoint.timeline_id.to_string(),
                    branch_name,
                    lsn_str.as_str(),
                    &format!("{}", endpoint.status()),
                ]);
            }

            println!("{table}");
        }
        EndpointCmd::Create(args) => {
            let tenant_id = get_tenant_id(args.tenant_id, env)?;
            let branch_name = args
                .branch_name
                .clone()
                .unwrap_or(DEFAULT_BRANCH_NAME.to_owned());
            let endpoint_id = args
                .endpoint_id
                .clone()
                .unwrap_or_else(|| format!("ep-{branch_name}"));

            cplane.check_endpoint_id_available(&endpoint_id)?;

            let timeline_id = env
                .get_branch_timeline_id(&branch_name, tenant_id)
                .ok_or_else(|| anyhow!("Found no timeline id for branch name '{branch_name}'"))?;

            let mode = match (args.lsn, args.hot_standby) {
                (Some(lsn), false) => ComputeMode::Static(lsn),
                (None, true) => ComputeMode::Replica,
                (None, false) => ComputeMode::Primary,
                (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
            };

            match (mode, args.hot_standby) {
                (ComputeMode::Static(_), true) => {
                    bail!(
                        "Cannot start a node in hot standby mode when it is already configured as a static replica"
                    )
                }
                (ComputeMode::Primary, true) => {
                    bail!(
                        "Cannot start a node as a hot standby replica, it is already configured as primary node"
                    )
                }
                _ => {}
            }

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(mode, tenant_id, timeline_id, None)?;
            }

            cplane.new_endpoint(
                &endpoint_id,
                tenant_id,
                timeline_id,
                args.pg_port,
                args.external_http_port,
                args.internal_http_port,
                args.pg_version,
                mode,
                args.grpc,
                !args.update_catalog,
                false,
                args.privileged_role_name.clone(),
            )?;
        }
        EndpointCmd::Start(args) => {
            let endpoint_id = &args.endpoint_id;
            let pageserver_id = args.endpoint_pageserver_id;
            let remote_ext_base_url = &args.remote_ext_base_url;

            let default_generation = env
                .storage_controller
                .timelines_onto_safekeepers
                .then_some(1);
            let safekeepers_generation = args
                .safekeepers_generation
                .or(default_generation)
                .map(SafekeeperGeneration::new);
            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = if let Some(safekeepers) = parse_safekeepers(&args.safekeepers)? {
                safekeepers
            } else {
                env.safekeepers.iter().map(|sk| sk.id).collect()
            };

            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .ok_or_else(|| anyhow!("endpoint {endpoint_id} not found"))?;

            // If the endpoint is in Crashed state, clean up the pidfile so it can be restarted
            if endpoint.status() == EndpointStatus::Crashed {
                let pidfile_path = endpoint.pgdata().join("postmaster.pid");
                if pidfile_path.exists() {
                    std::fs::remove_file(&pidfile_path).with_context(|| {
                        format!(
                            "failed to remove crashed pidfile: {}",
                            pidfile_path.display()
                        )
                    })?;
                }
            }

            if !args.allow_multiple {
                cplane.check_conflicting_endpoints(
                    endpoint.mode,
                    endpoint.tenant_id,
                    endpoint.timeline_id,
                    Some(endpoint_id),
                )?;
            }

            let (pageservers, stripe_size) = if let Some(pageserver_id) = pageserver_id {
                let conf = env.get_pageserver_conf(pageserver_id).unwrap();
                // Use gRPC if requested.
                let pageserver = if endpoint.grpc {
                    let grpc_addr = conf.listen_grpc_addr.as_ref().expect("bad config");
                    let (host, port) = parse_host_port(grpc_addr)?;
                    let port = port.unwrap_or(DEFAULT_PAGESERVER_GRPC_PORT);
                    (PageserverProtocol::Grpc, host, port)
                } else {
                    let (host, port) = parse_host_port(&conf.listen_pg_addr)?;
                    let port = port.unwrap_or(5432);
                    (PageserverProtocol::Libpq, host, port)
                };
                // If caller is telling us what pageserver to use, this is not a tenant which is
                // fully managed by storage controller, therefore not sharded.
                (vec![pageserver], DEFAULT_STRIPE_SIZE)
            } else {
                // Look up the currently attached location of the tenant, and its striping metadata,
                // to pass these on to postgres.
                let storage_controller = StorageController::from_env(env);
                let locate_result = storage_controller.tenant_locate(endpoint.tenant_id).await?;
                let pageservers = futures::future::try_join_all(
                    locate_result.shards.into_iter().map(|shard| async move {
                        if let ComputeMode::Static(lsn) = endpoint.mode {
                            // Initialize LSN leases for static computes.
                            let conf = env.get_pageserver_conf(shard.node_id).unwrap();
                            let pageserver = PageServerNode::from_env(env, conf);

                            pageserver
                                .http_client
                                .timeline_init_lsn_lease(shard.shard_id, endpoint.timeline_id, lsn)
                                .await?;
                        }

                        let pageserver = if endpoint.grpc {
                            (
                                PageserverProtocol::Grpc,
                                Host::parse(&shard.listen_grpc_addr.expect("no gRPC address"))?,
                                shard.listen_grpc_port.expect("no gRPC port"),
                            )
                        } else {
                            (
                                PageserverProtocol::Libpq,
                                Host::parse(&shard.listen_pg_addr)?,
                                shard.listen_pg_port,
                            )
                        };
                        anyhow::Ok(pageserver)
                    }),
                )
                .await?;
                let stripe_size = locate_result.shard_params.stripe_size;

                (pageservers, stripe_size)
            };
            assert!(!pageservers.is_empty());

            let ps_conf = env.get_pageserver_conf(DEFAULT_PAGESERVER_ID)?;
            let auth_token = if matches!(ps_conf.pg_auth_type, AuthType::NeonJWT) {
                let claims = Claims::new(Some(endpoint.tenant_id), Scope::Tenant);

                Some(env.generate_auth_token(&claims)?)
            } else {
                None
            };

            let exp = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?
                + Duration::from_secs(86400))
            .as_secs();
            let claims = endpoint_storage::claims::EndpointStorageClaims {
                tenant_id: endpoint.tenant_id,
                timeline_id: endpoint.timeline_id,
                endpoint_id: endpoint_id.to_string(),
                exp,
            };

            let endpoint_storage_token = env.generate_auth_token(&claims)?;
            let endpoint_storage_addr = env.endpoint_storage.listen_addr.to_string();

            // Build the compute-side oggit worker config for Primary openGauss
            // endpoints. The worker runs inside compute and decodes this
            // branch's WAL into the oggit metadata tables.
            let oggit = build_oggit_endpoint_config(env, endpoint, &safekeepers).await;

            let args = control_plane::endpoint::EndpointStartArgs {
                auth_token,
                endpoint_storage_token,
                endpoint_storage_addr,
                safekeepers_generation,
                safekeepers,
                pageservers,
                remote_ext_base_url: remote_ext_base_url.clone(),
                shard_stripe_size: stripe_size.0 as usize,
                create_test_user: args.create_test_user,
                start_timeout: args.start_timeout,
                autoprewarm: args.autoprewarm,
                offload_lfc_interval_seconds: args.offload_lfc_interval_seconds,
                dev: args.dev,
                oggit,
            };

            println!("Starting existing endpoint {endpoint_id}...");
            endpoint.start(args).await?;
        }
        EndpointCmd::UpdatePageservers(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            let pageservers = match args.pageserver_id {
                Some(pageserver_id) => {
                    let pageserver =
                        PageServerNode::from_env(env, env.get_pageserver_conf(pageserver_id)?);

                    vec![(
                        PageserverProtocol::Libpq,
                        pageserver.pg_connection_config.host().clone(),
                        pageserver.pg_connection_config.port(),
                    )]
                }
                None => {
                    let storage_controller = StorageController::from_env(env);
                    storage_controller
                        .tenant_locate(endpoint.tenant_id)
                        .await?
                        .shards
                        .into_iter()
                        .map(|shard| {
                            (
                                PageserverProtocol::Libpq,
                                Host::parse(&shard.listen_pg_addr)
                                    .expect("Storage controller reported malformed host"),
                                shard.listen_pg_port,
                            )
                        })
                        .collect::<Vec<_>>()
                }
            };

            endpoint.update_pageservers_in_config(pageservers).await?;
        }
        EndpointCmd::Reconfigure(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            let pageservers = if let Some(ps_id) = args.endpoint_pageserver_id {
                let conf = env.get_pageserver_conf(ps_id)?;
                // Use gRPC if requested.
                let pageserver = if endpoint.grpc {
                    let grpc_addr = conf.listen_grpc_addr.as_ref().expect("bad config");
                    let (host, port) = parse_host_port(grpc_addr)?;
                    let port = port.unwrap_or(DEFAULT_PAGESERVER_GRPC_PORT);
                    (PageserverProtocol::Grpc, host, port)
                } else {
                    let (host, port) = parse_host_port(&conf.listen_pg_addr)?;
                    let port = port.unwrap_or(5432);
                    (PageserverProtocol::Libpq, host, port)
                };
                vec![pageserver]
            } else {
                let storage_controller = StorageController::from_env(env);
                storage_controller
                    .tenant_locate(endpoint.tenant_id)
                    .await?
                    .shards
                    .into_iter()
                    .map(|shard| {
                        // Use gRPC if requested.
                        if endpoint.grpc {
                            (
                                PageserverProtocol::Grpc,
                                Host::parse(&shard.listen_grpc_addr.expect("no gRPC address"))
                                    .expect("bad hostname"),
                                shard.listen_grpc_port.expect("no gRPC port"),
                            )
                        } else {
                            (
                                PageserverProtocol::Libpq,
                                Host::parse(&shard.listen_pg_addr).expect("bad hostname"),
                                shard.listen_pg_port,
                            )
                        }
                    })
                    .collect::<Vec<_>>()
            };
            // If --safekeepers argument is given, use only the listed
            // safekeeper nodes; otherwise all from the env.
            let safekeepers = parse_safekeepers(&args.safekeepers)?;
            endpoint
                .reconfigure(Some(pageservers), None, safekeepers, None)
                .await?;
        }
        EndpointCmd::RefreshConfiguration(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id.as_str())
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            endpoint.refresh_configuration().await?;
        }
        EndpointCmd::Stop(args) => {
            let endpoint_id = &args.endpoint_id;
            let endpoint = cplane
                .endpoints
                .get(endpoint_id)
                .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?;
            match endpoint.stop(args.mode, args.destroy).await?.lsn {
                Some(lsn) => println!("{lsn}"),
                None => println!("null"),
            }
        }
        EndpointCmd::GenerateJwt(args) => {
            let endpoint = {
                let endpoint_id = &args.endpoint_id;

                cplane
                    .endpoints
                    .get(endpoint_id)
                    .with_context(|| format!("postgres endpoint {endpoint_id} is not found"))?
            };

            let jwt = endpoint.generate_jwt(args.scope)?;

            print!("{jwt}");
        }
    }

    Ok(())
}

/// Parse --safekeepers as list of safekeeper ids.
fn parse_safekeepers(safekeepers_str: &Option<String>) -> Result<Option<Vec<NodeId>>> {
    if let Some(safekeepers_str) = safekeepers_str {
        let mut safekeepers: Vec<NodeId> = Vec::new();
        for sk_id in safekeepers_str.split(',').map(str::trim) {
            let sk_id = NodeId(
                u64::from_str(sk_id)
                    .map_err(|_| anyhow!("invalid node ID \"{sk_id}\" in --safekeepers list"))?,
            );
            safekeepers.push(sk_id);
        }
        Ok(Some(safekeepers))
    } else {
        Ok(None)
    }
}

fn handle_mappings(subcmd: &MappingsCmd, env: &mut local_env::LocalEnv) -> Result<()> {
    match subcmd {
        MappingsCmd::Map(args) => {
            env.register_branch_mapping(
                args.branch_name.to_owned(),
                args.tenant_id,
                args.timeline_id,
            )?;

            Ok(())
        }
    }
}

fn get_pageserver(
    env: &local_env::LocalEnv,
    pageserver_id_arg: Option<NodeId>,
) -> Result<PageServerNode> {
    let node_id = pageserver_id_arg.unwrap_or(DEFAULT_PAGESERVER_ID);

    Ok(PageServerNode::from_env(
        env,
        env.get_pageserver_conf(node_id)?,
    ))
}

async fn handle_pageserver(subcmd: &PageserverCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        PageserverCmd::Start(args) => {
            if let Err(e) = get_pageserver(env, args.pageserver_id)?
                .start(&args.start_timeout)
                .await
            {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Stop(args) => {
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = get_pageserver(env, args.pageserver_id)?.stop(immediate) {
                eprintln!("pageserver stop failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Restart(args) => {
            let pageserver = get_pageserver(env, args.pageserver_id)?;
            //TODO what shutdown strategy should we use here?
            if let Err(e) = pageserver.stop(false) {
                eprintln!("pageserver stop failed: {e}");
                exit(1);
            }

            if let Err(e) = pageserver.start(&args.start_timeout).await {
                eprintln!("pageserver start failed: {e}");
                exit(1);
            }
        }

        PageserverCmd::Status(args) => {
            match get_pageserver(env, args.pageserver_id)?
                .check_status()
                .await
            {
                Ok(_) => println!("Page server is up and running"),
                Err(err) => {
                    eprintln!("Page server is not available: {err}");
                    exit(1);
                }
            }
        }
    }
    Ok(())
}

async fn handle_storage_controller(
    subcmd: &StorageControllerCmd,
    env: &local_env::LocalEnv,
) -> Result<()> {
    let svc = StorageController::from_env(env);
    match subcmd {
        StorageControllerCmd::Start(args) => {
            let start_args = NeonStorageControllerStartArgs {
                instance_id: args.instance_id,
                base_port: args.base_port,
                start_timeout: args.start_timeout,
                handle_ps_local_disk_loss: args.handle_ps_local_disk_loss,
            };

            if let Err(e) = svc.start(start_args).await {
                eprintln!("start failed: {e}");
                exit(1);
            }
        }

        StorageControllerCmd::Stop(args) => {
            let stop_args = NeonStorageControllerStopArgs {
                instance_id: args.instance_id,
                immediate: match args.stop_mode {
                    StopMode::Fast => false,
                    StopMode::Immediate => true,
                },
            };
            if let Err(e) = svc.stop(stop_args).await {
                eprintln!("stop failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

fn get_safekeeper(env: &local_env::LocalEnv, id: NodeId) -> Result<SafekeeperNode> {
    if let Some(node) = env.safekeepers.iter().find(|node| node.id == id) {
        Ok(SafekeeperNode::from_env(env, node))
    } else {
        bail!("could not find safekeeper {id}")
    }
}

async fn handle_safekeeper(subcmd: &SafekeeperCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        SafekeeperCmd::Start(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {e}");
                exit(1);
            }
        }

        SafekeeperCmd::Stop(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {e}");
                exit(1);
            }
        }

        SafekeeperCmd::Restart(args) => {
            let safekeeper = get_safekeeper(env, args.id)?;
            let immediate = match args.stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };

            if let Err(e) = safekeeper.stop(immediate) {
                eprintln!("safekeeper stop failed: {e}");
                exit(1);
            }

            if let Err(e) = safekeeper.start(&args.extra_opt, &args.start_timeout).await {
                eprintln!("safekeeper start failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_endpoint_storage(
    subcmd: &EndpointStorageCmd,
    env: &local_env::LocalEnv,
) -> Result<()> {
    use EndpointStorageCmd::*;
    let storage = EndpointStorage::from_env(env);

    // In tests like test_forward_compatibility or test_graceful_cluster_restart
    // old neon binaries (without endpoint_storage) are present
    if !storage.bin.exists() {
        eprintln!(
            "{} binary not found. Ignore if this is a compatibility test",
            storage.bin
        );
        return Ok(());
    }

    match subcmd {
        Start(EndpointStorageStartCmd { start_timeout }) => {
            if let Err(e) = storage.start(start_timeout).await {
                eprintln!("endpoint_storage start failed: {e}");
                exit(1);
            }
        }
        Stop(EndpointStorageStopCmd { stop_mode }) => {
            let immediate = match stop_mode {
                StopMode::Fast => false,
                StopMode::Immediate => true,
            };
            if let Err(e) = storage.stop(immediate) {
                eprintln!("proxy stop failed: {e}");
                exit(1);
            }
        }
    };
    Ok(())
}

async fn handle_storage_broker(subcmd: &StorageBrokerCmd, env: &local_env::LocalEnv) -> Result<()> {
    match subcmd {
        StorageBrokerCmd::Start(args) => {
            let storage_broker = StorageBroker::from_env(env);
            if let Err(e) = storage_broker.start(&args.start_timeout).await {
                eprintln!("broker start failed: {e}");
                exit(1);
            }
        }

        StorageBrokerCmd::Stop(_args) => {
            // FIXME: stop_mode unused
            let storage_broker = StorageBroker::from_env(env);
            if let Err(e) = storage_broker.stop() {
                eprintln!("broker stop failed: {e}");
                exit(1);
            }
        }
    }
    Ok(())
}

async fn handle_start_all(
    args: &StartCmdArgs,
    env: &'static local_env::LocalEnv,
) -> anyhow::Result<()> {
    // FIXME: this was called "retry_timeout", is it right?
    let Err(errors) = handle_start_all_impl(env, args.timeout).await else {
        neon_start_status_check(env, args.timeout.as_ref())
            .await
            .context("status check after successful startup of all services")?;
        return Ok(());
    };

    eprintln!("startup failed because one or more services could not be started");

    for e in errors {
        eprintln!("{e}");
        let debug_repr = format!("{e:?}");
        for line in debug_repr.lines() {
            eprintln!("  {line}");
        }
    }

    try_stop_all(env, true).await;

    exit(2);
}

/// Returns Ok() if and only if all services could be started successfully.
/// Otherwise, returns the list of errors that occurred during startup.
async fn handle_start_all_impl(
    env: &'static local_env::LocalEnv,
    retry_timeout: humantime::Duration,
) -> Result<(), Vec<anyhow::Error>> {
    // Endpoints are not started automatically

    let mut js = JoinSet::new();

    // force infalliblity through closure
    #[allow(clippy::redundant_closure_call)]
    (|| {
        js.spawn(async move {
            let storage_broker = StorageBroker::from_env(env);
            storage_broker
                .start(&retry_timeout)
                .await
                .map_err(|e| e.context("start storage_broker"))
        });

        js.spawn(async move {
            let storage_controller = StorageController::from_env(env);
            storage_controller
                .start(NeonStorageControllerStartArgs::with_default_instance_id(
                    retry_timeout,
                ))
                .await
                .map_err(|e| e.context("start storage_controller"))
        });

        for ps_conf in &env.pageservers {
            js.spawn(async move {
                let pageserver = PageServerNode::from_env(env, ps_conf);
                pageserver
                    .start(&retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start pageserver {}", ps_conf.id)))
            });
        }

        for node in env.safekeepers.iter() {
            js.spawn(async move {
                let safekeeper = SafekeeperNode::from_env(env, node);
                safekeeper
                    .start(&[], &retry_timeout)
                    .await
                    .map_err(|e| e.context(format!("start safekeeper {}", safekeeper.id)))
            });
        }

        js.spawn(async move {
            EndpointStorage::from_env(env)
                .start(&retry_timeout)
                .await
                .map_err(|e| e.context("start endpoint_storage"))
        });
    })();

    let mut errors = Vec::new();
    while let Some(result) = js.join_next().await {
        let result = result.expect("we don't panic or cancel the tasks");
        if let Err(e) = result {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(())
}

async fn neon_start_status_check(
    env: &local_env::LocalEnv,
    retry_timeout: &Duration,
) -> anyhow::Result<()> {
    const RETRY_INTERVAL: Duration = Duration::from_millis(100);
    const NOTICE_AFTER_RETRIES: Duration = Duration::from_secs(5);

    let storcon = StorageController::from_env(env);

    let retries = retry_timeout.as_millis() / RETRY_INTERVAL.as_millis();
    let notice_after_retries = retry_timeout.as_millis() / NOTICE_AFTER_RETRIES.as_millis();

    println!("\nRunning neon status check");

    for retry in 0..retries {
        if retry == notice_after_retries {
            println!("\nNeon status check has not passed yet, continuing to wait")
        }

        let mut passed = true;
        let mut nodes = storcon.node_list().await?;
        let mut pageservers = env.pageservers.clone();

        if nodes.len() != pageservers.len() {
            continue;
        }

        nodes.sort_by_key(|ps| ps.id);
        pageservers.sort_by_key(|ps| ps.id);

        for (idx, pageserver) in pageservers.iter().enumerate() {
            let node = &nodes[idx];
            if node.id != pageserver.id {
                passed = false;
                break;
            }

            if !matches!(node.availability, NodeAvailabilityWrapper::Active) {
                passed = false;
                break;
            }
        }

        if passed {
            println!("\nNeon started and passed status check");
            return Ok(());
        }

        tokio::time::sleep(RETRY_INTERVAL).await;
    }

    anyhow::bail!("\nNeon passed status check")
}

async fn handle_stop_all(args: &StopCmdArgs, env: &local_env::LocalEnv) -> Result<()> {
    let immediate = match args.mode {
        StopMode::Fast => false,
        StopMode::Immediate => true,
    };

    try_stop_all(env, immediate).await;

    Ok(())
}

async fn try_stop_all(env: &local_env::LocalEnv, immediate: bool) {
    let mode = if immediate {
        EndpointTerminateMode::Immediate
    } else {
        EndpointTerminateMode::Fast
    };
    // Stop all endpoints
    match ComputeControlPlane::load(env.clone()) {
        Ok(cplane) => {
            for (_k, node) in cplane.endpoints {
                if let Err(e) = node.stop(mode, false).await {
                    eprintln!("postgres stop failed: {e:#}");
                }
            }
        }
        Err(e) => {
            eprintln!("postgres stop failed, could not restore control plane data from env: {e:#}")
        }
    }

    let storage = EndpointStorage::from_env(env);
    if let Err(e) = storage.stop(immediate) {
        eprintln!("endpoint_storage stop failed: {e:#}");
    }

    for ps_conf in &env.pageservers {
        let pageserver = PageServerNode::from_env(env, ps_conf);
        if let Err(e) = pageserver.stop(immediate) {
            eprintln!("pageserver {} stop failed: {:#}", ps_conf.id, e);
        }
    }

    for node in env.safekeepers.iter() {
        let safekeeper = SafekeeperNode::from_env(env, node);
        if let Err(e) = safekeeper.stop(immediate) {
            eprintln!("safekeeper {} stop failed: {:#}", safekeeper.id, e);
        }
    }

    let storage_broker = StorageBroker::from_env(env);
    if let Err(e) = storage_broker.stop() {
        eprintln!("neon broker stop failed: {e:#}");
    }

    // Stop all storage controller instances. In the most common case there's only one,
    // but iterate though the base data directory in order to discover the instances.
    let storcon_instances = env
        .storage_controller_instances()
        .await
        .expect("Must inspect data dir");
    for (instance_id, _instance_dir_path) in storcon_instances {
        let storage_controller = StorageController::from_env(env);
        let stop_args = NeonStorageControllerStopArgs {
            instance_id,
            immediate,
        };

        if let Err(e) = storage_controller.stop(stop_args).await {
            eprintln!("Storage controller instance {instance_id} stop failed: {e:#}");
        }
    }
}
