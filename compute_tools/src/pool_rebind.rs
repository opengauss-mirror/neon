//! Narrow, authenticated pool binding API. No full spec or credentials are returned.
use anyhow::{Context, Result};
use compute_api::responses::{ComputeConfig, ComputeStatus, ComputeStatusResponse};
use compute_api::spec::{ComputeSpec, GenericOption};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::compute::{ComputeNode, ComputeState, PG_PID, ParsedSpec, compute_spec_setting};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PoolRebindRequest {
    pub operation_id: String,
    pub expected_generation: String,
    pub tenant_id: String,
    pub timeline_id: String,
    pub ancestor_timeline_id: String,
    pub ancestor_start_lsn: String,
    pub pageserver_connstring: String,
    pub safekeeper_connstrings: String,
    pub safekeeper_http_urls: String,
    pub oggit_enabled: bool,
    pub oggit_database: String,
}

#[derive(Deserialize, Serialize)]
struct Operation {
    request: PoolRebindRequest,
    phase: String,
    compute_ctl_before: Value,
    database_pid_before: u32,
    result: Option<Value>,
}

pub fn identity() -> Result<Value> {
    let pid = std::process::id();
    let stat = fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .context("invalid /proc/self/stat")?
        .1
        .split_whitespace()
        .collect();
    Ok(json!({"pid":pid,"start_ticks":fields.get(19).context("missing process start time")?}))
}

pub fn generation(state: &ComputeState) -> String {
    // Include the active spec as /configure can change it without changing
    // start_time. The hash also fences requests captured before such a change.
    let spec = serde_json::to_vec(&state.pspec.as_ref().map(|s| &s.spec))
        .expect("compute spec serialization cannot fail");
    let digest = ring::digest::digest(&ring::digest::SHA256, &spec);
    let hash = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{}:{}:{}",
        std::process::id(),
        state.start_time.to_rfc3339(),
        hash
    )
}

fn recovery_root() -> Result<PathBuf> {
    let root = std::env::var_os("COMPUTE_CTL_POOL_RECOVERY_DIR")
        .context("COMPUTE_CTL_POOL_RECOVERY_DIR is required for pool rebind")?;
    let root = PathBuf::from(root);
    anyhow::ensure!(
        root.is_absolute(),
        "pool recovery directory must be absolute"
    );
    Ok(root)
}

fn operation_path() -> Result<PathBuf> {
    Ok(recovery_root()?.join("pool-operation.json"))
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("missing parent directory")?;
    fs::create_dir_all(parent)?;
    let tmp = path.with_extension("pool-tmp");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn load_operation() -> Result<Option<Operation>> {
    let path = match std::env::var_os("COMPUTE_CTL_POOL_RECOVERY_DIR") {
        Some(_) => operation_path()?,
        None => return Ok(None),
    };
    match fs::read(path) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("invalid pool operation journal")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn save_operation(operation: &Operation) -> Result<()> {
    atomic_write(&operation_path()?, &serde_json::to_vec_pretty(operation)?)
}

fn quote_shell(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// Called while the native pending journal still exists, before status becomes Running.
/// The single durable bundle is authoritative; the supervisor derives both
/// startup files from it before sourcing environment variables.
pub fn persist_supervisor_config(config: &ComputeConfig) -> Result<()> {
    if std::env::var_os("COMPUTE_CTL_POOL_RECOVERY_DIR").is_none() {
        return Ok(());
    }
    let root = recovery_root()?;
    let spec = config.spec.as_ref().context("missing active spec")?;
    let parsed = ParsedSpec::try_from(spec.clone()).map_err(anyhow::Error::msg)?;
    let enabled = compute_spec_setting(spec, "neon.oggit_enabled")
        .is_some_and(|s| matches!(s.to_ascii_lowercase().as_str(), "on" | "true" | "1" | "yes"));
    let settings = [
        ("TENANT_ID", parsed.tenant_id.to_string()),
        ("TIMELINE_ID", parsed.timeline_id.to_string()),
        (
            "ANCESTOR_TIMELINE_ID",
            compute_spec_setting(spec, "neon.oggit_ancestor_timeline_id").unwrap_or_default(),
        ),
        (
            "ANCESTOR_START_LSN",
            compute_spec_setting(spec, "neon.oggit_branch_start_lsn")
                .unwrap_or_else(|| "0/0".into()),
        ),
        ("OGGIT_ENABLED", enabled.to_string()),
        (
            "OGGIT_DATABASE",
            compute_spec_setting(spec, "neon.oggit_database").unwrap_or_else(|| "postgres".into()),
        ),
        (
            "SAFEKEEPER_CONNSTRINGS",
            compute_spec_setting(spec, "neon.safekeepers").unwrap_or_default(),
        ),
        (
            "OGGIT_SAFEKEEPER_HTTP_URLS",
            compute_spec_setting(spec, "neon.oggit_safekeeper_http_urls").unwrap_or_default(),
        ),
    ];
    let text = settings
        .iter()
        .map(|(k, v)| format!("export {k}={}\n", quote_shell(v)))
        .collect::<String>();
    atomic_write(
        &root.join("pool-binding-committed.json"),
        &serde_json::to_vec_pretty(&json!({"config": config, "environment": text}))?,
    )?;
    atomic_write(
        &root.join("configs/config.json"),
        &serde_json::to_vec_pretty(config)?,
    )?;
    atomic_write(&root.join("pool-rebind.env"), text.as_bytes())?;
    Ok(())
}

pub fn context(compute: &ComputeNode) -> Result<Value> {
    let state = compute.state.lock().unwrap().clone();
    let status = ComputeStatusResponse::from(&state);
    let warm = if let Some(pspec) = &state.pspec {
        json!({
            "neon.tenant_id":pspec.tenant_id.to_string(),
            "neon.timeline_id":pspec.timeline_id.to_string(),
            "neon.pageserver_connstring":pspec.pageserver_connstr,
            "neon.safekeepers":compute_spec_setting(&pspec.spec,"neon.safekeepers").unwrap_or_default(),
            "neon.oggit_enabled":if compute_spec_setting(&pspec.spec,"neon.oggit_enabled")
                .is_some_and(|s|matches!(s.to_ascii_lowercase().as_str(),"true"|"on"|"1"|"yes")) {"on"} else {"off"},
        })
    } else {
        Value::Null
    };
    let active = load_operation()?.map(|op| {
        let matching = completed_binding_matches(&state, &op.request);
        let phase = if matching {
            "completed"
        } else if op.phase == "completed" {
            "unresolved"
        } else {
            &op.phase
        };
        json!({"id":op.request.operation_id,"phase":phase,
            "target_tenant_id":op.request.tenant_id,"target_timeline_id":op.request.timeline_id})
    });
    Ok(
        json!({"generation":generation(&state),"status":status.status,
        "warm_settings":warm,"compute_ctl":identity()?,"active_operation":active,
        "tenant":status.tenant,"timeline":status.timeline,"error":status.error}),
    )
}

fn validate_request(request: &PoolRebindRequest) -> Result<()> {
    anyhow::ensure!(
        !request.operation_id.is_empty()
            && request.operation_id.len() <= 128
            && request
                .operation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid operation_id"
    );
    let _: utils::id::TenantId = request.tenant_id.parse()?;
    let _: utils::id::TimelineId = request.timeline_id.parse()?;
    if !request.ancestor_timeline_id.is_empty() {
        let _: utils::id::TimelineId = request.ancestor_timeline_id.parse()?;
    }
    let _: utils::lsn::Lsn = request.ancestor_start_lsn.parse()?;
    anyhow::ensure!(
        !request.pageserver_connstring.is_empty() && !request.safekeeper_connstrings.is_empty(),
        "missing storage routes"
    );
    anyhow::ensure!(!request.oggit_database.is_empty(), "missing database");
    Ok(())
}

fn requested_settings(request: &PoolRebindRequest) -> Vec<(&'static str, String)> {
    let mut settings = vec![
        ("neon.tenant_id", request.tenant_id.clone()),
        ("neon.timeline_id", request.timeline_id.clone()),
        (
            "neon.pageserver_connstring",
            request.pageserver_connstring.clone(),
        ),
        ("neon.safekeepers", request.safekeeper_connstrings.clone()),
        (
            "neon.oggit_enabled",
            if request.oggit_enabled { "on" } else { "off" }.to_owned(),
        ),
        ("neon.oggit_database", request.oggit_database.clone()),
        ("neon.oggit_tenant_id", request.tenant_id.clone()),
        ("neon.oggit_timeline_id", request.timeline_id.clone()),
        (
            "neon.oggit_ancestor_timeline_id",
            request.ancestor_timeline_id.clone(),
        ),
        (
            "neon.oggit_branch_start_lsn",
            request.ancestor_start_lsn.clone(),
        ),
        (
            "neon.oggit_safekeeper_http_urls",
            request.safekeeper_http_urls.clone(),
        ),
    ];
    if request.oggit_enabled {
        settings.push(("enable_subscription", "on".into()));
    }
    settings
}

fn completed_binding_matches(state: &ComputeState, request: &PoolRebindRequest) -> bool {
    state.status == ComputeStatus::Running
        && state
            .pspec
            .as_ref()
            .is_some_and(|spec| binding_matches(spec, request))
}

/// Compare the complete requested binding, not merely its operation marker.
/// In particular, supervisor recovery must never mistake mixed OGGIT settings
/// from an older binding for a completed operation on the target timeline.
fn binding_matches(spec: &ParsedSpec, request: &PoolRebindRequest) -> bool {
    if spec.spec.operation_uuid.as_deref() != Some(request.operation_id.as_str())
        || request.tenant_id.parse::<utils::id::TenantId>().ok() != Some(spec.tenant_id)
        || request.timeline_id.parse::<utils::id::TimelineId>().ok() != Some(spec.timeline_id)
        || spec.pageserver_connstr != request.pageserver_connstring
        || spec
            .safekeeper_connstrings
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != request
                .safekeeper_connstrings
                .split(',')
                .collect::<Vec<_>>()
    {
        return false;
    }
    requested_settings(request).iter().all(|(name, expected)| {
        let Some(actual) = compute_spec_setting(&spec.spec, name) else {
            return false;
        };
        if *name == "neon.oggit_enabled" || *name == "enable_subscription" {
            let enabled = *name == "enable_subscription" || request.oggit_enabled;
            match actual.to_ascii_lowercase().as_str() {
                "on" | "true" | "1" | "yes" => enabled,
                "off" | "false" | "0" | "no" => !enabled,
                _ => false,
            }
        } else {
            actual == *expected
        }
    })
}

fn target_spec(mut spec: ComputeSpec, request: &PoolRebindRequest) -> Result<ParsedSpec> {
    validate_request(request)?;
    spec.tenant_id = Some(request.tenant_id.parse()?);
    spec.timeline_id = Some(request.timeline_id.parse()?);
    spec.pageserver_connstring = Some(request.pageserver_connstring.clone());
    spec.safekeeper_connstrings = request
        .safekeeper_connstrings
        .split(',')
        .map(str::to_owned)
        .collect();
    spec.operation_uuid = Some(request.operation_id.clone());
    let entries = requested_settings(request);
    let options = spec.cluster.settings.get_or_insert_with(Vec::new);
    options.retain(|o| !entries.iter().any(|(name, _)| *name == o.name));
    options.extend(entries.into_iter().map(|(name, value)| {
        GenericOption {
            name: name.into(),
            value: Some(value),
            vartype: if name == "neon.oggit_enabled" || name == "enable_subscription" {
                "bool"
            } else {
                "string"
            }
            .into(),
        }
    }));
    ParsedSpec::try_from(spec).map_err(anyhow::Error::msg)
}

type ApiResult = std::result::Result<Value, (http::StatusCode, String)>;
fn internal(error: impl std::fmt::Display) -> (http::StatusCode, String) {
    (http::StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
fn conflict(message: &str) -> (http::StatusCode, String) {
    (http::StatusCode::CONFLICT, message.into())
}

/// The HTTP layer serializes these operations. A cancelled HTTP request does not
/// cancel this blocking worker: recovery observes operation_id and effective state.
pub fn rebind(compute: &ComputeNode, request: PoolRebindRequest) -> ApiResult {
    let started = Instant::now();
    validate_request(&request).map_err(|e| (http::StatusCode::BAD_REQUEST, e.to_string()))?;
    recovery_root().map_err(internal)?;
    let existing = load_operation().map_err(internal)?;
    let state = compute.state.lock().unwrap().clone();
    if let Some(mut old) = existing {
        if old.request.operation_id == request.operation_id {
            if old.request != request {
                return Err(conflict(
                    "operation_id was already used with different parameters",
                ));
            }
            if completed_binding_matches(&state, &request) {
                // Timings describe the original rebind, but process identity and
                // generation must describe the current instance after recovery.
                let timing = |key: &str| {
                    old.result
                        .as_ref()
                        .and_then(|v| v.get(key))
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0)
                };
                let response = result(
                    compute,
                    &old,
                    timing("prepare_ms"),
                    timing("http_rebind_ms"),
                    true,
                )
                .map_err(internal)?;
                return finish_operation(&mut old, response).map_err(internal);
            }
            return Err(conflict(
                "operation outcome is unresolved; inspect binding_context before recovery",
            ));
        }
    }
    if state.status != ComputeStatus::Running {
        return Err(conflict("compute is not running"));
    }
    if generation(&state) != request.expected_generation {
        return Err(conflict("stale binding generation"));
    }
    let original = state.pspec.context("no active spec").map_err(internal)?;
    let pspec = target_spec(original.spec, &request)
        .map_err(|e| (http::StatusCode::BAD_REQUEST, e.to_string()))?;
    let mut operation = Operation {
        request: request.clone(),
        phase: "preparing".into(),
        compute_ctl_before: identity().map_err(internal)?,
        database_pid_before: PG_PID.load(Ordering::SeqCst),
        result: None,
    };
    save_operation(&operation).map_err(internal)?;
    compute
        .begin_rebind_checked(pspec, Some(&request.expected_generation))
        .map_err(|e| conflict(&e.to_string()))?;
    let prepare_ms = started.elapsed().as_secs_f64() * 1000.0;
    let rebind_started = Instant::now();
    {
        let mut state = compute.state.lock().unwrap();
        while !matches!(state.status, ComputeStatus::Running | ComputeStatus::Failed) {
            state = compute.state_changed.wait(state).unwrap();
        }
        if state.status != ComputeStatus::Running {
            operation.phase = "failed".into();
            save_operation(&operation).map_err(internal)?;
            return Err(internal(
                state
                    .error
                    .clone()
                    .unwrap_or_else(|| "rebind failed".into()),
            ));
        }
    }
    let response = result(
        compute,
        &operation,
        prepare_ms,
        rebind_started.elapsed().as_secs_f64() * 1000.0,
        false,
    )
    .map_err(internal)?;
    finish_operation(&mut operation, response).map_err(internal)
}

fn finish_operation(operation: &mut Operation, mut response: Value) -> Result<Value> {
    operation.phase = "completed".into();
    operation.result = Some(response.clone());
    let persist_started = Instant::now();
    save_operation(operation)?;
    // A journal cannot contain the duration of its own fsync. Populate it only
    // after the write in this response; an idempotent replay measures its own
    // completion-journal write rather than returning a fabricated zero.
    response["persist_ms"] = json!(persist_started.elapsed().as_secs_f64() * 1000.0);
    Ok(response)
}

fn result(
    compute: &ComputeNode,
    operation: &Operation,
    prepare_ms: f64,
    rebind_ms: f64,
    replayed: bool,
) -> Result<Value> {
    let state = compute.state.lock().unwrap().clone();
    let status = ComputeStatusResponse::from(&state);
    anyhow::ensure!(
        completed_binding_matches(&state, &operation.request),
        "unexpected final binding identity or settings"
    );
    let current_identity = identity()?;
    let restarted = current_identity != operation.compute_ctl_before;
    Ok(json!({"generation":generation(&state),"status":status,
        "operation_id":operation.request.operation_id,
        "replayed":replayed,"compute_ctl_restarted":restarted,
        "compute_ctl_before":operation.compute_ctl_before,"compute_ctl_after":current_identity,
        "database_pid_before":operation.database_pid_before,"database_pid_after":PG_PID.load(Ordering::SeqCst),
        "prepare_ms":prepare_ms,"http_rebind_ms":rebind_ms,"persist_ms":null}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> PoolRebindRequest {
        PoolRebindRequest {
            operation_id: "branch-test-1".into(),
            expected_generation: "previous-generation".into(),
            tenant_id: "11111111111111111111111111111111".into(),
            timeline_id: "22222222222222222222222222222222".into(),
            ancestor_timeline_id: "33333333333333333333333333333333".into(),
            ancestor_start_lsn: "0/123456".into(),
            pageserver_connstring: "host=pageserver port=6400".into(),
            safekeeper_connstrings: "safekeeper:5454".into(),
            safekeeper_http_urls: "http://safekeeper:7676".into(),
            oggit_enabled: true,
            oggit_database: "postgres".into(),
        }
    }

    fn fixture_spec() -> ComputeSpec {
        serde_json::from_str(include_str!("../tests/cluster_spec.json")).unwrap()
    }

    #[test]
    fn target_spec_replaces_duplicate_settings_and_enables_oggit_prerequisite() {
        let request = request();
        let mut original = fixture_spec();
        let options = original.cluster.settings.get_or_insert_with(Vec::new);
        for _ in 0..2 {
            options.push(GenericOption {
                name: "neon.timeline_id".into(),
                value: Some("stale".into()),
                vartype: "string".into(),
            });
        }
        let role_count = original.cluster.roles.len();
        let spec = target_spec(original, &request).unwrap();
        assert!(binding_matches(&spec, &request));
        assert_eq!(spec.spec.cluster.roles.len(), role_count);
        assert_eq!(
            compute_spec_setting(&spec.spec, "enable_subscription").as_deref(),
            Some("on")
        );
        let options = spec.spec.cluster.settings.as_ref().unwrap();
        for (name, _) in requested_settings(&request) {
            assert_eq!(
                options.iter().filter(|o| o.name == name).count(),
                1,
                "{name}"
            );
        }
    }

    #[test]
    fn completed_binding_rejects_mixed_recovery_settings() {
        let request = request();
        let spec = target_spec(fixture_spec(), &request).unwrap();
        for (name, _) in requested_settings(&request) {
            let mut mixed = spec.clone();
            let option = mixed
                .spec
                .cluster
                .settings
                .as_mut()
                .unwrap()
                .iter_mut()
                .find(|o| o.name == name)
                .unwrap();
            option.value = Some("stale".into());
            assert!(!binding_matches(&mixed, &request), "accepted stale {name}");
        }
        let mut state = ComputeState::new();
        state.pspec = Some(spec);
        state.status = ComputeStatus::Running;
        assert!(completed_binding_matches(&state, &request));
        state.status = ComputeStatus::Rebinding;
        assert!(!completed_binding_matches(&state, &request));
        state.status = ComputeStatus::Running;
        state.pspec.as_mut().unwrap().spec.operation_uuid = Some("other-operation".into());
        assert!(!completed_binding_matches(&state, &request));
    }

    #[test]
    fn generation_changes_when_spec_changes_without_restart() {
        let request = request();
        let mut state = ComputeState::new();
        state.pspec = Some(target_spec(fixture_spec(), &request).unwrap());
        state.status = ComputeStatus::Running;
        let original_generation = generation(&state);
        let original_start = state.start_time;
        state.pspec.as_mut().unwrap().spec.cluster.postgresql_conf = Some("work_mem=16MB".into());
        assert_eq!(state.start_time, original_start);
        assert_ne!(generation(&state), original_generation);
        assert_eq!(generation(&state), generation(&state.clone()));
    }

    #[test]
    fn shell_quote_cannot_execute_substitution() {
        assert_eq!(quote_shell("x'$(id)"), "'x'\"'\"'$(id)'");
    }
    #[test]
    fn private_atomic_file_and_replace() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        atomic_write(&path, b"one").unwrap();
        atomic_write(&path, b"two").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    #[test]
    fn reject_bad_operation_before_rebind() {
        let req = PoolRebindRequest {
            operation_id: "bad/operation".into(),
            expected_generation: "".into(),
            tenant_id: "".into(),
            timeline_id: "".into(),
            ancestor_timeline_id: "".into(),
            ancestor_start_lsn: "0/0".into(),
            pageserver_connstring: "".into(),
            safekeeper_connstrings: "".into(),
            safekeeper_http_urls: "".into(),
            oggit_enabled: false,
            oggit_database: "postgres".into(),
        };
        assert!(validate_request(&req).is_err());
    }
}
