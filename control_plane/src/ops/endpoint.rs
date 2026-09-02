use anyhow::{Result, anyhow};
use compute_api::spec::{ComputeMode, PageserverProtocol};
use safekeeper_api::{PgMajorVersion, PgVersionId};
use serde_json::{Value, json};
use url::Host;
use utils::lsn::Lsn;

pub fn compute_mode_from_options(
    static_lsn: Option<Lsn>,
    hot_standby: bool,
) -> Result<ComputeMode> {
    match (static_lsn, hot_standby) {
        (Some(lsn), false) => Ok(ComputeMode::Static(lsn)),
        (None, true) => Ok(ComputeMode::Replica),
        (None, false) => Ok(ComputeMode::Primary),
        (Some(_), true) => anyhow::bail!("cannot specify both lsn and hot-standby"),
    }
}

pub fn build_pageserver_connstr(pageservers: &[(PageserverProtocol, Host, u16)]) -> String {
    pageservers
        .iter()
        .map(|(scheme, host, port)| format!("{scheme}://no_user@{host}:{port}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub struct EndpointRenderInput {
    pub base_config: Value,
    pub tenant_id: String,
    pub timeline_id: String,
    pub endpoint_id: String,
    pub pageserver_connstring: String,
    pub safekeeper_connstrings: Vec<String>,
    pub safekeepers_generation: u32,
    pub storage_auth_token: Option<String>,
    pub endpoint_storage_addr: Option<String>,
    pub endpoint_storage_token: Option<String>,
    pub autoprewarm: bool,
    pub offload_lfc_interval_seconds: Option<u64>,
    pub pg_version: PgMajorVersion,
    pub static_lsn: Option<String>,
    pub hot_standby: bool,
    pub enable_oggit: bool,
    pub oggit_database: String,
    pub oggit_ancestor_timeline_id: String,
    pub oggit_branch_start_lsn: String,
    pub oggit_safekeeper_http_urls: String,
}

pub fn render_endpoint_config(input: EndpointRenderInput) -> Result<Value> {
    let mut config = input.base_config;
    let spec = config
        .get_mut("spec")
        .ok_or_else(|| anyhow!("base compute config has no spec field"))?;

    spec["tenant_id"] = Value::String(input.tenant_id.clone());
    spec["timeline_id"] = Value::String(input.timeline_id.clone());
    spec["endpoint_id"] = Value::String(input.endpoint_id);
    spec["pageserver_connstring"] = Value::String(input.pageserver_connstring.clone());
    spec["safekeeper_connstrings"] = json!(input.safekeeper_connstrings);
    spec["safekeepers_generation"] = json!(input.safekeepers_generation);
    spec["storage_auth_token"] = json!(input.storage_auth_token);
    spec["endpoint_storage_addr"] = json!(input.endpoint_storage_addr);
    spec["endpoint_storage_token"] = json!(input.endpoint_storage_token);
    spec["autoprewarm"] = json!(input.autoprewarm);
    spec["pg_version"] = json!(PgVersionId::from(input.pg_version));
    if let Some(seconds) = input.offload_lfc_interval_seconds {
        spec["offload_lfc_interval_seconds"] = json!(seconds);
    }

    if let Some(lsn) = &input.static_lsn {
        spec["mode"] = json!({ "Static": lsn });
        upsert_setting(spec, "recovery_target_lsn", lsn, "string")?;
    } else if input.hot_standby {
        spec["mode"] = json!("Replica");
        upsert_setting(spec, "hot_standby", "on", "bool")?;
    } else {
        spec["mode"] = json!("Primary");
    }

    upsert_setting(spec, "neon.tenant_id", &input.tenant_id, "string")?;
    upsert_setting(spec, "neon.timeline_id", &input.timeline_id, "string")?;
    upsert_setting(
        spec,
        "neon.pageserver_connstring",
        &input.pageserver_connstring,
        "string",
    )?;
    upsert_setting(
        spec,
        "neon.safekeepers",
        &input.safekeeper_connstrings.join(","),
        "string",
    )?;
    upsert_setting(spec, "enable_subscription", "on", "bool")?;
    if input.enable_oggit {
        upsert_setting(spec, "neon.oggit_enabled", "on", "bool")?;
        if input.oggit_database != "postgres" {
            upsert_setting(spec, "neon.oggit_database", &input.oggit_database, "string")?;
        }
        upsert_setting(spec, "neon.oggit_tenant_id", &input.tenant_id, "string")?;
        upsert_setting(spec, "neon.oggit_timeline_id", &input.timeline_id, "string")?;
        upsert_setting(
            spec,
            "neon.oggit_ancestor_timeline_id",
            &input.oggit_ancestor_timeline_id,
            "string",
        )?;
        upsert_setting(
            spec,
            "neon.oggit_branch_start_lsn",
            &input.oggit_branch_start_lsn,
            "string",
        )?;
        upsert_setting(
            spec,
            "neon.oggit_safekeeper_http_urls",
            &input.oggit_safekeeper_http_urls,
            "string",
        )?;
    }

    Ok(config)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use compute_api::spec::PageserverProtocol;
    use url::Host;
    use utils::lsn::Lsn;

    use super::{build_pageserver_connstr, compute_mode_from_options};

    #[test]
    fn compute_mode_options_reject_static_hot_standby_mix() {
        let err =
            compute_mode_from_options(Some(Lsn::from_str("0/16B6C50").unwrap()), true).unwrap_err();

        assert!(err.to_string().contains("cannot specify both"));
    }

    #[test]
    fn pageserver_connstr_uses_common_format() {
        let connstr = build_pageserver_connstr(&[
            (
                PageserverProtocol::Libpq,
                Host::parse("pageserver").unwrap(),
                6400,
            ),
            (
                PageserverProtocol::Grpc,
                Host::parse("pageserver2").unwrap(),
                6401,
            ),
        ]);

        assert_eq!(
            connstr,
            "postgresql://no_user@pageserver:6400,grpc://no_user@pageserver2:6401"
        );
    }
}
