use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::time::{Duration, Instant, sleep};
use uuid::Uuid;

use crate::BranchMergeStrategy;

type OggitForeignKeyLookupKey = (String, String, String, String, String);

fn quote_sql_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn oggit_format_sql_ident(ident: &str) -> String {
    let mut chars = ident.chars();
    let Some(first) = chars.next() else {
        return quote_sql_ident(ident);
    };
    if !(first == '_' || first.is_ascii_lowercase()) {
        return quote_sql_ident(ident);
    }
    if !chars.all(|ch| ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()) {
        return quote_sql_ident(ident);
    }

    ident.to_string()
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn oggit_truncate_replay_sql(event: &JsonValue) -> Option<String> {
    let relations = event.get("relations")?.as_array()?;
    let table_refs = relations
        .iter()
        .filter_map(|relation| {
            let schema = relation.get("schema").and_then(JsonValue::as_str)?;
            let table = relation.get("table").and_then(JsonValue::as_str)?;
            (!oggit_is_internal_schema(schema)).then(|| {
                format!(
                    "{}.{}",
                    oggit_format_sql_ident(schema),
                    oggit_format_sql_ident(table)
                )
            })
        })
        .collect::<Vec<_>>();
    if table_refs.is_empty() {
        return None;
    }

    let mut sql = format!("TRUNCATE TABLE {}", table_refs.join(", "));
    if event
        .get("restart_seqs")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        sql.push_str(" RESTART IDENTITY");
    }
    if event
        .get("cascade")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        sql.push_str(" CASCADE");
    }
    Some(sql)
}

fn oggit_lsn_value(lsn: &str) -> u128 {
    let mut result = 0_u128;
    for part in lsn.split('/') {
        let value = u128::from_str_radix(part, 16).unwrap_or(0);
        result = result.saturating_mul(4_294_967_296).saturating_add(value);
    }
    result
}

fn oggit_lsn_from_value(value: u128) -> String {
    format!("{:X}/{:08X}", value >> 32, value & 0xffff_ffff)
}

#[derive(Clone, Debug)]
pub struct OggitGcParentRequest {
    pub tenant_id: String,
    pub timeline_id: String,
    pub floor_lsn: String,
    pub retention_lsn_distance: u128,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct OggitGcParentResult {
    pub tenant_id: String,
    pub timeline_id: String,
    pub status: String,
    pub floor_lsn: String,
    pub deleted_change_log: u64,
    pub deleted_object_change: u64,
    pub skipped_reason: Option<String>,
}

async fn oggit_ensure_gc_state(client: &tokio_opengauss::Client) -> Result<()> {
    if !oggit_table_exists(client, "oggit", "gc_state").await? {
        client
            .batch_execute(
                "CREATE TABLE oggit.gc_state (
                    tenant_id text NOT NULL,
                    timeline_id text NOT NULL,
                    gc_lsn text NOT NULL,
                    deleted_change_log bigint NOT NULL DEFAULT 0,
                    deleted_object_change bigint NOT NULL DEFAULT 0,
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (tenant_id, timeline_id)
                )",
            )
            .await
            .context("failed to create oggit.gc_state")?;
    }
    Ok(())
}

async fn oggit_pending_merge_reason(
    client: &tokio_opengauss::Client,
    floor_lsn: &str,
) -> Result<Option<String>> {
    if !oggit_table_exists(client, "oggit", "merge_history").await? {
        return Ok(None);
    }
    let floor_value = oggit_lsn_value(floor_lsn);
    let rows = client
        .query(
            "SELECT merge_id::text, status, parent_from_lsn, parent_to_lsn
               FROM oggit.merge_history
              WHERE status IN ('planning', 'blocked')
              ORDER BY created_at DESC NULLS LAST",
            &[],
        )
        .await
        .context("failed to inspect pending oggit merges before GC")?;
    for row in rows {
        let parent_from_lsn: String = row.get(2);
        if oggit_lsn_value(&parent_from_lsn) < floor_value {
            let merge_id: String = row.get(0);
            let status: String = row.get(1);
            let parent_to_lsn: String = row.get(3);
            return Ok(Some(format!(
                "pending merge {merge_id} is {status} and may reference parent interval {parent_from_lsn}..{parent_to_lsn}"
            )));
        }
    }
    Ok(None)
}

async fn oggit_delete_rows_before_lsn(
    client: &tokio_opengauss::Client,
    table_name: &str,
    floor_lsn: &str,
) -> Result<u64> {
    let select_sql = format!(
        "SELECT id, commit_lsn FROM oggit.{}",
        quote_sql_ident(table_name)
    );
    let floor_value = oggit_lsn_value(floor_lsn);
    let ids = client
        .query(select_sql.as_str(), &[])
        .await
        .with_context(|| format!("failed to scan oggit.{table_name} before {floor_lsn}"))?
        .into_iter()
        .filter_map(|row| {
            let id: i64 = row.get(0);
            let commit_lsn: String = row.get(1);
            (oggit_lsn_value(&commit_lsn) < floor_value).then_some(id)
        })
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(0);
    }
    let delete_sql = format!(
        "DELETE FROM oggit.{} WHERE id = ANY($1)",
        quote_sql_ident(table_name)
    );
    client
        .execute(delete_sql.as_str(), &[&ids])
        .await
        .with_context(|| format!("failed to delete oggit.{table_name} before {floor_lsn}"))
}

pub async fn oggit_gc_parent(
    client: &tokio_opengauss::Client,
    request: OggitGcParentRequest,
) -> Result<OggitGcParentResult> {
    let mut result = OggitGcParentResult {
        tenant_id: request.tenant_id.clone(),
        timeline_id: request.timeline_id.clone(),
        status: "skipped".to_string(),
        floor_lsn: request.floor_lsn.clone(),
        deleted_change_log: 0,
        deleted_object_change: 0,
        skipped_reason: None,
    };

    if !oggit_table_exists(client, "oggit", "state").await? {
        result.skipped_reason = Some("oggit.state does not exist".to_string());
        return Ok(result);
    }

    let state = oggit_read_state(client, "oggit").await?;
    if state.timeline_id != request.timeline_id {
        result.skipped_reason = Some(format!(
            "endpoint timeline {} does not match requested parent {}",
            state.timeline_id, request.timeline_id
        ));
        return Ok(result);
    }
    if state.ancestor_timeline_id.is_some() {
        result.skipped_reason = Some("timeline is not root parent".to_string());
        return Ok(result);
    }

    let state_floor = if oggit_lsn_value(&request.floor_lsn) == 0 {
        let confirmed = oggit_lsn_value(&state.scanned_lsn).max(oggit_lsn_value(&state.decode_lsn));
        let floor_value = confirmed.saturating_sub(request.retention_lsn_distance);
        oggit_lsn_from_value(floor_value)
    } else {
        request.floor_lsn.clone()
    };
    result.floor_lsn = state_floor.clone();

    if oggit_lsn_value(&state_floor) == 0 {
        result.skipped_reason = Some("computed GC floor is 0/0".to_string());
        return Ok(result);
    }

    if let Some(reason) = oggit_pending_merge_reason(client, &state_floor).await? {
        result.skipped_reason = Some(reason);
        return Ok(result);
    }

    oggit_ensure_gc_state(client).await?;
    client
        .batch_execute("BEGIN")
        .await
        .context("failed to begin oggit GC")?;
    let gc_result = async {
        let deleted_change_log =
            oggit_delete_rows_before_lsn(client, "change_log", &state_floor).await?;
        let deleted_object_change =
            oggit_delete_rows_before_lsn(client, "object_change", &state_floor).await?;
        let deleted_change_log_i64 = deleted_change_log as i64;
        let deleted_object_change_i64 = deleted_object_change as i64;
        let updated = client
            .execute(
                "UPDATE oggit.gc_state
                    SET gc_lsn = $3,
                        deleted_change_log = deleted_change_log + $4::bigint,
                        deleted_object_change = deleted_object_change + $5::bigint,
                        updated_at = now()
                  WHERE tenant_id = $1 AND timeline_id = $2",
                &[
                    &request.tenant_id,
                    &request.timeline_id,
                    &state_floor,
                    &deleted_change_log_i64,
                    &deleted_object_change_i64,
                ],
            )
            .await
            .context("failed to update oggit.gc_state")?;
        if updated == 0 {
            client
                .execute(
                    "INSERT INTO oggit.gc_state (
                        tenant_id, timeline_id, gc_lsn, deleted_change_log, deleted_object_change
                     ) VALUES ($1, $2, $3, $4::bigint, $5::bigint)",
                    &[
                        &request.tenant_id,
                        &request.timeline_id,
                        &state_floor,
                        &deleted_change_log_i64,
                        &deleted_object_change_i64,
                    ],
                )
                .await
                .context("failed to insert oggit.gc_state")?;
        }
        Ok::<_, anyhow::Error>((deleted_change_log, deleted_object_change))
    }
    .await;

    match gc_result {
        Ok((deleted_change_log, deleted_object_change)) => {
            client
                .batch_execute("COMMIT")
                .await
                .context("failed to commit oggit GC")?;
            result.status = "deleted".to_string();
            result.deleted_change_log = deleted_change_log;
            result.deleted_object_change = deleted_object_change;
            Ok(result)
        }
        Err(err) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(err)
        }
    }
}

pub(crate) fn oggit_json_text(value: &Option<JsonValue>) -> String {
    value
        .as_ref()
        .map(|value| oggit_canonical_json(value).to_string())
        .unwrap_or_default()
}

fn oggit_canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut canonical = JsonMap::new();
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if let Some(value) = map.get(key) {
                    canonical.insert(key.clone(), oggit_canonical_json(value));
                }
            }
            JsonValue::Object(canonical)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(oggit_canonical_json).collect())
        }
        _ => value.clone(),
    }
}

fn oggit_parse_json_text(value: Option<String>) -> Result<Option<JsonValue>> {
    value
        .filter(|text| !text.is_empty())
        .map(|text| {
            serde_json::from_str(&text).with_context(|| format!("invalid oggit json {text}"))
        })
        .transpose()
}

fn oggit_empty_object() -> JsonValue {
    JsonValue::Object(JsonMap::new())
}

fn oggit_json_object_or_empty(value: Option<JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Object(_)) => value.unwrap(),
        _ => oggit_empty_object(),
    }
}

fn oggit_json_merge(left: Option<&JsonValue>, right: Option<&JsonValue>) -> JsonValue {
    let mut merged = JsonMap::new();
    for value in [left, right].into_iter().flatten() {
        if let JsonValue::Object(map) = value {
            for (key, value) in map {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    JsonValue::Object(merged)
}

fn oggit_json_sql_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::String(text) => quote_sql_literal(text),
        other => quote_sql_literal(&other.to_string()),
    }
}

fn oggit_json_where_expr(row_alias: &str, payload: &Option<JsonValue>) -> String {
    let Some(JsonValue::Object(map)) = payload else {
        return "TRUE".to_string();
    };

    if map.is_empty() {
        return "TRUE".to_string();
    }

    map.iter()
        .map(|(key, value)| {
            format!(
                "{}.{} IS NOT DISTINCT FROM {}",
                row_alias,
                quote_sql_ident(key),
                oggit_json_sql_value(value)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn oggit_json_defensive_where_expr(
    row_alias: &str,
    key_payload: &Option<JsonValue>,
    expected_payload: &Option<JsonValue>,
) -> String {
    let expected = expected_payload
        .as_ref()
        .map(|expected| oggit_json_merge(key_payload.as_ref(), Some(expected)));
    let key_expr = oggit_json_where_expr(row_alias, key_payload);
    let expected_expr = oggit_json_where_expr(row_alias, &expected);

    match (key_expr.as_str(), expected_expr.as_str()) {
        ("TRUE", "TRUE") => "TRUE".to_string(),
        ("TRUE", _) => expected_expr,
        (_, "TRUE") => key_expr,
        _ => format!("({key_expr}) AND ({expected_expr})"),
    }
}

fn oggit_tuple_payload_to_json(payload: Option<&JsonValue>) -> JsonValue {
    let mut result = JsonMap::new();
    if let Some(JsonValue::Object(fields)) = payload {
        for (key, value) in fields {
            if let Some(field_value) = value.get("value") {
                result.insert(key.clone(), field_value.clone());
            }
        }
    }
    JsonValue::Object(result)
}

fn oggit_tuple_payload_nonkey_columns(payload: Option<&JsonValue>) -> Vec<String> {
    let mut cols = Vec::new();
    if let Some(JsonValue::Object(fields)) = payload {
        for (key, value) in fields {
            if !value
                .get("is_key")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
            {
                cols.push(key.clone());
            }
        }
    }
    cols.sort();
    cols
}

fn oggit_json_array_to_text_vec(payload: Option<&JsonValue>) -> Vec<String> {
    payload
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn oggit_event_lsn(event: &JsonValue) -> String {
    [
        "commit_lsn",
        "change_lsn",
        "message_lsn",
        "callback_commit_lsn",
    ]
    .into_iter()
    .find_map(|key| event.get(key).and_then(JsonValue::as_str))
    .unwrap_or("0/0")
    .to_string()
}

fn oggit_array_union(left_cols: &[String], right_cols: &[String]) -> Vec<String> {
    left_cols
        .iter()
        .chain(right_cols)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn oggit_merge_conflict_scope(object_type: &str) -> &'static str {
    match object_type {
        "SEQUENCE" => "sequence",
        "TRUNCATE" => "truncate",
        "OTHER" => "unsupported",
        _ => "ddl",
    }
}

fn oggit_is_internal_schema(schema_name: &str) -> bool {
    schema_name == "oggit"
        || schema_name == "_oggit"
        || schema_name == "neon"
        || schema_name == "coverage"
        || schema_name == "oggit_fdw"
        || schema_name.starts_with("neon_")
}

fn oggit_sql_schema_name(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == '(');
    let mut words = trimmed.split_whitespace();
    let first = words.next()?;
    let second = words.next()?;
    if !first.eq_ignore_ascii_case("CREATE") && !first.eq_ignore_ascii_case("DROP") {
        return None;
    }
    if !second.eq_ignore_ascii_case("SCHEMA") {
        return None;
    }

    let mut name = words.next()?;
    if name.eq_ignore_ascii_case("IF") {
        if !matches!(
            (words.next(), words.next()),
            (Some(not), Some(exists))
                if not.eq_ignore_ascii_case("NOT") && exists.eq_ignore_ascii_case("EXISTS")
        ) {
            return None;
        }
        name = words.next()?;
    }

    Some(
        name.trim_matches(|c| c == '"' || c == '\'' || c == '`' || c == ';')
            .split('.')
            .next()
            .unwrap_or_default(),
    )
}

fn oggit_json_str_mentions_internal_schema(s: &str) -> bool {
    if oggit_is_internal_schema(s.trim_matches('"')) {
        return true;
    }
    if oggit_sql_schema_name(s).is_some_and(oggit_is_internal_schema) {
        return true;
    }
    if let Some(schema) = s.split('.').next() {
        if oggit_is_internal_schema(schema.trim_matches('"')) {
            return true;
        }
    }
    false
}

fn oggit_json_mentions_internal_schema(value: &JsonValue) -> bool {
    if value
        .pointer("/identity/schemaname")
        .and_then(JsonValue::as_str)
        .is_some_and(oggit_is_internal_schema)
    {
        return true;
    }
    if value
        .get("schema")
        .and_then(JsonValue::as_str)
        .is_some_and(oggit_json_str_mentions_internal_schema)
    {
        return true;
    }
    if value
        .get("objidentity")
        .and_then(JsonValue::as_str)
        .is_some_and(|objidentity| {
            objidentity
                .split('.')
                .next()
                .is_some_and(|schema| objidentity.contains('.') && oggit_is_internal_schema(schema))
        })
    {
        return true;
    }
    let describes_schema = value
        .get("fmt")
        .and_then(JsonValue::as_str)
        .is_some_and(|fmt| fmt.to_ascii_uppercase().contains("SCHEMA"))
        || value
            .get("objtype")
            .and_then(JsonValue::as_str)
            .is_some_and(|objtype| objtype.eq_ignore_ascii_case("schema"));
    if describes_schema
        && value
            .get("name")
            .and_then(JsonValue::as_str)
            .is_some_and(oggit_is_internal_schema)
    {
        return true;
    }
    if let Some(object) = value.as_object() {
        for (key, child) in object {
            if matches!(key.as_str(), "schemaname" | "schema")
                && child.as_str().is_some_and(oggit_is_internal_schema)
            {
                return true;
            }
            if oggit_json_mentions_internal_schema(child) {
                return true;
            }
        }
    }
    if let Some(array) = value.as_array() {
        for child in array {
            if oggit_json_mentions_internal_schema(child) {
                return true;
            }
        }
    }
    if value.as_str().is_some_and(|s| {
        oggit_sql_schema_name(s).is_some_and(oggit_is_internal_schema)
            || serde_json::from_str::<JsonValue>(s)
                .ok()
                .is_some_and(|nested| oggit_json_mentions_internal_schema(&nested))
    }) {
        return true;
    }

    false
}

fn oggit_object_change_is_internal(change: &OggitObjectChange) -> bool {
    if change
        .schema_name
        .as_deref()
        .is_some_and(oggit_is_internal_schema)
    {
        return true;
    }
    if change.schema_name.is_none()
        && change
            .object_name
            .as_deref()
            .is_some_and(oggit_is_internal_schema)
    {
        return true;
    }
    if oggit_json_mentions_internal_schema(&change.change_json) {
        return true;
    }
    if change
        .change_json
        .get("message")
        .and_then(JsonValue::as_str)
        .and_then(|message| serde_json::from_str::<JsonValue>(message).ok())
        .is_some_and(|message| oggit_json_mentions_internal_schema(&message))
    {
        return true;
    }
    if change
        .change_json
        .get("sql")
        .and_then(JsonValue::as_str)
        .and_then(oggit_sql_schema_name)
        .is_some_and(oggit_is_internal_schema)
    {
        return true;
    }
    false
}

fn oggit_json_mentions_merge_barrier(value: &JsonValue) -> bool {
    if value.as_str().is_some_and(|s| {
        s.contains("oggit_merge_write_barrier")
            || s.contains("enforce_merge_write_barrier")
            || s.contains("merge_write_barrier")
    }) {
        return true;
    }
    if let Some(object) = value.as_object() {
        for child in object.values() {
            if oggit_json_mentions_merge_barrier(child) {
                return true;
            }
        }
    }
    if let Some(array) = value.as_array() {
        for child in array {
            if oggit_json_mentions_merge_barrier(child) {
                return true;
            }
        }
    }
    false
}

fn oggit_object_change_is_merge_barrier_ddl(change: &OggitObjectChange) -> bool {
    change
        .change_json
        .get("sql")
        .and_then(JsonValue::as_str)
        .is_some_and(|sql| sql.contains("oggit_merge_write_barrier"))
        || change
            .change_json
            .get("message")
            .and_then(JsonValue::as_str)
            .is_some_and(|message| message.contains("oggit_merge_write_barrier"))
        || oggit_json_mentions_merge_barrier(&change.change_json)
}

fn oggit_object_change_diff_key(change: &OggitObjectChange) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        change.object_type,
        change.schema_name.as_deref().unwrap_or_default(),
        change.object_name.as_deref().unwrap_or_default(),
        change
            .change_json
            .get("sql")
            .and_then(JsonValue::as_str)
            .unwrap_or_default(),
        change.safety_class
    )
}

fn oggit_object_change_merge_key(change: &OggitObjectChange) -> String {
    format!(
        "{}:{}:{}",
        change.object_type,
        change.schema_name.as_deref().unwrap_or_default(),
        change.object_name.as_deref().unwrap_or_default()
    )
}

fn oggit_rule_target_from_text(text: &str) -> Option<(Option<String>, String)> {
    let upper_text = text.to_uppercase();
    let (marker_pos, marker_len) = upper_text
        .find(" TO ")
        .map(|pos| (pos, 4))
        .or_else(|| upper_text.find(" ON ").map(|pos| (pos, 4)))?;
    let table_start = marker_pos + marker_len;
    let table_ref = text[table_start..]
        .trim_start()
        .split(|ch: char| ch.is_whitespace() || matches!(ch, ';' | ',' | ')' | '(' | '"'))
        .next()
        .filter(|name| !name.is_empty())?;
    let (schema, name) = table_ref
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.to_string()), name.to_string()))
        .unwrap_or_else(|| (None, table_ref.to_string()));
    Some((schema, name))
}

fn oggit_alter_table_target_from_text(text: &str) -> Option<(Option<String>, String)> {
    let upper_text = text.to_uppercase();
    let marker_pos = upper_text.find("ALTER TABLE ")?;
    let mut table_ref = text[marker_pos + "ALTER TABLE ".len()..].trim_start();
    if table_ref.to_uppercase().starts_with("IF EXISTS ") {
        table_ref = table_ref["IF EXISTS ".len()..].trim_start();
    }
    if table_ref.to_uppercase().starts_with("ONLY ") {
        table_ref = table_ref["ONLY ".len()..].trim_start();
    }
    let table_ref = table_ref
        .split(|ch: char| ch.is_whitespace() || matches!(ch, ';' | ',' | ')' | '(' | '"'))
        .next()
        .filter(|name| !name.is_empty())?;
    let (schema, name) = table_ref
        .rsplit_once('.')
        .map(|(schema, name)| (Some(schema.to_string()), name.to_string()))
        .unwrap_or_else(|| (None, table_ref.to_string()));
    Some((schema, name))
}

fn oggit_object_change_sql_text(change: &OggitObjectChange) -> Option<&str> {
    change
        .change_json
        .get("sql")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            change
                .change_json
                .get("message")
                .and_then(JsonValue::as_str)
        })
}

fn oggit_table_ref_matches(
    table_ref: Option<(Option<String>, String)>,
    schema_name: &str,
    table_name: &str,
) -> bool {
    let Some((schema, name)) = table_ref else {
        return false;
    };
    schema.as_deref().is_none_or(|schema| schema == schema_name) && name == table_name
}

fn oggit_object_change_affects_table(
    change: &OggitObjectChange,
    schema_name: &str,
    table_name: &str,
) -> bool {
    if change
        .schema_name
        .as_deref()
        .is_some_and(|schema| schema != schema_name)
    {
        return false;
    }
    let affects_table_object = change.object_name.as_deref() == Some(table_name)
        && change
            .schema_name
            .as_deref()
            .is_none_or(|schema| schema == schema_name)
        && matches!(
            change.object_type.as_str(),
            "TABLE" | "COLUMN" | "INDEX" | "CONSTRAINT" | "TRUNCATE"
        );
    if affects_table_object {
        return true;
    }
    if !matches!(
        change.object_type.as_str(),
        "TABLE" | "COLUMN" | "INDEX" | "CONSTRAINT" | "TRUNCATE"
    ) {
        return false;
    }

    let Some(sql) = oggit_object_change_sql_text(change) else {
        return false;
    };
    let table_ref = if change.object_type == "INDEX" {
        oggit_rule_target_from_text(sql)
    } else {
        oggit_alter_table_target_from_text(sql)
    };
    oggit_table_ref_matches(table_ref, schema_name, table_name)
}

fn oggit_target_object_change_is_dml_compatible(object: &OggitObjectChange) -> bool {
    object.safety_class == "safe_additive"
}

fn oggit_event_order(commit_lsn: &str, ordinal: i32, id: i64) -> (u128, i32, i64) {
    (oggit_lsn_value(commit_lsn), ordinal, id)
}

#[derive(Clone)]
struct OggitState {
    timeline_id: String,
    ancestor_timeline_id: Option<String>,
    branch_start_lsn: String,
    decode_lsn: String,
    scanned_lsn: String,
    status: String,
    last_error: Option<String>,
}

#[derive(Clone)]
struct OggitChange {
    id: i64,
    commit_lsn: String,
    ordinal: i32,
    op: String,
    schema_name: String,
    table_name: String,
    identity_kind: String,
    key_json: Option<JsonValue>,
    old_row: Option<JsonValue>,
    new_row: Option<JsonValue>,
    changed_cols: Vec<String>,
    unsupported_reason: Option<String>,
}

#[derive(Clone)]
struct OggitObjectChange {
    id: i64,
    commit_lsn: String,
    ordinal: i32,
    object_type: String,
    schema_name: Option<String>,
    object_name: Option<String>,
    change_json: JsonValue,
    safety_class: String,
    unsupported_reason: Option<String>,
}

#[derive(Clone)]
struct OggitDelta {
    schema_name: String,
    table_name: String,
    identity_kind: String,
    key_json: Option<JsonValue>,
    final_op: String,
    old_row: Option<JsonValue>,
    new_row: Option<JsonValue>,
    changed_cols: Vec<String>,
    unsupported_reason: Option<String>,
    first_commit_lsn: String,
    first_ordinal: i32,
    first_id: i64,
    segment_id: i64,
}

#[derive(Clone)]
enum OggitPlanEvent {
    Object {
        segment_id: i64,
        change: OggitObjectChange,
    },
    Dml(OggitDelta),
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct OggitOrderKey {
    lsn: u128,
    ordinal: i32,
    id: i64,
}

#[derive(Clone)]
enum OggitContinuePlanEvent {
    Object {
        order: OggitOrderKey,
        change: OggitObjectChange,
        conflict: Option<OggitConflict>,
    },
    Dml {
        order: OggitOrderKey,
        delta: OggitDelta,
        conflict: Option<OggitConflict>,
    },
}

impl OggitPlanEvent {
    fn order(&self) -> (i64, u128, i32, i64) {
        match self {
            Self::Object { segment_id, change } => {
                let (lsn, ordinal, id) =
                    oggit_event_order(&change.commit_lsn, change.ordinal, change.id);
                (*segment_id, lsn, ordinal, id)
            }
            Self::Dml(delta) => {
                let (lsn, ordinal, id) =
                    oggit_event_order(&delta.first_commit_lsn, delta.first_ordinal, delta.first_id);
                (delta.segment_id, lsn, ordinal, id)
            }
        }
    }
}

impl OggitContinuePlanEvent {
    fn order(&self) -> OggitOrderKey {
        match self {
            Self::Object { order, .. } | Self::Dml { order, .. } => *order,
        }
    }
}

#[derive(Clone)]
pub(crate) struct OggitDiffRow {
    pub(crate) diff_scope: String,
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) key_json: Option<JsonValue>,
    pub(crate) diff_type: String,
    pub(crate) ours_json: Option<JsonValue>,
    pub(crate) theirs_json: Option<JsonValue>,
    pub(crate) detail: Option<String>,
}

pub(crate) struct OggitDiffResult {
    pub(crate) base_lsn: String,
    pub(crate) child_to_lsn: String,
    pub(crate) parent_to_lsn: String,
    pub(crate) rows: Vec<OggitDiffRow>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OggitMergeDirection {
    ChildToParent,
    ParentToChild,
}

impl OggitMergeDirection {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ChildToParent => "child_to_parent",
            Self::ParentToChild => "parent_to_child",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "child_to_parent" => Ok(Self::ChildToParent),
            "parent_to_child" => Ok(Self::ParentToChild),
            other => bail!("unsupported oggit merge direction {other}"),
        }
    }
}

struct OggitMergeBounds {
    child_state: OggitState,
    parent_state: OggitState,
    direction: OggitMergeDirection,
    base_lsn: String,
    child_from_lsn: String,
    child_to_lsn: String,
    parent_from_lsn: String,
    parent_to_lsn: String,
}

impl OggitMergeBounds {
    fn source_state(&self) -> &OggitState {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.child_state,
            OggitMergeDirection::ParentToChild => &self.parent_state,
        }
    }

    fn target_state(&self) -> &OggitState {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.parent_state,
            OggitMergeDirection::ParentToChild => &self.child_state,
        }
    }

    fn source_from_lsn(&self) -> &str {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.child_from_lsn,
            OggitMergeDirection::ParentToChild => &self.parent_from_lsn,
        }
    }

    fn source_to_lsn(&self) -> &str {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.child_to_lsn,
            OggitMergeDirection::ParentToChild => &self.parent_to_lsn,
        }
    }

    fn target_from_lsn(&self) -> &str {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.parent_from_lsn,
            OggitMergeDirection::ParentToChild => &self.child_from_lsn,
        }
    }

    fn target_to_lsn(&self) -> &str {
        match self.direction {
            OggitMergeDirection::ChildToParent => &self.parent_to_lsn,
            OggitMergeDirection::ParentToChild => &self.child_to_lsn,
        }
    }
}

struct OggitAppliedMergeBounds {
    child_from_lsn: String,
    parent_from_lsn: String,
}

#[derive(Clone)]
pub(crate) struct OggitMergeResult {
    pub(crate) merge_id: String,
    pub(crate) status: String,
    pub(crate) conflict_count: i64,
    pub(crate) applied_count: i64,
    pub(crate) skipped_details: Vec<String>,
}

struct OggitApplyResult {
    status: String,
    generated_key: Option<JsonValue>,
}

impl OggitApplyResult {
    fn new(status: &str) -> Self {
        Self {
            status: status.to_string(),
            generated_key: None,
        }
    }

    fn with_generated_key(status: &str, generated_key: JsonValue) -> Self {
        Self {
            status: status.to_string(),
            generated_key: Some(generated_key),
        }
    }
}

pub(crate) struct OggitMergeHistory {
    pub(crate) merge_id: String,
    pub(crate) child_timeline_id: String,
    pub(crate) parent_timeline_id: String,
    pub(crate) base_lsn: String,
    child_from_lsn: String,
    pub(crate) child_to_lsn: String,
    parent_from_lsn: String,
    pub(crate) parent_to_lsn: String,
    pub(crate) strategy: String,
    pub(crate) status: String,
    pub(crate) conflict_count: i64,
    child_meta_schema: String,
    pub(crate) merge_direction: OggitMergeDirection,
}

impl OggitMergeHistory {
    fn source_from_lsn(&self) -> &str {
        match self.merge_direction {
            OggitMergeDirection::ChildToParent => &self.child_from_lsn,
            OggitMergeDirection::ParentToChild => &self.parent_from_lsn,
        }
    }

    fn source_to_lsn(&self) -> &str {
        match self.merge_direction {
            OggitMergeDirection::ChildToParent => &self.child_to_lsn,
            OggitMergeDirection::ParentToChild => &self.parent_to_lsn,
        }
    }

    fn target_from_lsn(&self) -> &str {
        match self.merge_direction {
            OggitMergeDirection::ChildToParent => &self.parent_from_lsn,
            OggitMergeDirection::ParentToChild => &self.child_from_lsn,
        }
    }

    fn target_to_lsn(&self) -> &str {
        match self.merge_direction {
            OggitMergeDirection::ChildToParent => &self.parent_to_lsn,
            OggitMergeDirection::ParentToChild => &self.child_to_lsn,
        }
    }
}

#[derive(Clone)]
pub(crate) struct OggitConflict {
    pub(crate) conflict_id: i64,
    pub(crate) conflict_scope: String,
    pub(crate) conflict_type: String,
    pub(crate) schema_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) object_name: Option<String>,
    theirs_op: Option<String>,
    pub(crate) key_json: Option<JsonValue>,
    pub(crate) ours_json: Option<JsonValue>,
    pub(crate) theirs_json: Option<JsonValue>,
    pub(crate) reason: String,
    pub(crate) resolution: Option<String>,
    custom_sql: Option<String>,
    pub(crate) status: String,
}

async fn oggit_meta_table_name(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
    base_name: &str,
) -> Result<String> {
    let rows = client
        .query(
            "SELECT c.relname
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = $1
                AND c.relname IN ($2, $3)
                AND c.relkind IN ('r', 'f', 'v')
              ORDER BY CASE WHEN c.relname = $2 THEN 0 ELSE 1 END
              LIMIT 1",
            &[&meta_schema, &base_name, &format!("oggit_{base_name}")],
        )
        .await
        .with_context(|| {
            format!("failed to find oggit metadata table {meta_schema}.{base_name}")
        })?;

    rows.first()
        .map(|row| row.get::<_, String>(0))
        .with_context(|| format!("oggit metadata table {meta_schema}.{base_name} not found"))
}

// Legacy in-Rust event persistence. Superseded by the compute-side oggit
// bgworker (pgxn/neon/oggit_worker.c), which decodes and persists events inside
// compute. Kept only for reference; not referenced by the CLI anymore.
#[allow(dead_code)]
async fn oggit_record_event(
    client: &tokio_opengauss::Client,
    event: &JsonValue,
    ordinal: i32,
) -> Result<()> {
    let event_kind = event.get("event").and_then(JsonValue::as_str);
    let commit_lsn = oggit_event_lsn(event);

    match event_kind {
        Some("change") => {
            if event
                .get("schema")
                .and_then(JsonValue::as_str)
                .is_some_and(oggit_is_internal_schema)
            {
                return Ok(());
            }

            let key_payload = oggit_tuple_payload_to_json(event.get("key"));
            let identity = if key_payload.as_object().is_some_and(|map| !map.is_empty()) {
                "primary_key"
            } else if event.get("old_row").is_some() || event.get("new_row").is_some() {
                "replica_identity_full"
            } else {
                "unsupported"
            };
            let key_json = if key_payload.as_object().is_some_and(|map| map.is_empty()) {
                None
            } else {
                Some(key_payload)
            };
            let old_row = oggit_tuple_payload_to_json(event.get("old_row"));
            let old_row = if old_row.as_object().is_some_and(|map| map.is_empty()) {
                None
            } else {
                Some(old_row)
            };
            let new_row = oggit_tuple_payload_to_json(event.get("new_row"));
            let new_row = if new_row.as_object().is_some_and(|map| map.is_empty()) {
                None
            } else {
                Some(new_row)
            };
            let changed_cols = if event.get("op").and_then(JsonValue::as_str) == Some("INSERT") {
                oggit_tuple_payload_nonkey_columns(event.get("new_row"))
            } else {
                oggit_json_array_to_text_vec(event.get("changed_cols"))
            };
            let unsupported_reason = (identity == "unsupported")
                .then(|| "no usable row identity in decoded event".to_string());
            let relid = event
                .get("relid")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_string();
            let key_json_text = key_json.as_ref().map(JsonValue::to_string);
            let old_row_text = old_row.as_ref().map(JsonValue::to_string);
            let new_row_text = new_row.as_ref().map(JsonValue::to_string);

            client
                .execute(
                    "INSERT INTO oggit.change_log (
                        commit_lsn, record_lsn, xid, ordinal, op, schema_name, table_name,
                        relid, identity_kind, key_json, old_row, new_row, changed_cols,
                        unsupported_reason
                     )
                     VALUES (
                        $1, $2, $3, $4, $5, $6, $7, NULLIF($8, '')::oid, $9,
                        $10::text::jsonb, $11::text::jsonb, $12::text::jsonb, $13, $14
                     )",
                    &[
                        &commit_lsn,
                        &event
                            .get("change_lsn")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &event
                            .get("xid")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &ordinal,
                        &event
                            .get("op")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("UNSUPPORTED"),
                        &event
                            .get("schema")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &event
                            .get("table")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &relid,
                        &identity,
                        &key_json_text,
                        &old_row_text,
                        &new_row_text,
                        &changed_cols,
                        &unsupported_reason,
                    ],
                )
                .await
                .context("failed to insert oggit.change_log")?;
        }
        Some("truncate") => {
            let relation = event
                .get("relations")
                .and_then(JsonValue::as_array)
                .and_then(|relations| {
                    relations.iter().find(|relation| {
                        relation
                            .get("schema")
                            .and_then(JsonValue::as_str)
                            .is_some_and(|schema| !oggit_is_internal_schema(schema))
                    })
                });
            let Some(relation) = relation else {
                return Ok(());
            };

            let mut change_json = event.clone();
            if let Some(sql) = oggit_truncate_replay_sql(&change_json) {
                if let Some(map) = change_json.as_object_mut() {
                    map.insert("sql".to_string(), JsonValue::String(sql));
                }
            }
            let change_json_text = change_json.to_string();
            client
                .execute(
                    "INSERT INTO oggit.object_change (
                        commit_lsn, ordinal, object_type, schema_name, object_name,
                        action, change_json, safety_class
                     )
                     VALUES ($1, $2, 'TRUNCATE', $3, $4, 'TRUNCATE', $5::text::jsonb, 'destructive')",
                    &[
                        &commit_lsn,
                        &ordinal,
                        &relation
                            .get("schema")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &relation
                            .get("table")
                            .and_then(JsonValue::as_str)
                            .map(str::to_string),
                        &change_json_text,
                    ],
                )
                .await
                .context("failed to insert oggit truncate object_change")?;
        }
        Some("ddl") => {
            let ddl_payload = event
                .get("message")
                .and_then(JsonValue::as_str)
                .and_then(|message| serde_json::from_str::<JsonValue>(message).ok())
                .unwrap_or_else(oggit_empty_object);
            let raw_type = ddl_payload
                .get("objtype")
                .and_then(JsonValue::as_str)
                .unwrap_or("OTHER")
                .to_uppercase();
            let message = event
                .get("message")
                .and_then(JsonValue::as_str)
                .unwrap_or("");
            let sql_text = event.get("sql").and_then(JsonValue::as_str).unwrap_or("");
            let ddl_text = if sql_text.is_empty() {
                message.to_string()
            } else {
                format!("{message} {sql_text}")
            };
            let upper_message = ddl_text.to_uppercase();
            let rename_column = upper_message.contains("RENAME COLUMN");
            let rename_table =
                upper_message.contains("ALTER TABLE") && upper_message.contains(" RENAME TO ");
            let add_column = upper_message.contains("ADD COLUMN");
            let create_index = upper_message.contains("INDEX") && upper_message.contains(" ON ");
            let object_type = if rename_column || add_column {
                "COLUMN"
            } else if create_index {
                "INDEX"
            } else {
                match raw_type.as_str() {
                    "TABLE" => "TABLE",
                    "COLUMN" => "COLUMN",
                    "INDEX" => "INDEX",
                    "CONSTRAINT" | "TABLE CONSTRAINT" => "CONSTRAINT",
                    "SEQUENCE" | "LARGE SEQUENCE" => "SEQUENCE",
                    "TRIGGER" => "TRIGGER",
                    "FUNCTION" => "FUNCTION",
                    "VIEW" => "VIEW",
                    "RULE" => "RULE",
                    _ if upper_message.contains("SEQUENCE") => "SEQUENCE",
                    _ if upper_message.contains("RULE") => "RULE",
                    _ if upper_message.contains("ADD CONSTRAINT")
                        || upper_message.contains("CHECK")
                        || upper_message.contains("FOREIGN KEY") =>
                    {
                        "CONSTRAINT"
                    }
                    _ if upper_message.contains("TABLE") => "TABLE",
                    _ => "OTHER",
                }
            };
            let cmdtype = event
                .get("cmdtype")
                .and_then(JsonValue::as_str)
                .unwrap_or("DDL");
            let safety_class = if cmdtype.to_lowercase().contains("drop")
                || upper_message.contains("DROP ")
            {
                "destructive"
            } else if (matches!(cmdtype, "table_alter") || upper_message.contains("ALTER TABLE"))
                && (upper_message.contains("ADD CONSTRAINT")
                    || upper_message.contains("CHECK")
                    || upper_message.contains("FOREIGN KEY")
                    || upper_message.contains("SET NOT NULL"))
            {
                "requires_validation"
            } else if rename_column
                || rename_table
                || ((matches!(cmdtype, "table_alter") || upper_message.contains("ALTER TABLE"))
                    && upper_message.contains("ALTER COLUMN")
                    && (upper_message.contains(" TYPE ")
                        || upper_message.contains("SET DATA TYPE")))
            {
                "semantic"
            } else if matches!(object_type, "COLUMN" | "INDEX")
                && (matches!(cmdtype, "table_alter" | "object_create")
                    || add_column
                    || create_index)
                && !upper_message.contains("NOT NULL")
                && !upper_message.contains("UNIQUE")
                && !upper_message.contains("CHECK")
                && !upper_message.contains("FOREIGN KEY")
            {
                "safe_additive"
            } else if matches!(object_type, "SEQUENCE" | "TRIGGER" | "FUNCTION" | "VIEW") {
                "semantic"
            } else if object_type == "CONSTRAINT" {
                "requires_validation"
            } else {
                "unsupported"
            };
            let unsupported_reason = (safety_class == "unsupported")
                .then(|| "DDL is not supported by object-level merge".to_string());
            let object_schema = ddl_payload
                .pointer("/identity/schemaname")
                .and_then(JsonValue::as_str)
                .or_else(|| {
                    ddl_payload
                        .pointer("/table/schemaname")
                        .and_then(JsonValue::as_str)
                })
                .map(str::to_string);
            let object_name = if object_type == "COLUMN" {
                ddl_payload.get("colname").and_then(JsonValue::as_str)
            } else {
                None
            }
            .or_else(|| {
                ddl_payload
                    .pointer("/identity/objname")
                    .and_then(JsonValue::as_str)
            })
            .or_else(|| ddl_payload.get("name").and_then(JsonValue::as_str))
            .or_else(|| ddl_payload.get("objidentity").and_then(JsonValue::as_str))
            .map(str::to_string);
            let objidentity = ddl_payload
                .get("objidentity")
                .and_then(JsonValue::as_str)
                .unwrap_or("");
            let (object_schema, object_name) = if object_type == "RULE" {
                oggit_rule_target_from_text(message)
                    .or_else(|| oggit_rule_target_from_text(objidentity))
                    .unwrap_or((object_schema, object_name.unwrap_or_default()))
            } else {
                (object_schema, object_name.unwrap_or_default())
            };
            let object_name = Some(object_name);
            if object_schema
                .as_deref()
                .is_some_and(oggit_is_internal_schema)
                || (object_schema.is_none()
                    && object_name.as_deref().is_some_and(oggit_is_internal_schema))
            {
                return Ok(());
            }

            let change_json_text = event.to_string();

            client
                .execute(
                    "INSERT INTO oggit.object_change (
                        commit_lsn, ordinal, object_type, schema_name, object_name,
                        action, change_json, safety_class, unsupported_reason
                     )
                     VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb, $8, $9)",
                    &[
                        &commit_lsn,
                        &ordinal,
                        &object_type,
                        &object_schema,
                        &object_name,
                        &cmdtype,
                        &change_json_text,
                        &safety_class,
                        &unsupported_reason,
                    ],
                )
                .await
                .context("failed to insert oggit ddl object_change")?;
        }
        Some("commit") => {
            let confirmed_lsn = event
                .get("callback_commit_lsn")
                .and_then(JsonValue::as_str)
                .unwrap_or(&commit_lsn)
                .to_string();
            client
                .execute(
                    "UPDATE oggit.state
                        SET decode_lsn = $1,
                            confirmed_lsn = $2,
                            updated_at = now()
                      WHERE id",
                    &[&commit_lsn, &confirmed_lsn],
                )
                .await
                .context("failed to update oggit.state commit progress")?;
        }
        _ => {
            let change_json_text = event.to_string();
            let action = event_kind.unwrap_or("unknown");
            client
                .execute(
                    "INSERT INTO oggit.object_change (
                        commit_lsn, ordinal, object_type, action, change_json,
                        safety_class, unsupported_reason
                     )
                     VALUES ($1, $2, 'OTHER', $3, $4::text::jsonb, 'unsupported', 'unknown neon_oggit event')",
                    &[&commit_lsn, &ordinal, &action, &change_json_text],
                )
                .await
                .context("failed to insert oggit unsupported object_change")?;
        }
    }

    Ok(())
}

async fn oggit_read_state(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
) -> Result<OggitState> {
    let relname = oggit_meta_table_name(client, meta_schema, "state").await?;
    let sql = format!(
        "SELECT tenant_id, timeline_id, ancestor_timeline_id, branch_start_lsn,
                required_lsn, decode_lsn, scanned_lsn, confirmed_lsn, status, last_error
           FROM {}.{}
          WHERE id",
        quote_sql_ident(meta_schema),
        quote_sql_ident(&relname)
    );
    let row = client
        .query_opt(sql.as_str(), &[])
        .await
        .with_context(|| format!("failed to read oggit state from {meta_schema}.{relname}"))?
        .with_context(|| format!("oggit state not found in schema {meta_schema}"))?;

    Ok(OggitState {
        timeline_id: row.get(1),
        ancestor_timeline_id: row.get(2),
        branch_start_lsn: row.get(3),
        decode_lsn: row.get(5),
        scanned_lsn: row.get(6),
        status: row.get(8),
        last_error: row.get(9),
    })
}

async fn oggit_read_change_log(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
    from_lsn: &str,
    to_lsn: &str,
    include_tables: Option<&[String]>,
) -> Result<Vec<OggitChange>> {
    let relname = oggit_meta_table_name(client, meta_schema, "change_log").await?;
    let sql = format!(
        "SELECT id, commit_lsn, ordinal, op, schema_name, table_name, identity_kind,
                key_json::text, old_row::text, new_row::text, changed_cols, unsupported_reason
           FROM {}.{}",
        quote_sql_ident(meta_schema),
        quote_sql_ident(&relname)
    );
    let include_tables = include_tables
        .map(|tables| tables.iter().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let from_value = oggit_lsn_value(from_lsn);
    let to_value = oggit_lsn_value(to_lsn);
    let mut changes = Vec::new();

    for row in client
        .query(sql.as_str(), &[])
        .await
        .with_context(|| format!("failed to read oggit change_log from {meta_schema}.{relname}"))?
    {
        let commit_lsn: String = row.get(1);
        let table_name: Option<String> = row.get(5);
        if oggit_lsn_value(&commit_lsn) <= from_value || oggit_lsn_value(&commit_lsn) > to_value {
            continue;
        }
        let table_name = table_name.unwrap_or_default();
        if !include_tables.is_empty() && !include_tables.contains(&table_name) {
            continue;
        }

        changes.push(OggitChange {
            id: row.get(0),
            commit_lsn,
            ordinal: row.get(2),
            op: row.get(3),
            schema_name: row.get::<_, Option<String>>(4).unwrap_or_default(),
            table_name,
            identity_kind: row.get(6),
            key_json: oggit_parse_json_text(row.get(7))?,
            old_row: oggit_parse_json_text(row.get(8))?,
            new_row: oggit_parse_json_text(row.get(9))?,
            changed_cols: row.get::<_, Option<Vec<String>>>(10).unwrap_or_default(),
            unsupported_reason: row.get(11),
        });
    }

    changes.sort_by_key(|change| {
        (
            oggit_lsn_value(&change.commit_lsn),
            change.ordinal,
            change.id,
        )
    });
    Ok(changes)
}

async fn oggit_read_object_change(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
    from_lsn: &str,
    to_lsn: &str,
) -> Result<Vec<OggitObjectChange>> {
    let relname = oggit_meta_table_name(client, meta_schema, "object_change").await?;
    let sql = format!(
        "SELECT id, commit_lsn, ordinal, object_type, schema_name, object_name,
                action, change_json::text, safety_class, unsupported_reason
           FROM {}.{}",
        quote_sql_ident(meta_schema),
        quote_sql_ident(&relname)
    );
    let from_value = oggit_lsn_value(from_lsn);
    let to_value = oggit_lsn_value(to_lsn);
    let mut changes = Vec::new();

    for row in client.query(sql.as_str(), &[]).await.with_context(|| {
        format!("failed to read oggit object_change from {meta_schema}.{relname}")
    })? {
        let commit_lsn: String = row.get(1);
        if oggit_lsn_value(&commit_lsn) <= from_value || oggit_lsn_value(&commit_lsn) > to_value {
            continue;
        }
        let change_json_text: Option<String> = row.get(7);
        changes.push(OggitObjectChange {
            id: row.get(0),
            commit_lsn,
            ordinal: row.get(2),
            object_type: row.get(3),
            schema_name: row.get(4),
            object_name: row.get(5),
            change_json: oggit_parse_json_text(change_json_text)?
                .unwrap_or_else(oggit_empty_object),
            safety_class: row.get(8),
            unsupported_reason: row.get(9),
        });
    }

    changes.sort_by_key(|change| {
        (
            oggit_lsn_value(&change.commit_lsn),
            change.ordinal,
            change.id,
        )
    });
    Ok(changes)
}

async fn oggit_user_changes_after_planned_lsn(
    client: &tokio_opengauss::Client,
    planned_lsn: &str,
    current_lsn: &str,
) -> Result<Vec<String>> {
    let planned_value = oggit_lsn_value(planned_lsn);
    let mut changes = Vec::new();

    for change in oggit_read_change_log(client, "oggit", planned_lsn, current_lsn, None).await? {
        if oggit_lsn_value(&change.commit_lsn) <= planned_value
            || oggit_is_internal_schema(&change.schema_name)
        {
            continue;
        }
        changes.push(format!(
            "row {}.{} at {}",
            change.schema_name, change.table_name, change.commit_lsn
        ));
    }

    for object in oggit_read_object_change(client, "oggit", planned_lsn, current_lsn).await? {
        if oggit_lsn_value(&object.commit_lsn) <= planned_value {
            continue;
        }
        if oggit_object_change_is_internal(&object) {
            continue;
        }
        if oggit_object_change_is_merge_barrier_ddl(&object) {
            continue;
        }
        changes.push(format!(
            "object {}.{} ({}) at {}",
            object.schema_name.unwrap_or_default(),
            object.object_name.unwrap_or_default(),
            object.object_type,
            object.commit_lsn
        ));
    }

    Ok(changes)
}

async fn oggit_validate_merge_inputs(
    client: &tokio_opengauss::Client,
    source_meta_schema: &str,
    from_lsn: Option<&str>,
    source_to_lsn: Option<&str>,
    target_to_lsn: Option<&str>,
) -> Result<OggitMergeBounds> {
    let source_state = oggit_read_state(client, source_meta_schema).await?;
    if source_state.status != "active" {
        bail!(
            "source oggit worker is not active: {}, last_error={}",
            source_state.status,
            source_state.last_error.unwrap_or_default()
        );
    }

    let target_state = oggit_read_state(client, "oggit").await?;
    if target_state.status != "active" {
        bail!(
            "target oggit worker is not active: {}, last_error={}",
            target_state.status,
            target_state.last_error.unwrap_or_default()
        );
    }

    let direction = if source_state.ancestor_timeline_id.as_deref()
        == Some(target_state.timeline_id.as_str())
    {
        OggitMergeDirection::ChildToParent
    } else if target_state.ancestor_timeline_id.as_deref()
        == Some(source_state.timeline_id.as_str())
    {
        OggitMergeDirection::ParentToChild
    } else {
        bail!(
            "source timeline {} and target timeline {} are not direct parent/child timelines (source ancestor: {}, target ancestor: {})",
            source_state.timeline_id,
            target_state.timeline_id,
            source_state
                .ancestor_timeline_id
                .clone()
                .unwrap_or_default(),
            target_state
                .ancestor_timeline_id
                .clone()
                .unwrap_or_default()
        );
    };

    let source_timeline_id = source_state.timeline_id.clone();
    let target_timeline_id = target_state.timeline_id.clone();
    let (mut child_state, mut parent_state) = match direction {
        OggitMergeDirection::ChildToParent => (source_state, target_state),
        OggitMergeDirection::ParentToChild => (target_state, source_state),
    };

    let base_lsn = child_state.branch_start_lsn.clone();
    let (child_to_lsn, parent_to_lsn) = match direction {
        OggitMergeDirection::ChildToParent => (
            source_to_lsn
                .map(str::to_string)
                .unwrap_or_else(|| child_state.decode_lsn.clone()),
            target_to_lsn
                .map(str::to_string)
                .unwrap_or_else(|| parent_state.decode_lsn.clone()),
        ),
        OggitMergeDirection::ParentToChild => (
            target_to_lsn
                .map(str::to_string)
                .unwrap_or_else(|| child_state.decode_lsn.clone()),
            source_to_lsn
                .map(str::to_string)
                .unwrap_or_else(|| parent_state.decode_lsn.clone()),
        ),
    };

    let (child_from_lsn, parent_from_lsn) = if let Some(from_lsn) = from_lsn {
        (from_lsn.to_string(), from_lsn.to_string())
    } else if let Some(previous) = oggit_find_latest_applied_merge_bounds(
        client,
        source_meta_schema,
        &source_timeline_id,
        &target_timeline_id,
        &child_state.timeline_id,
        &parent_state.timeline_id,
        &child_to_lsn,
        &parent_to_lsn,
    )
    .await?
    {
        (previous.child_from_lsn, previous.parent_from_lsn)
    } else {
        (base_lsn.clone(), base_lsn.clone())
    };

    if oggit_lsn_value(&child_from_lsn) > oggit_lsn_value(&child_to_lsn)
        || oggit_lsn_value(&parent_from_lsn) > oggit_lsn_value(&parent_to_lsn)
    {
        bail!(
            "oggit merge history boundary is invalid: child_from_lsn={}, child_to_lsn={}, parent_from_lsn={}, parent_to_lsn={}",
            child_from_lsn,
            child_to_lsn,
            parent_from_lsn,
            parent_to_lsn
        );
    }
    if oggit_lsn_value(&child_from_lsn) < oggit_lsn_value(&child_state.branch_start_lsn) {
        bail!(
            "oggit merge history boundary {} predates child branch start {}",
            child_from_lsn,
            child_state.branch_start_lsn
        );
    }

    let child_meta_schema = match direction {
        OggitMergeDirection::ChildToParent => source_meta_schema,
        OggitMergeDirection::ParentToChild => "oggit",
    };
    let parent_meta_schema = match direction {
        OggitMergeDirection::ChildToParent => "oggit",
        OggitMergeDirection::ParentToChild => source_meta_schema,
    };
    if oggit_lsn_value(&child_to_lsn) > oggit_lsn_value(&child_state.scanned_lsn) {
        child_state =
            oggit_wait_metadata_lsn(client, child_meta_schema, &child_to_lsn, "child").await?;
    }
    if oggit_lsn_value(&parent_to_lsn) > oggit_lsn_value(&parent_state.scanned_lsn) {
        parent_state =
            oggit_wait_metadata_lsn(client, parent_meta_schema, &parent_to_lsn, "parent").await?;
    }

    Ok(OggitMergeBounds {
        child_state,
        parent_state,
        direction,
        base_lsn,
        child_from_lsn,
        child_to_lsn,
        parent_from_lsn,
        parent_to_lsn,
    })
}

async fn oggit_wait_metadata_lsn(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
    requested_lsn: &str,
    label: &str,
) -> Result<OggitState> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let state = oggit_read_state(client, meta_schema).await?;
        if state.status != "active" {
            bail!(
                "{label} oggit worker is not active while waiting for requested LSN {}: {}, last_error={}",
                requested_lsn,
                state.status,
                state.last_error.clone().unwrap_or_default()
            );
        }
        if oggit_lsn_value(&state.scanned_lsn) >= oggit_lsn_value(requested_lsn) {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for {label} oggit metadata: requested_lsn={}, scanned_lsn={}, decode_lsn={}, status={}, last_error={}",
                requested_lsn,
                state.scanned_lsn,
                state.decode_lsn,
                state.status,
                state.last_error.clone().unwrap_or_default()
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}

pub(crate) async fn oggit_freeze_metadata_lsn(
    client: &tokio_opengauss::Client,
    requested_lsn: &str,
    label: &str,
) -> Result<()> {
    oggit_wait_metadata_lsn(client, "oggit", requested_lsn, label)
        .await
        .map(|_| ())
}

async fn oggit_compact_change_log(
    client: &tokio_opengauss::Client,
    meta_schema: &str,
    from_lsn: &str,
    to_lsn: &str,
    include_tables: Option<&[String]>,
) -> Result<Vec<OggitDelta>> {
    let mut work = BTreeMap::<String, OggitDelta>::new();
    let changes =
        oggit_read_change_log(client, meta_schema, from_lsn, to_lsn, include_tables).await?;
    let object_barriers = oggit_read_object_change(client, meta_schema, from_lsn, to_lsn)
        .await?
        .into_iter()
        .filter(|object| !oggit_object_change_is_internal(object))
        .collect::<Vec<_>>();
    let mut barrier_index = 0_usize;
    let mut segment_id = 0_i64;

    for change in changes {
        while object_barriers.get(barrier_index).is_some_and(|barrier| {
            oggit_event_order(&barrier.commit_lsn, barrier.ordinal, barrier.id)
                <= oggit_event_order(&change.commit_lsn, change.ordinal, change.id)
        }) {
            segment_id += 1;
            barrier_index += 1;
        }

        let key_text = format!(
            "{}:{}.{}:{}",
            segment_id,
            change.schema_name,
            change.table_name,
            change
                .key_json
                .as_ref()
                .or(change.old_row.as_ref())
                .map(JsonValue::to_string)
                .unwrap_or_else(|| change.id.to_string())
        );

        if change.identity_kind == "unsupported" || change.op == "UNSUPPORTED" {
            work.entry(format!("{key_text}:unsupported:{}", change.id))
                .or_insert(OggitDelta {
                    schema_name: change.schema_name,
                    table_name: change.table_name,
                    identity_kind: change.identity_kind,
                    key_json: change.key_json,
                    final_op: "UNSUPPORTED".to_string(),
                    old_row: change.old_row,
                    new_row: change.new_row,
                    changed_cols: change.changed_cols,
                    unsupported_reason: Some(
                        change
                            .unsupported_reason
                            .unwrap_or_else(|| "unsupported decoded row change".to_string()),
                    ),
                    first_commit_lsn: change.commit_lsn,
                    first_ordinal: change.ordinal,
                    first_id: change.id,
                    segment_id,
                });
            continue;
        }

        if change.identity_kind == "replica_identity_full" {
            let full_key = match change.op.as_str() {
                "UPDATE" | "DELETE" => change.old_row.clone(),
                "INSERT" => change.new_row.clone(),
                _ => change.old_row.clone().or(change.new_row.clone()),
            };
            if full_key.is_none() {
                work.entry(format!("{key_text}:replica_identity_full:{}", change.id))
                    .or_insert(OggitDelta {
                        schema_name: change.schema_name,
                        table_name: change.table_name,
                        identity_kind: change.identity_kind,
                        key_json: change.key_json,
                        final_op: "UNSUPPORTED".to_string(),
                        old_row: change.old_row,
                        new_row: change.new_row,
                        changed_cols: change.changed_cols,
                        unsupported_reason: Some(
                            "replica identity full change has no row image for matching"
                                .to_string(),
                        ),
                        first_commit_lsn: change.commit_lsn,
                        first_ordinal: change.ordinal,
                        first_id: change.id,
                        segment_id,
                    });
                continue;
            }
            let key_text = format!(
                "{}:{}.{}:{}",
                segment_id,
                change.schema_name,
                change.table_name,
                full_key
                    .as_ref()
                    .map(JsonValue::to_string)
                    .unwrap_or_default()
            );
            if !work.contains_key(&key_text) {
                work.insert(
                    key_text.clone(),
                    OggitDelta {
                        schema_name: change.schema_name.clone(),
                        table_name: change.table_name.clone(),
                        identity_kind: change.identity_kind.clone(),
                        key_json: full_key.clone(),
                        final_op: change.op.clone(),
                        old_row: change.old_row.clone(),
                        new_row: change.new_row.clone(),
                        changed_cols: change.changed_cols.clone(),
                        unsupported_reason: None,
                        first_commit_lsn: change.commit_lsn.clone(),
                        first_ordinal: change.ordinal,
                        first_id: change.id,
                        segment_id,
                    },
                );
                continue;
            }
        }

        if !work.contains_key(&key_text) {
            match change.op.as_str() {
                "INSERT" => {
                    work.insert(
                        key_text,
                        OggitDelta {
                            schema_name: change.schema_name,
                            table_name: change.table_name,
                            identity_kind: change.identity_kind,
                            key_json: change.key_json,
                            final_op: "INSERT".to_string(),
                            old_row: None,
                            new_row: Some(oggit_json_object_or_empty(change.new_row)),
                            changed_cols: change.changed_cols,
                            unsupported_reason: None,
                            first_commit_lsn: change.commit_lsn,
                            first_ordinal: change.ordinal,
                            first_id: change.id,
                            segment_id,
                        },
                    );
                }
                "UPDATE" => {
                    work.insert(
                        key_text,
                        OggitDelta {
                            schema_name: change.schema_name,
                            table_name: change.table_name,
                            identity_kind: change.identity_kind,
                            key_json: change.key_json,
                            final_op: "UPDATE".to_string(),
                            old_row: Some(oggit_json_object_or_empty(change.old_row)),
                            new_row: Some(oggit_json_object_or_empty(change.new_row)),
                            changed_cols: change.changed_cols,
                            unsupported_reason: None,
                            first_commit_lsn: change.commit_lsn,
                            first_ordinal: change.ordinal,
                            first_id: change.id,
                            segment_id,
                        },
                    );
                }
                "DELETE" => {
                    work.insert(
                        key_text,
                        OggitDelta {
                            schema_name: change.schema_name,
                            table_name: change.table_name,
                            identity_kind: change.identity_kind,
                            key_json: change.key_json,
                            final_op: "DELETE".to_string(),
                            old_row: Some(oggit_json_object_or_empty(change.old_row)),
                            new_row: None,
                            changed_cols: Vec::new(),
                            unsupported_reason: None,
                            first_commit_lsn: change.commit_lsn,
                            first_ordinal: change.ordinal,
                            first_id: change.id,
                            segment_id,
                        },
                    );
                }
                _ => {}
            }
            continue;
        }

        if let Some(existing) = work.get_mut(&key_text) {
            match change.op.as_str() {
                "INSERT" => {
                    existing.final_op = "INSERT".to_string();
                    existing.old_row = None;
                    existing.new_row = Some(oggit_json_object_or_empty(change.new_row));
                    existing.changed_cols =
                        oggit_array_union(&existing.changed_cols, &change.changed_cols);
                }
                "UPDATE" => {
                    if existing.final_op != "INSERT" {
                        existing.final_op = "UPDATE".to_string();
                    }
                    existing.new_row = Some(oggit_json_merge(
                        existing.new_row.as_ref(),
                        change.new_row.as_ref(),
                    ));
                    existing.changed_cols =
                        oggit_array_union(&existing.changed_cols, &change.changed_cols);
                }
                "DELETE" => {
                    if existing.final_op == "INSERT" {
                        work.remove(&key_text);
                    } else {
                        existing.final_op = "DELETE".to_string();
                        existing.new_row = None;
                        existing.changed_cols.clear();
                    }
                }
                _ => {}
            }
        }
    }

    let mut values = work.into_values().collect::<Vec<_>>();
    values.sort_by_key(|delta| {
        oggit_event_order(&delta.first_commit_lsn, delta.first_ordinal, delta.first_id)
    });
    Ok(values)
}

fn oggit_row_conflict_type(
    ours_op: Option<&str>,
    ours_new: Option<&JsonValue>,
    ours_cols: &[String],
    theirs_op: Option<&str>,
    theirs_new: Option<&JsonValue>,
    theirs_cols: &[String],
) -> Option<String> {
    let (Some(ours_op), Some(theirs_op)) = (ours_op, theirs_op) else {
        return None;
    };

    if ours_op == "DELETE" && theirs_op == "DELETE" {
        return None;
    }
    if ours_op == "DELETE" || theirs_op == "DELETE" {
        return Some("delete_update".to_string());
    }
    if oggit_json_object_or_empty(ours_new.cloned())
        == oggit_json_object_or_empty(theirs_new.cloned())
    {
        return None;
    }

    let ours_cols = oggit_effective_changed_cols(ours_cols);
    let theirs_cols = oggit_effective_changed_cols(theirs_cols);
    for col in &theirs_cols {
        if ours_cols.contains(col)
            && ours_new.and_then(|v| v.get(col)) != theirs_new.and_then(|v| v.get(col))
        {
            return Some("same_column_update".to_string());
        }
    }

    None
}

fn oggit_effective_changed_cols(changed_cols: &[String]) -> BTreeSet<String> {
    changed_cols.iter().cloned().collect()
}

fn oggit_delta_key(delta: &OggitDelta) -> String {
    format!(
        "{}.{}:{}",
        delta.schema_name,
        delta.table_name,
        oggit_json_text(&delta.key_json)
    )
}

fn oggit_delta_map_by_key(delta: &[OggitDelta]) -> BTreeMap<String, &OggitDelta> {
    delta
        .iter()
        .map(|delta| (oggit_delta_key(delta), delta))
        .collect::<BTreeMap<_, _>>()
}

fn oggit_row_conflict_matches_delta(conflict: &OggitConflict, delta: &OggitDelta) -> bool {
    if conflict.table_name.is_none()
        || conflict.key_json.is_none()
        || conflict.theirs_op.as_deref() != Some(delta.final_op.as_str())
    {
        return false;
    }
    conflict.schema_name.as_deref() == Some(delta.schema_name.as_str())
        && conflict.table_name.as_deref() == Some(delta.table_name.as_str())
        && oggit_json_text(&conflict.key_json) == oggit_json_text(&delta.key_json)
        && (conflict.conflict_type == "apply_error" || conflict.theirs_json == delta.new_row)
}

fn oggit_project_update_to_changed_cols(delta: &mut OggitDelta) {
    if delta.final_op != "UPDATE" {
        return;
    }
    let Some(JsonValue::Object(new_map)) = delta.new_row.as_ref() else {
        return;
    };

    let projected = if !delta.changed_cols.is_empty() {
        new_map
            .iter()
            .filter(|(key, _)| delta.changed_cols.contains(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<JsonMap<_, _>>()
    } else {
        JsonMap::new()
    };

    if !projected.is_empty() || !new_map.is_empty() {
        delta.new_row = Some(JsonValue::Object(projected));
    }
}

fn oggit_projected_update_delta(delta: Option<&OggitDelta>) -> Option<OggitDelta> {
    let mut delta = delta.cloned()?;
    oggit_project_update_to_changed_cols(&mut delta);
    Some(delta)
}

fn oggit_rebase_replica_identity_full_key(delta: &mut OggitDelta, ours: Option<&OggitDelta>) {
    if !oggit_uses_full_row_identity(delta) || delta.final_op == "INSERT" {
        return;
    }

    let Some(ours) = ours else {
        return;
    };
    if ours.final_op == "DELETE" {
        return;
    }
    let Some(current_row) = ours.new_row.as_ref() else {
        return;
    };

    delta.key_json = Some(oggit_json_merge(delta.key_json.as_ref(), Some(current_row)));
}

fn oggit_uses_full_row_identity(delta: &OggitDelta) -> bool {
    if delta.identity_kind == "replica_identity_full" {
        return true;
    }
    if delta.identity_kind != "unique_key" || delta.final_op == "INSERT" {
        return false;
    }

    match (&delta.key_json, &delta.old_row) {
        (Some(key_json), Some(old_row)) => key_json == old_row,
        _ => false,
    }
}

async fn oggit_replica_full_match_conflict(
    client: &tokio_opengauss::Client,
    delta: &OggitDelta,
    ours: Option<&OggitDelta>,
) -> Result<Option<String>> {
    if !oggit_uses_full_row_identity(delta) || delta.final_op == "INSERT" {
        return Ok(None);
    }

    let mut match_delta = delta.clone();
    oggit_rebase_replica_identity_full_key(&mut match_delta, ours);
    let matches = oggit_count_matching_rows(
        client,
        &match_delta.schema_name,
        &match_delta.table_name,
        &match_delta.key_json,
    )
    .await?;

    Ok((matches != 1).then(|| format!("replica_identity_full_match_count_{matches}")))
}

pub(crate) async fn oggit_diff_from_meta(
    client: &tokio_opengauss::Client,
    source_meta_schema: &str,
    include_tables: Option<&[String]>,
    from_lsn: Option<&str>,
    source_to_lsn: Option<&str>,
    target_to_lsn: Option<&str>,
) -> Result<OggitDiffResult> {
    let bounds = oggit_validate_merge_inputs(
        client,
        source_meta_schema,
        from_lsn,
        source_to_lsn,
        target_to_lsn,
    )
    .await?;

    let ours_delta = oggit_compact_change_log(
        client,
        "oggit",
        bounds.target_from_lsn(),
        bounds.target_to_lsn(),
        include_tables,
    )
    .await?;
    let theirs_delta = oggit_compact_change_log(
        client,
        source_meta_schema,
        bounds.source_from_lsn(),
        bounds.source_to_lsn(),
        include_tables,
    )
    .await?;
    let mut ours_by_key = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for delta in &ours_delta {
        let key = format!(
            "{}.{}:{}",
            delta.schema_name,
            delta.table_name,
            oggit_json_text(&delta.key_json)
        );
        keys.insert(key.clone());
        ours_by_key.insert(key, delta.clone());
    }
    let mut theirs_by_key = BTreeMap::new();
    for delta in &theirs_delta {
        let key = format!(
            "{}.{}:{}",
            delta.schema_name,
            delta.table_name,
            oggit_json_text(&delta.key_json)
        );
        keys.insert(key.clone());
        theirs_by_key.insert(key, delta.clone());
    }

    let mut rows = Vec::new();
    for key in keys {
        let ours = ours_by_key.get(&key);
        let theirs = theirs_by_key.get(&key);
        let sample = ours.or(theirs).expect("key came from a delta");
        let projected_ours = oggit_projected_update_delta(ours);
        let projected_theirs = oggit_projected_update_delta(theirs);
        let ours_for_diff = projected_ours.as_ref().or(ours);
        let theirs_for_diff = projected_theirs.as_ref().or(theirs);
        let mut detail = ours
            .and_then(|delta| delta.unsupported_reason.clone())
            .or_else(|| theirs.and_then(|delta| delta.unsupported_reason.clone()));
        let replica_full_match_conflict = if let Some(theirs) = theirs {
            oggit_replica_full_match_conflict(client, theirs, ours).await?
        } else {
            None
        };
        let diff_type = if detail.is_some() {
            "unsupported".to_string()
        } else if let Some(conflict_type) = replica_full_match_conflict {
            detail = Some(conflict_type);
            "row_conflict".to_string()
        } else if ours.is_none() {
            "theirs_only".to_string()
        } else if theirs.is_none() {
            "ours_only".to_string()
        } else if let Some(conflict_type) = oggit_row_conflict_type(
            ours_for_diff.map(|d| d.final_op.as_str()),
            ours_for_diff.and_then(|d| d.new_row.as_ref()),
            ours_for_diff
                .map(|d| d.changed_cols.as_slice())
                .unwrap_or(&[]),
            theirs_for_diff.map(|d| d.final_op.as_str()),
            theirs_for_diff.and_then(|d| d.new_row.as_ref()),
            theirs_for_diff
                .map(|d| d.changed_cols.as_slice())
                .unwrap_or(&[]),
        ) {
            detail = Some(conflict_type);
            "row_conflict".to_string()
        } else if ours_for_diff.map(|d| d.final_op.as_str())
            == theirs_for_diff.map(|d| d.final_op.as_str())
            && oggit_json_object_or_empty(ours_for_diff.and_then(|d| d.new_row.clone()))
                == oggit_json_object_or_empty(theirs_for_diff.and_then(|d| d.new_row.clone()))
        {
            "same_change".to_string()
        } else {
            "mergeable".to_string()
        };

        rows.push(OggitDiffRow {
            diff_scope: "row".to_string(),
            schema_name: sample.schema_name.clone(),
            table_name: sample.table_name.clone(),
            key_json: sample.key_json.clone(),
            diff_type,
            ours_json: ours_for_diff.and_then(|d| d.new_row.clone()),
            theirs_json: theirs_for_diff.and_then(|d| d.new_row.clone()),
            detail,
        });
    }

    let theirs_objects = oggit_read_object_change(
        client,
        source_meta_schema,
        bounds.source_from_lsn(),
        bounds.source_to_lsn(),
    )
    .await?
    .into_iter()
    .filter(|object| !oggit_object_change_is_internal(object))
    .collect::<Vec<_>>();
    let ours_objects = oggit_read_object_change(
        client,
        "oggit",
        bounds.target_from_lsn(),
        bounds.target_to_lsn(),
    )
    .await?
    .into_iter()
    .filter(|object| !oggit_object_change_is_internal(object))
    .collect::<Vec<_>>();
    let ours_object_keys = ours_objects
        .iter()
        .map(oggit_object_change_diff_key)
        .collect::<BTreeSet<_>>();
    let theirs_object_keys = theirs_objects
        .iter()
        .map(oggit_object_change_diff_key)
        .collect::<BTreeSet<_>>();
    let ours_objects_by_merge_key = ours_objects
        .iter()
        .map(|object| (oggit_object_change_merge_key(object), object))
        .collect::<BTreeMap<_, _>>();
    let theirs_objects_by_merge_key = theirs_objects
        .iter()
        .map(|object| (oggit_object_change_merge_key(object), object))
        .collect::<BTreeMap<_, _>>();

    for (key, theirs_object) in &theirs_objects_by_merge_key {
        if let Some(ours_object) = ours_objects_by_merge_key.get(key) {
            if oggit_object_change_diff_key(theirs_object)
                == oggit_object_change_diff_key(ours_object)
            {
                continue;
            }
            rows.push(OggitDiffRow {
                diff_scope: theirs_object.object_type.to_lowercase(),
                schema_name: theirs_object.schema_name.clone().unwrap_or_default(),
                table_name: theirs_object.object_name.clone().unwrap_or_default(),
                key_json: None,
                diff_type: "object_conflict".to_string(),
                ours_json: Some(ours_object.change_json.clone()),
                theirs_json: Some(theirs_object.change_json.clone()),
                detail: Some(format!(
                    "ours={} theirs={}",
                    ours_object.safety_class, theirs_object.safety_class
                )),
            });
        }
    }

    for (side, objects, other_keys) in [
        ("theirs_object", &theirs_objects, &ours_object_keys),
        ("ours_object", &ours_objects, &theirs_object_keys),
    ] {
        for object in objects {
            if other_keys.contains(&oggit_object_change_diff_key(&object))
                || (side == "theirs_object"
                    && ours_objects_by_merge_key
                        .contains_key(&oggit_object_change_merge_key(&object)))
                || (side == "ours_object"
                    && theirs_objects_by_merge_key
                        .contains_key(&oggit_object_change_merge_key(&object)))
            {
                continue;
            }
            rows.push(OggitDiffRow {
                diff_scope: object.object_type.to_lowercase(),
                schema_name: object.schema_name.clone().unwrap_or_default(),
                table_name: object.object_name.clone().unwrap_or_default(),
                key_json: None,
                diff_type: side.to_string(),
                ours_json: (side == "ours_object").then(|| object.change_json.clone()),
                theirs_json: (side == "theirs_object").then(|| object.change_json.clone()),
                detail: Some(
                    if oggit_object_change_can_auto_apply(object, bounds.direction) {
                        object.safety_class.clone()
                    } else {
                        format!(
                            "{}: {}",
                            object.safety_class,
                            oggit_object_change_review_reason(object)
                        )
                    },
                ),
            });
        }
    }

    Ok(OggitDiffResult {
        base_lsn: bounds.base_lsn,
        child_to_lsn: bounds.child_to_lsn,
        parent_to_lsn: bounds.parent_to_lsn,
        rows,
    })
}
