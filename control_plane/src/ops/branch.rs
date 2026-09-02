use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use tokio_opengauss::NoTls;
use url::Url;
use utils::id::TenantId;

use crate::BranchMergeStrategy;
use crate::endpoint::Endpoint;
use crate::merge_oggit::{
    oggit_abort_merge, oggit_continue_merge, oggit_diff_from_meta, oggit_finalize_merge_commit_lsn,
    oggit_freeze_metadata_lsn, oggit_json_text, oggit_merge_from_meta,
    oggit_merge_status, oggit_read_conflicts, oggit_require_no_active_merge,
    oggit_resolve_conflict, oggit_resolve_conflict_sql,
};

const OGGIT_FDW_SCHEMA: &str = "oggit_fdw";
const OGGIT_FDW_LOCK_NAME: &str = "neon_local:branch:oggit_fdw";

#[derive(Clone, Debug)]
pub struct BranchEndpointRef {
    pub endpoint_id: String,
    pub branch_name: String,
    pub connstr: String,
    pub fdw_host: String,
    pub fdw_port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl BranchEndpointRef {
    pub fn from_local(
        endpoint_id: String,
        endpoint: Arc<Endpoint>,
        branch_name: String,
        user: &str,
        database: &str,
    ) -> Self {
        Self {
            endpoint_id,
            branch_name,
            connstr: endpoint.connstr(user, database),
            fdw_host: endpoint.pg_address.ip().to_string(),
            fdw_port: endpoint.pg_address.port(),
            user: user.to_string(),
            password: None,
            database: database.to_string(),
        }
    }

    pub fn from_connstr(
        connstr: &str,
        branch_name: Option<&String>,
        fallback_name: &str,
        fallback_user: &str,
        fallback_database: &str,
    ) -> Result<Self> {
        let (fdw_host, fdw_port, user, password, database) =
            parse_external_connstr(connstr, fallback_user, fallback_database)?;
        if database != fallback_database {
            bail!(
                "endpoint connection string database '{}' does not match --database '{}'",
                database,
                fallback_database
            );
        }
        let branch_name = branch_name
            .cloned()
            .unwrap_or_else(|| fallback_name.to_string());

        Ok(Self {
            endpoint_id: connstr.to_string(),
            branch_name,
            connstr: connstr.to_string(),
            fdw_host,
            fdw_port,
            user,
            password,
            database,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum BranchConflictResolution {
    Ours,
    Theirs,
    Skip,
}

impl BranchConflictResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            BranchConflictResolution::Ours => "ours",
            BranchConflictResolution::Theirs => "theirs",
            BranchConflictResolution::Skip => "skip",
        }
    }
}

impl std::str::FromStr for BranchConflictResolution {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ours" => Ok(Self::Ours),
            "theirs" => Ok(Self::Theirs),
            "skip" => Ok(Self::Skip),
            _ => bail!("invalid conflict resolution {value}"),
        }
    }
}

pub struct BranchDiffOptions {
    pub source_endpoint: BranchEndpointRef,
    pub target_endpoint: BranchEndpointRef,
    pub source_schema: String,
    pub target_schema: String,
    pub fdw_server: String,
    pub keep_fdw: bool,
    pub incremental_oggit: bool,
}

pub struct BranchMergeOptions {
    pub source_endpoint: BranchEndpointRef,
    pub target_endpoint: BranchEndpointRef,
    pub source_schema: String,
    pub target_schema: String,
    pub strategy: BranchMergeStrategy,
    pub fdw_server: String,
    pub copy_source_only_tables: bool,
    pub keep_fdw: bool,
    pub incremental_oggit: bool,
}

pub struct BranchTargetOptions {
    pub target_endpoint: BranchEndpointRef,
    pub merge_id: String,
}

pub struct BranchResolveOptions {
    pub target_endpoint: BranchEndpointRef,
    pub merge_id: String,
    pub conflict_id: i64,
    pub resolution: Option<BranchConflictResolution>,
    pub custom_sql: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BranchCommandOutput {
    pub status: String,
    pub stdout: String,
    pub lines: Vec<String>,
}

impl BranchCommandOutput {
    fn ok(lines: Vec<String>) -> Self {
        Self {
            status: "ok".to_string(),
            stdout: lines.join("\n") + if lines.is_empty() { "" } else { "\n" },
            lines,
        }
    }
}

fn parse_external_connstr(
    connstr: &str,
    fallback_user: &str,
    fallback_database: &str,
) -> Result<(String, u16, String, Option<String>, String)> {
    if connstr.starts_with("postgresql://") || connstr.starts_with("postgres://") {
        let url = Url::parse(connstr).context("invalid endpoint connection string")?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow!("endpoint connection string must include host"))?
            .to_string();
        let port = url.port().unwrap_or(5432);
        let user = if url.username().is_empty() {
            fallback_user.to_string()
        } else {
            url.username().to_string()
        };
        let password = url
            .password()
            .map(urlencoding::decode)
            .transpose()
            .context("invalid percent-encoding in endpoint password")?
            .map(Cow::into_owned);
        let database = url.path().trim_start_matches('/');
        let database = if database.is_empty() {
            fallback_database.to_string()
        } else {
            database.to_string()
        };
        return Ok((host, port, user, password, database));
    }

    let mut host = None;
    let mut port = None;
    let mut user = None;
    let mut password = None;
    let mut database = None;
    for part in connstr.split_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        let value = value.trim_matches('\'').trim_matches('"');
        match key {
            "host" => host = Some(value.to_string()),
            "port" => port = Some(value.parse::<u16>().context("invalid connstr port")?),
            "user" => user = Some(value.to_string()),
            "password" => password = Some(value.to_string()),
            "dbname" | "database" => database = Some(value.to_string()),
            _ => {}
        }
    }

    let host = host.ok_or_else(|| anyhow!("endpoint connection string must include host"))?;
    Ok((
        host,
        port.unwrap_or(5432),
        user.unwrap_or_else(|| fallback_user.to_string()),
        password,
        database.unwrap_or_else(|| fallback_database.to_string()),
    ))
}

async fn connect_to_branch_endpoint(
    endpoint: &BranchEndpointRef,
) -> Result<tokio_opengauss::Client> {
    let (client, connection) = tokio_opengauss::connect(&endpoint.connstr, NoTls)
        .await
        .with_context(|| format!("failed to connect to endpoint at {}", endpoint.connstr))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

async fn current_endpoint_lsn(client: &tokio_opengauss::Client) -> Result<String> {
    let row = client
        .query_one("SELECT pg_current_xlog_location()::text", &[])
        .await
        .context("failed to read endpoint current LSN")?;
    Ok(row.get(0))
}

async fn ensure_oggit_worker_ready(
    client: &tokio_opengauss::Client,
    endpoint: &BranchEndpointRef,
    label: &str,
) -> Result<()> {
    let database_row = client
        .query_opt(
            "SELECT database_name, database_oid::text, current_database()::text
               FROM oggit.state
              WHERE id = true",
            &[],
        )
        .await
        .with_context(|| {
            format!(
                "failed to read oggit.state database identity on {label} endpoint '{}'",
                endpoint.endpoint_id
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "oggit.state is missing on {label} endpoint '{}' database '{}'",
                endpoint.endpoint_id,
                endpoint.database
            )
        })?;
    let state_database: Option<String> = database_row.get(0);
    let current_database: String = database_row.get(2);
    if state_database.as_deref() != Some(current_database.as_str())
        || current_database != endpoint.database
    {
        bail!(
            "oggit database mismatch on {label} endpoint '{}': requested '{}', connected '{}', state {:?}",
            endpoint.endpoint_id,
            endpoint.database,
            current_database,
            state_database
        );
    }

    let enabled = client
        .query_opt(
            "SELECT setting
               FROM pg_settings
              WHERE name = 'neon.oggit_enabled'",
            &[],
        )
        .await
        .with_context(|| {
            format!(
                "failed to read neon.oggit_enabled on {label} endpoint '{}'",
                endpoint.endpoint_id
            )
        })?;

    let enabled = enabled.map(|row| row.get::<_, String>(0));
    if enabled.as_deref() != Some("on") {
        let detail = enabled
            .as_deref()
            .map(|value| format!("neon.oggit_enabled={value}"))
            .unwrap_or_else(|| "neon.oggit_enabled is not registered".to_string());
        bail!(
            "incremental-oggit requires oggit worker enabled on {label} endpoint '{}' (branch '{}'): {}",
            endpoint.endpoint_id,
            endpoint.branch_name,
            detail
        );
    }

    let row = client
        .query_one(
            "SELECT count(*)::bigint
               FROM pg_stat_activity
              WHERE application_name = 'OggitWorker'",
            &[],
        )
        .await
        .with_context(|| {
            format!(
                "failed to check oggit worker activity on {label} endpoint '{}'",
                endpoint.endpoint_id
            )
        })?;
    let worker_count: i64 = row.get(0);
    if worker_count == 0 {
        bail!(
            "incremental-oggit requires a running oggit worker on {label} endpoint '{}' (branch '{}')",
            endpoint.endpoint_id,
            endpoint.branch_name
        );
    }

    Ok(())
}

async fn lock_branch_fdw_workspace(client: &tokio_opengauss::Client) -> Result<()> {
    client
        .query_one(
            "SELECT pg_advisory_lock(hashtext(current_database()), hashtext($1::text))",
            &[&OGGIT_FDW_LOCK_NAME],
        )
        .await
        .context("failed to lock the oggit FDW workspace")?;
    Ok(())
}

fn quote_sql_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

struct SourceColumn {
    name: String,
    data_type: String,
    not_null: bool,
    default_expr: Option<String>,
}

struct SourceConstraint {
    name: String,
    definition: String,
}

struct SourceIndex {
    definition: String,
}

async fn import_branch_source_foreign_tables(
    target_client: &mut tokio_opengauss::Client,
    source_endpoint: &BranchEndpointRef,
    source_schema: &str,
    fdw_schema: &str,
    fdw_server: &str,
) -> Result<()> {
    let source_client = connect_to_branch_endpoint(source_endpoint).await?;
    let rows = source_client
        .query(
            "SELECT c.relname::text,
                    a.attname::text,
                    pg_catalog.format_type(a.atttypid, a.atttypmod)::text,
                    a.attnotnull
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_attribute a ON a.attrelid = c.oid
             WHERE n.nspname = $1
               AND c.relkind = 'r'
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY c.relname, a.attnum",
            &[&source_schema],
        )
        .await
        .with_context(|| format!("failed to read source schema {source_schema} metadata"))?;

    let mut tables: BTreeMap<String, Vec<SourceColumn>> = BTreeMap::new();
    for row in rows {
        let table_name: String = row.get(0);
        tables.entry(table_name).or_default().push(SourceColumn {
            name: row.get(1),
            data_type: row.get(2),
            not_null: row.get(3),
            default_expr: None,
        });
    }

    for (table_name, columns) in tables {
        let column_defs = columns
            .iter()
            .map(|column| {
                let not_null = if column.not_null { " NOT NULL" } else { "" };
                format!(
                    "{} {}{}",
                    quote_sql_ident(&column.name),
                    column.data_type,
                    not_null
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let create_sql = format!(
            "CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS (schema_name {}, table_name {})",
            quote_sql_ident(fdw_schema),
            quote_sql_ident(&table_name),
            column_defs,
            quote_sql_ident(fdw_server),
            quote_sql_literal(source_schema),
            quote_sql_literal(&table_name)
        );

        target_client
            .batch_execute(&create_sql)
            .await
            .with_context(|| format!("failed to create foreign table {fdw_schema}.{table_name}"))?;
    }

    Ok(())
}

async fn import_source_oggit_foreign_tables(
    target_client: &mut tokio_opengauss::Client,
    fdw_schema: &str,
    fdw_server: &str,
) -> Result<()> {
    let table_defs = [
        (
            "oggit_state",
            "state",
            "id boolean,
             tenant_id text,
             timeline_id text,
             database_name text,
             database_oid oid,
             ancestor_timeline_id text,
             branch_start_lsn text,
             slot_name text,
             required_lsn text,
             decode_lsn text,
             scanned_lsn text,
             confirmed_lsn text,
             status text,
             last_error text,
             updated_at timestamptz",
        ),
        (
            "oggit_change_log",
            "change_log",
            "id bigint,
             commit_lsn text,
             record_lsn text,
             xid text,
             merge_id uuid,
             ordinal integer,
             op text,
             schema_name text,
             table_name text,
             relid oid,
             identity_kind text,
             key_json jsonb,
             old_row jsonb,
             new_row jsonb,
             changed_cols text[],
             unsupported_reason text,
             created_at timestamptz",
        ),
        (
            "oggit_object_change",
            "object_change",
            "id bigint,
             commit_lsn text,
             merge_id uuid,
             ordinal integer,
             object_type text,
             schema_name text,
             object_name text,
             action text,
             change_json jsonb,
             safety_class text,
             unsupported_reason text,
             created_at timestamptz",
        ),
        (
            "oggit_merge_history",
            "merge_history",
            "merge_id uuid,
             child_timeline_id text,
             parent_timeline_id text,
             base_timeline_id text,
             child_meta_schema text,
             base_lsn text,
             child_from_lsn text,
             child_to_lsn text,
             parent_from_lsn text,
             parent_to_lsn text,
             strategy text,
             status text,
             merge_commit_lsn text,
             conflict_count integer,
             created_at timestamptz,
             finished_at timestamptz",
        ),
    ];

    for (foreign_name, remote_name, columns) in table_defs {
        let create_sql = format!(
            "CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS (schema_name {}, table_name {})",
            quote_sql_ident(fdw_schema),
            quote_sql_ident(foreign_name),
            columns,
            quote_sql_ident(fdw_server),
            quote_sql_literal("oggit"),
            quote_sql_literal(remote_name)
        );

        target_client
            .batch_execute(&create_sql)
            .await
            .with_context(|| {
                format!("failed to create foreign table {fdw_schema}.{foreign_name}")
            })?;
    }

    Ok(())
}

async fn read_database_compatibility(client: &tokio_opengauss::Client) -> Result<String> {
    let row = client
        .query_one(
            "SELECT datcompatibility::text
             FROM pg_database
             WHERE datname = current_database()",
            &[],
        )
        .await
        .context("failed to read database compatibility")?;
    Ok(row.get(0))
}

async fn ensure_branch_database_compatibility(
    target_client: &tokio_opengauss::Client,
    source_endpoint: &BranchEndpointRef,
) -> Result<()> {
    let source_client = connect_to_branch_endpoint(source_endpoint).await?;
    let source_compatibility = read_database_compatibility(&source_client)
        .await
        .context("failed to read source database compatibility")?;
    let target_compatibility = read_database_compatibility(target_client)
        .await
        .context("failed to read target database compatibility")?;

    if source_compatibility != target_compatibility {
        bail!(
            "database compatibility mismatch: source database {} is {}, target database is {}",
            source_endpoint.database,
            source_compatibility,
            target_compatibility,
        );
    }

    Ok(())
}

async fn list_schema_tables(
    client: &tokio_opengauss::Client,
    schema: &str,
) -> Result<BTreeSet<String>> {
    let rows = client
        .query(
            "SELECT c.relname::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1
               AND c.relkind = 'r'
             ORDER BY c.relname",
            &[&schema],
        )
        .await
        .with_context(|| format!("failed to list tables in schema {schema}"))?;

    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn load_source_columns(
    source_client: &tokio_opengauss::Client,
    source_schema: &str,
    table_name: &str,
) -> Result<Vec<SourceColumn>> {
    let rows = source_client
        .query(
            "SELECT a.attname::text,
                    pg_catalog.format_type(a.atttypid, a.atttypmod)::text,
                    a.attnotnull,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_attribute a ON a.attrelid = c.oid
             LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
             WHERE n.nspname = $1
               AND c.relname = $2
               AND c.relkind = 'r'
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&source_schema, &table_name],
        )
        .await
        .with_context(|| format!("failed to read columns for {source_schema}.{table_name}"))?;

    if rows.is_empty() {
        bail!("source table {source_schema}.{table_name} does not exist or has no columns");
    }

    Ok(rows
        .into_iter()
        .map(|row| SourceColumn {
            name: row.get(0),
            data_type: row.get(1),
            not_null: row.get(2),
            default_expr: row.get(3),
        })
        .collect())
}

async fn load_source_constraints(
    source_client: &tokio_opengauss::Client,
    source_schema: &str,
    table_name: &str,
) -> Result<Vec<SourceConstraint>> {
    let rows = source_client
        .query(
            "SELECT con.conname::text,
                    pg_catalog.pg_get_constraintdef(con.oid, true)::text
             FROM pg_constraint con
             JOIN pg_class c ON c.oid = con.conrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1
               AND c.relname = $2
               AND con.contype IN ('p', 'u', 'c')
             ORDER BY CASE con.contype WHEN 'p' THEN 0 WHEN 'u' THEN 1 ELSE 2 END,
                      con.conname",
            &[&source_schema, &table_name],
        )
        .await
        .with_context(|| format!("failed to read constraints for {source_schema}.{table_name}"))?;

    Ok(rows
        .into_iter()
        .map(|row| SourceConstraint {
            name: row.get(0),
            definition: row.get(1),
        })
        .collect())
}

async fn reject_unsupported_source_schema_objects(
    source_client: &tokio_opengauss::Client,
    source_schema: &str,
) -> Result<()> {
    let rows = source_client
        .query(
            "SELECT object_name, feature
             FROM (
                 SELECT c.relname::text AS object_name,
                        CASE c.relkind
                            WHEN 'v' THEN 'views'
                            WHEN 'm' THEN 'materialized views'
                            WHEN 'S' THEN 'sequences'
                            WHEN 'L' THEN 'sequences'
                            WHEN 'z' THEN 'sequences'
                            WHEN 'Z' THEN 'sequences'
                            WHEN 'f' THEN 'foreign tables'
                            ELSE 'non-regular relations'
                        END AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relkind IN ('v', 'm', 'S', 'L', 'z', 'Z', 'f')

                 UNION ALL

                 SELECT p.proname::text AS object_name,
                        'functions or procedures'::text AS feature
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = $1
             ) unsupported
             ORDER BY feature, object_name",
            &[&source_schema],
        )
        .await
        .with_context(|| {
            format!("failed to inspect unsupported objects in schema {source_schema}")
        })?;

    let objects = rows
        .into_iter()
        .map(|row| {
            let object_name: String = row.get(0);
            let feature: String = row.get(1);
            format!("{object_name} ({feature})")
        })
        .collect::<Vec<_>>();

    if !objects.is_empty() {
        bail!(
            "source schema {source_schema} contains unsupported objects: {}",
            objects.join(", ")
        );
    }

    Ok(())
}

async fn reject_unsupported_source_table_features(
    source_client: &tokio_opengauss::Client,
    source_schema: &str,
    table_name: &str,
) -> Result<()> {
    let rows = source_client
        .query(
            "SELECT feature
             FROM (
                 SELECT 'foreign key constraints'::text AS feature
                 FROM pg_constraint con
                 JOIN pg_class c ON c.oid = con.conrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND con.contype = 'f'

                 UNION ALL

                 SELECT 'sequence or auto-increment defaults'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 JOIN pg_attribute a ON a.attrelid = c.oid
                 JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND c.relkind = 'r'
                   AND pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%'

                 UNION ALL

                 SELECT 'table or column comments'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND (
                       pg_catalog.obj_description(c.oid, 'pg_class') IS NOT NULL
                       OR EXISTS (
                           SELECT 1
                           FROM pg_attribute a
                           WHERE a.attrelid = c.oid
                             AND a.attnum > 0
                             AND NOT a.attisdropped
                             AND pg_catalog.col_description(c.oid, a.attnum) IS NOT NULL
                       )
                   )

                 UNION ALL

                 SELECT 'explicit privileges'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND c.relacl IS NOT NULL

                 UNION ALL

                 SELECT 'partitioned tables'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND EXISTS (
                       SELECT 1
                       FROM pg_partition p
                       WHERE p.parentid = c.oid
                          OR p.oid = c.oid
                   )

                 UNION ALL

                 SELECT 'triggers'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 JOIN pg_trigger t ON t.tgrelid = c.oid
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND NOT t.tgisinternal

                 UNION ALL

                 SELECT 'tablespace settings'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND c.reltablespace <> 0

                 UNION ALL

                 SELECT 'storage parameters'::text AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relname = $2
                   AND EXISTS (
                       SELECT 1
                       FROM unnest(c.reloptions) AS opt
                       WHERE opt NOT IN ('orientation=row', 'compression=no')
                         AND opt NOT LIKE 'collate=%'
                   )
             ) unsupported
             ORDER BY feature",
            &[&source_schema, &table_name],
        )
        .await
        .with_context(|| {
            format!("failed to inspect unsupported features for {source_schema}.{table_name}")
        })?;

    let features = rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    if !features.is_empty() {
        bail!(
            "source-only table {source_schema}.{table_name} uses unsupported features: {}",
            features.join(", ")
        );
    }

    Ok(())
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
    // Must pin schema: branch ops connect as branch_merge, whose default
    // search_path is "$user",public. Without WITH SCHEMA, a first-time install
    // lands in the branch_merge schema instead of neon.
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS neon WITH SCHEMA neon")
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

pub async fn diff_branch(opts: BranchDiffOptions) -> Result<BranchCommandOutput> {
    let mut lines = vec![format!(
        "Diffing source branch '{}' ({}) against target branch '{}' ({})",
        opts.source_endpoint.branch_name,
        opts.source_endpoint.endpoint_id,
        opts.target_endpoint.branch_name,
        opts.target_endpoint.endpoint_id
    )];

    let mut client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    lock_branch_fdw_workspace(&client).await?;
    let target_command_lsn = if opts.incremental_oggit {
        ensure_oggit_worker_ready(&client, &opts.target_endpoint, "target").await?;
        Some(current_endpoint_lsn(&client).await?)
    } else {
        None
    };
    let source_command_lsn = if opts.incremental_oggit {
        let source_client = connect_to_branch_endpoint(&opts.source_endpoint).await?;
        ensure_oggit_worker_ready(&source_client, &opts.source_endpoint, "source").await?;
        Some(current_endpoint_lsn(&source_client).await?)
    } else {
        None
    };
    ensure_branch_database_compatibility(&client, &opts.source_endpoint).await?;
    prepare_branch_source_fdw(
        &mut client,
        &opts.source_schema,
        OGGIT_FDW_SCHEMA,
        &opts.fdw_server,
        &opts.source_endpoint,
    )
    .await?;

    if opts.incremental_oggit {
        import_source_oggit_foreign_tables(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await?;
        let diff_result = oggit_diff_from_meta(
            &client,
            OGGIT_FDW_SCHEMA,
            None,
            None,
            source_command_lsn.as_deref(),
            target_command_lsn.as_deref(),
        )
        .await
        .context("branch diff failed")?;

        if !opts.keep_fdw {
            if let Err(e) =
                cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
            {
                lines.push(format!("Warning: {e:#}"));
            }
        }

        lines.push(format!(
            "oggit.diff\tbase_lsn={}\tchild_to_lsn={}\tparent_to_lsn={}",
            diff_result.base_lsn, diff_result.child_to_lsn, diff_result.parent_to_lsn
        ));

        for row in diff_result.rows {
            lines.push(format!(
                "{}\t{}.{}\t{}\tkey={}\tours={}\ttheirs={}\t{}",
                row.diff_scope,
                row.schema_name,
                row.table_name,
                row.diff_type,
                oggit_json_text(&row.key_json),
                oggit_json_text(&row.ours_json),
                oggit_json_text(&row.theirs_json),
                row.detail.unwrap_or_default()
            ));
        }
    } else {
        let diff_result = client
            .query(
                "SELECT 'row'::text, schema_name, table_name, diff_type,
                        COALESCE(row_data, ''), ''::text, ''::text, ''::text
                   FROM neon.neon_branch_diff($1::name, $2::name, NULL::name[])",
                &[&OGGIT_FDW_SCHEMA, &opts.target_schema],
            )
            .await;

        if !opts.keep_fdw {
            if let Err(e) =
                cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
            {
                lines.push(format!("Warning: {e:#}"));
            }
        }

        for row in diff_result.context("branch diff failed")? {
            let diff_scope = row.get::<_, Option<String>>(0).unwrap_or_default();
            let schema_name = row.get::<_, Option<String>>(1).unwrap_or_default();
            let table_name = row.get::<_, Option<String>>(2).unwrap_or_default();
            let diff_type = row.get::<_, Option<String>>(3).unwrap_or_default();
            let key_json = row.get::<_, Option<String>>(4).unwrap_or_default();
            let ours_json = row.get::<_, Option<String>>(5).unwrap_or_default();
            let theirs_json = row.get::<_, Option<String>>(6).unwrap_or_default();
            let detail = row.get::<_, Option<String>>(7).unwrap_or_default();

            lines.push(format!(
                "{diff_scope}\t{schema_name}.{table_name}\t{diff_type}\tkey={key_json}\tours={ours_json}\ttheirs={theirs_json}\t{detail}"
            ));
        }
    }

    Ok(BranchCommandOutput::ok(lines))
}

pub async fn merge_branch(opts: BranchMergeOptions) -> Result<BranchCommandOutput> {
    let mut lines = vec![format!(
        "Merging source branch '{}' ({}) into target branch '{}' ({}) with strategy '{}'",
        opts.source_endpoint.branch_name,
        opts.source_endpoint.endpoint_id,
        opts.target_endpoint.branch_name,
        opts.target_endpoint.endpoint_id,
        opts.strategy.as_str()
    )];

    let mut client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    lock_branch_fdw_workspace(&client).await?;
    if opts.incremental_oggit {
        oggit_require_no_active_merge(&client).await?;
    }
    let target_command_lsn = if opts.incremental_oggit {
        ensure_oggit_worker_ready(&client, &opts.target_endpoint, "target").await?;
        Some(current_endpoint_lsn(&client).await?)
    } else {
        None
    };
    let source_command_lsn = if opts.incremental_oggit {
        let source_client = connect_to_branch_endpoint(&opts.source_endpoint).await?;
        ensure_oggit_worker_ready(&source_client, &opts.source_endpoint, "source").await?;
        let requested_lsn = current_endpoint_lsn(&source_client).await?;
        oggit_freeze_metadata_lsn(&source_client, &requested_lsn, "source")
            .await
            .context("failed to freeze source oggit metadata before merge")?;
        Some(requested_lsn)
    } else {
        None
    };
    if let Some(requested_lsn) = target_command_lsn.as_deref() {
        oggit_freeze_metadata_lsn(&client, requested_lsn, "target")
            .await
            .context("failed to freeze target oggit metadata before merge")?;
    }
    ensure_branch_database_compatibility(&client, &opts.source_endpoint).await?;
    prepare_branch_source_fdw(
        &mut client,
        &opts.source_schema,
        OGGIT_FDW_SCHEMA,
        &opts.fdw_server,
        &opts.source_endpoint,
    )
    .await?;

    if !opts.incremental_oggit && matches!(opts.strategy, BranchMergeStrategy::Manual) {
        bail!("manual merge strategy requires --incremental-oggit");
    }

    if opts.incremental_oggit {
        import_source_oggit_foreign_tables(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await?;
        client
            .batch_execute("BEGIN")
            .await
            .context("failed to start branch merge transaction")?;

        let merge_result = oggit_merge_from_meta(
            &client,
            OGGIT_FDW_SCHEMA,
            opts.strategy,
            None,
            None,
            source_command_lsn.as_deref(),
            target_command_lsn.as_deref(),
        )
        .await;

        if merge_result.is_err() {
            if let Err(e) = client.batch_execute("ROLLBACK").await {
                lines.push(format!(
                    "Warning: failed to roll back branch merge transaction: {e:#}"
                ));
            }
            if !opts.keep_fdw {
                if let Err(e) =
                    cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
                {
                    lines.push(format!("Warning: {e:#}"));
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

        if merge_result.status == "blocked" && !opts.keep_fdw {
            lines.push(format!(
                "Keeping FDW schema '{}' and server '{}' so branch continue can reread source oggit metadata",
                OGGIT_FDW_SCHEMA, opts.fdw_server
            ));
        } else if !opts.keep_fdw {
            if let Err(e) =
                cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
            {
                lines.push(format!("Warning: {e:#}"));
            }
        }

        lines.push(format!(
            "oggit.merge\tmerge_id={}\tstatus={}\tconflicts={}\tapplied={}",
            merge_result.merge_id,
            merge_result.status,
            merge_result.conflict_count,
            merge_result.applied_count
        ));
        for detail in &merge_result.skipped_details {
            lines.push(format!("oggit.skipped\t{detail}"));
        }
        if merge_result.status == "failed" {
            bail!("branch merge failed");
        }
        return Ok(BranchCommandOutput::ok(lines));
    }

    client
        .batch_execute("BEGIN")
        .await
        .context("failed to start branch merge transaction")?;

    let merge_result: Result<(Vec<(String, i64)>, Vec<tokio_opengauss::Row>)> = async {
        let (copied_tables, common_tables) = copy_source_only_tables_with_schema(
            &mut client,
            &opts.source_endpoint,
            &opts.source_schema,
            &opts.target_schema,
            OGGIT_FDW_SCHEMA,
            opts.copy_source_only_tables,
        )
        .await?;

        let include_tables_sql = name_array_literal(&common_tables);
        let merge_sql = format!(
            "SELECT schema_name, table_name, inserted_count, updated_count
             FROM neon.neon_branch_merge($1::name, $2::name, $3, {include_tables_sql}, false)"
        );
        let merged_tables = client
            .query(
                merge_sql.as_str(),
                &[
                    &OGGIT_FDW_SCHEMA,
                    &opts.target_schema,
                    &opts.strategy.as_str(),
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
            lines.push(format!(
                "Warning: failed to roll back branch merge transaction: {e:#}"
            ));
        }
        if !opts.keep_fdw {
            if let Err(e) =
                cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
            {
                lines.push(format!("Warning: {e:#}"));
            }
        }
    }

    let (copied_tables, merged_tables) = merge_result.context("branch merge failed")?;

    if !opts.keep_fdw {
        if let Err(e) =
            cleanup_branch_source_fdw(&mut client, OGGIT_FDW_SCHEMA, &opts.fdw_server).await
        {
            lines.push(format!("Warning: {e:#}"));
        }
    }

    for (table_name, inserted_count) in copied_tables {
        lines.push(format!(
            "{}.{}\tinserted={}\tupdated=0",
            opts.target_schema, table_name, inserted_count
        ));
    }

    for row in merged_tables {
        let schema_name: String = row.get(0);
        let table_name: String = row.get(1);
        let inserted_count: i64 = row.get(2);
        let updated_count: i64 = row.get(3);

        lines.push(format!(
            "{schema_name}.{table_name}\tinserted={inserted_count}\tupdated={updated_count}"
        ));
    }

    Ok(BranchCommandOutput::ok(lines))
}

pub async fn merge_status(opts: BranchTargetOptions) -> Result<BranchCommandOutput> {
    let client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    let (history, pending_conflict_count) = oggit_merge_status(&client, &opts.merge_id)
        .await
        .context("failed to read oggit merge status")?
        .with_context(|| format!("merge {} not found", opts.merge_id))?;

    Ok(BranchCommandOutput::ok(vec![format!(
        "oggit.merge\tmerge_id={}\tbranch={} ({})\tdirection={}\tchild_timeline={}\tparent_timeline={}\tbase_lsn={}\tchild_to_lsn={}\tparent_to_lsn={}\tstrategy={}\tstatus={}\tconflicts={}\tpending={}",
        history.merge_id,
        opts.target_endpoint.branch_name,
        opts.target_endpoint.endpoint_id,
        history.merge_direction.as_str(),
        history.child_timeline_id,
        history.parent_timeline_id,
        history.base_lsn,
        history.child_to_lsn,
        history.parent_to_lsn,
        history.strategy,
        history.status,
        history.conflict_count,
        pending_conflict_count
    )]))
}

pub async fn conflicts(opts: BranchTargetOptions) -> Result<BranchCommandOutput> {
    let client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    let mut lines = vec![format!(
        "Listing oggit merge conflicts on target branch '{}' ({}) for merge {}",
        opts.target_endpoint.branch_name, opts.target_endpoint.endpoint_id, opts.merge_id
    )];
    for conflict in oggit_read_conflicts(&client, &opts.merge_id)
        .await
        .context("failed to list oggit merge conflicts")?
    {
        lines.push(format!(
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
        ));
    }
    Ok(BranchCommandOutput::ok(lines))
}

pub async fn resolve_conflict(opts: BranchResolveOptions) -> Result<BranchCommandOutput> {
    let client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    if let Some(custom_sql) = &opts.custom_sql {
        oggit_resolve_conflict_sql(&client, &opts.merge_id, opts.conflict_id, custom_sql)
            .await
            .context("failed to store custom oggit conflict SQL")?;
        Ok(BranchCommandOutput::ok(vec![format!(
            "Resolved conflict {} on target branch '{}' ({}) with custom SQL",
            opts.conflict_id, opts.target_endpoint.branch_name, opts.target_endpoint.endpoint_id
        )]))
    } else {
        let resolution = opts
            .resolution
            .context("pass either --resolution or --custom-sql")?;
        oggit_resolve_conflict(
            &client,
            &opts.merge_id,
            opts.conflict_id,
            resolution.as_str(),
        )
        .await
        .context("failed to resolve oggit conflict")?;
        Ok(BranchCommandOutput::ok(vec![format!(
            "Resolved conflict {} on target branch '{}' ({}) as {}",
            opts.conflict_id,
            opts.target_endpoint.branch_name,
            opts.target_endpoint.endpoint_id,
            resolution.as_str()
        )]))
    }
}

pub async fn continue_merge(opts: BranchTargetOptions) -> Result<BranchCommandOutput> {
    let client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    let mut lines = vec![format!(
        "Continuing oggit merge {} on target branch '{}' ({})",
        opts.merge_id, opts.target_endpoint.branch_name, opts.target_endpoint.endpoint_id
    )];
    client
        .batch_execute("BEGIN")
        .await
        .context("failed to start oggit continue transaction")?;
    let result = oggit_continue_merge(&client, &opts.merge_id).await;
    if result.is_err() {
        if let Err(e) = client.batch_execute("ROLLBACK").await {
            lines.push(format!(
                "Warning: failed to roll back oggit continue transaction: {e:#}"
            ));
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
    lines.push(format!(
        "oggit.merge\tmerge_id={}\tstatus={}\tconflicts={}\tapplied={}",
        result.merge_id, result.status, result.conflict_count, result.applied_count
    ));
    for detail in &result.skipped_details {
        lines.push(format!("oggit.skipped\t{detail}"));
    }
    Ok(BranchCommandOutput::ok(lines))
}

pub async fn abort_merge(opts: BranchTargetOptions) -> Result<BranchCommandOutput> {
    let client = connect_to_branch_endpoint(&opts.target_endpoint).await?;
    client
        .batch_execute("BEGIN")
        .await
        .context("failed to start oggit abort transaction")?;
    let result = oggit_abort_merge(&client, &opts.merge_id).await;
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
    Ok(BranchCommandOutput::ok(vec![format!(
        "Aborted oggit merge {} on target branch '{}' ({})",
        opts.merge_id, opts.target_endpoint.branch_name, opts.target_endpoint.endpoint_id
    )]))
}

pub fn default_tenant_id(
    explicit_tenant_id: Option<TenantId>,
    default_tenant_id: Option<TenantId>,
) -> Result<TenantId> {
    explicit_tenant_id
        .or(default_tenant_id)
        .context("No tenant id specified and default tenant is not configured")
}
