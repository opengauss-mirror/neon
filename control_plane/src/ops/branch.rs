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
                            WHEN 'f' THEN 'foreign tables'
                            ELSE 'non-regular relations'
                        END AS feature
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                   AND c.relkind IN ('v', 'm', 'S', 'f')

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
