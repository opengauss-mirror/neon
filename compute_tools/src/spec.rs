use std::fs::File;
use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use compute_api::responses::{
    ComputeConfig, ControlPlaneComputeStatus, ControlPlaneConfigResponse,
};
use reqwest::StatusCode;
use tokio_postgres::Client;
use tracing::{error, info, instrument};

use crate::compute::ComputeNodeParams;
use crate::config;
use crate::metrics::{CPLANE_REQUESTS_TOTAL, CPlaneRequestRPC, UNKNOWN_HTTP_STATUS};
use crate::migration::MigrationRunner;
use crate::params::PG_HBA_ALL_MD5;

// Do control plane request and return response if any. In case of error it
// returns a bool flag indicating whether it makes sense to retry the request
// and a string with error message.
fn do_control_plane_request(
    uri: &str,
    jwt: &str,
) -> Result<ControlPlaneConfigResponse, (bool, String, String)> {
    let resp = reqwest::blocking::Client::new()
        .get(uri)
        .header("Authorization", format!("Bearer {jwt}"))
        .send()
        .map_err(|e| {
            (
                true,
                format!("could not perform request to control plane: {e:?}"),
                UNKNOWN_HTTP_STATUS.to_string(),
            )
        })?;

    let status = resp.status();
    match status {
        StatusCode::OK => match resp.json::<ControlPlaneConfigResponse>() {
            Ok(spec_resp) => Ok(spec_resp),
            Err(e) => Err((
                true,
                format!("could not deserialize control plane response: {e:?}"),
                status.to_string(),
            )),
        },
        StatusCode::SERVICE_UNAVAILABLE => Err((
            true,
            "control plane is temporarily unavailable".to_string(),
            status.to_string(),
        )),
        StatusCode::BAD_GATEWAY => {
            // We have a problem with intermittent 502 errors now
            // https://github.com/neondatabase/cloud/issues/2353
            // It's fine to retry GET request in this case.
            Err((
                true,
                "control plane request failed with 502".to_string(),
                status.to_string(),
            ))
        }
        // Another code, likely 500 or 404, means that compute is unknown to the control plane
        // or some internal failure happened. Doesn't make much sense to retry in this case.
        _ => Err((
            false,
            format!("unexpected control plane response status code: {status}"),
            status.to_string(),
        )),
    }
}

/// Request config from the control-plane by compute_id. If
/// `NEON_CONTROL_PLANE_TOKEN` env variable is set, it will be used for
/// authorization.
pub fn get_config_from_control_plane(base_uri: &str, compute_id: &str) -> Result<ComputeConfig> {
    let cp_uri = format!("{base_uri}/compute/api/v2/computes/{compute_id}/spec");
    let jwt: String = std::env::var("NEON_CONTROL_PLANE_TOKEN").unwrap_or_default();
    let mut attempt = 1;

    info!("getting config from control plane: {}", cp_uri);

    // Do 3 attempts to get spec from the control plane using the following logic:
    // - network error -> then retry
    // - compute id is unknown or any other error -> bail out
    // - no spec for compute yet (Empty state) -> return Ok(None)
    // - got config -> return Ok(Some(config))
    while attempt < 4 {
        let result = match do_control_plane_request(&cp_uri, &jwt) {
            Ok(config_resp) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[
                        CPlaneRequestRPC::GetConfig.as_str(),
                        &StatusCode::OK.to_string(),
                    ])
                    .inc();
                match config_resp.status {
                    ControlPlaneComputeStatus::Empty => Ok(config_resp.into()),
                    ControlPlaneComputeStatus::Attached => {
                        if config_resp.spec.is_some() {
                            Ok(config_resp.into())
                        } else {
                            bail!("compute is attached, but spec is empty")
                        }
                    }
                }
            }
            Err((retry, msg, status)) => {
                CPLANE_REQUESTS_TOTAL
                    .with_label_values(&[CPlaneRequestRPC::GetConfig.as_str(), &status])
                    .inc();
                if retry {
                    Err(anyhow!(msg))
                } else {
                    bail!(msg);
                }
            }
        };

        if let Err(e) = &result {
            error!("attempt {} to get config failed with: {}", attempt, e);
        } else {
            return result;
        }

        attempt += 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // All attempts failed, return error.
    Err(anyhow::anyhow!(
        "Exhausted all attempts to retrieve the config from the control plane"
    ))
}

/// Check `pg_hba.conf` and update if needed to allow external connections.
pub fn update_pg_hba(pgdata_path: &Path, databricks_pg_hba: Option<&String>) -> Result<()> {
    // XXX: consider making it a part of config.json
    let pghba_path = pgdata_path.join("pg_hba.conf");

    // Update pg_hba to contains databricks specfic settings before adding neon settings
    // PG uses the first record that matches to perform authentication, so we need to have
    // our rules before the default ones from neon.
    // See https://www.postgresql.org/docs/16/auth-pg-hba-conf.html
    if let Some(databricks_pg_hba) = databricks_pg_hba {
        if config::line_in_file(
            &pghba_path,
            &format!("include_if_exists {}\n", *databricks_pg_hba),
        )? {
            info!("updated pg_hba.conf to include databricks_pg_hba.conf");
        } else {
            info!("pg_hba.conf already included databricks_pg_hba.conf");
        }
    }

    if let Ok(remote_cidr) = std::env::var("COMPUTE_REMOTE_HBA_CIDR") {
        let remote_hba = format!("host\tall\t\tall\t\t{remote_cidr}\t\tsha256");

        if config::line_in_file(&pghba_path, &remote_hba)? {
            info!("updated pg_hba.conf to allow external openGauss connections");
        }
    }

    if config::line_in_file(&pghba_path, PG_HBA_ALL_MD5)? {
        info!("updated pg_hba.conf to allow external connections");
    } else {
        info!("pg_hba.conf is up-to-date");
    }

    Ok(())
}

/// Check `pg_ident.conf` and update if needed to allow databricks config.
pub fn update_pg_ident(pgdata_path: &Path, databricks_pg_ident: Option<&String>) -> Result<()> {
    info!("checking pg_ident.conf");
    let pghba_path = pgdata_path.join("pg_ident.conf");

    // Update pg_ident to contains databricks specfic settings
    if let Some(databricks_pg_ident) = databricks_pg_ident {
        if config::line_in_file(
            &pghba_path,
            &format!("include_if_exists {}\n", *databricks_pg_ident),
        )? {
            info!("updated pg_ident.conf to include databricks_pg_ident.conf");
        } else {
            info!("pg_ident.conf already included databricks_pg_ident.conf");
        }
    }

    Ok(())
}

/// Copy tls key_file and cert_file from k8s secret mount directory
/// to pgdata and set private key file permissions as expected by Postgres.
/// See this doc for expected permission <https://www.postgresql.org/docs/current/ssl-tcp.html>
/// K8s secrets mount on dblet does not honor permission and ownership
/// specified in the Volume or VolumeMount. So we need to explicitly copy the file and set the permissions.
pub fn copy_tls_certificates(
    key_file: &String,
    cert_file: &String,
    pgdata_path: &Path,
) -> Result<()> {
    let files = [cert_file, key_file];
    for file in files.iter() {
        let source = Path::new(file);
        let dest = pgdata_path.join(source.file_name().unwrap());
        if !dest.exists() {
            std::fs::copy(source, &dest)?;
            info!(
                "Copying tls file: {} to {}",
                &source.display(),
                &dest.display()
            );
        }
        if *file == key_file {
            // Postgres requires private key to be readable only by the owner by having
            // chmod 600 permissions.
            let permissions = Permissions::from_mode(0o600);
            fs::set_permissions(&dest, permissions)?;
            info!("Setting permission on {}.", &dest.display());
        }
    }
    Ok(())
}

/// Create a standby.signal file (PostgreSQL 12+) or recovery.conf (openGauss)
/// For openGauss, we need to create recovery.conf with standby_mode = 'on'
pub fn add_standby_signal(pgdata_path: &Path) -> Result<()> {
    add_standby_signal_ext(pgdata_path, false, None, None)
}

/// Create standby signal with openGauss support
/// For openGauss: creates recovery.conf with standby_mode = 'on' and primary_conninfo
/// For PostgreSQL: creates standby.signal file
pub fn add_standby_signal_ext(
    pgdata_path: &Path,
    is_opengauss: bool,
    primary_conninfo: Option<&str>,
    primary_slotname: Option<&str>,
) -> Result<()> {
    if is_opengauss {
        // openGauss uses recovery.conf file with standby_mode = 'on'
        let recovery_conf_path = pgdata_path.join("recovery.conf");

        use std::io::Write;
        let mut file = File::create(&recovery_conf_path)?;

        writeln!(
            file,
            "# openGauss recovery configuration for Neon hot standby"
        )?;
        writeln!(file, "standby_mode = 'on'")?;

        if let Some(conninfo) = primary_conninfo {
            writeln!(file, "primary_conninfo = '{}'", conninfo)?;
        }

        // openGauss uses 'primary_slotname' instead of PostgreSQL's 'primary_slot_name'
        if let Some(slotname) = primary_slotname {
            writeln!(file, "primary_slotname = '{}'", slotname)?;
        }

        info!("created recovery.conf for openGauss standby mode");
    } else {
        // PostgreSQL 12+ uses standby.signal file
        let signalfile = pgdata_path.join("standby.signal");

        if !signalfile.exists() {
            File::create(signalfile)?;
            info!("created standby.signal");
        } else {
            info!("reused pre-existing standby.signal");
        }
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn handle_neon_extension_upgrade(client: &mut Client, is_opengauss: bool) -> Result<()> {
    let query = if is_opengauss {
        info!("refreshing neon extension compatibility views on openGauss");
        OPENGAUSS_NEON_EXTENSION_COMPAT_SQL
    } else {
        "ALTER EXTENSION neon UPDATE"
    };
    info!("update neon extension version with query: {}", query);
    client.simple_query(query).await?;

    Ok(())
}

pub const OPENGAUSS_NEON_EXTENSION_COMPAT_SQL: &str = r#"
CREATE OR REPLACE FUNCTION neon.neon_start_oggit_worker()
RETURNS boolean
AS '$libdir/neon', 'neon_start_oggit_worker'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION neon.neon_oggit_worker_is_ready()
RETURNS boolean
AS '$libdir/neon', 'neon_oggit_worker_is_ready'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION neon.neon_start_oggit_worker() FROM PUBLIC;
REVOKE ALL ON FUNCTION neon.neon_oggit_worker_is_ready() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION neon.neon_start_oggit_worker() TO cloud_admin;
GRANT EXECUTE ON FUNCTION neon.neon_oggit_worker_is_ready() TO cloud_admin;

DELETE FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
   AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'neon')
   AND classid = 'pg_class'::regclass
   AND objid IN (
     SELECT c.oid
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'neon'
        AND c.relname IN ('neon_backend_perf_counters', 'neon_perf_counters')
   );

DROP VIEW IF EXISTS neon.neon_backend_perf_counters;
CREATE VIEW neon.neon_backend_perf_counters AS
  SELECT P.procno, P.pid, P.metric, P.bucket_le, P.value
  FROM neon.get_backend_perf_counters() AS P (
    procno integer,
    pid bigint,
    metric text,
    bucket_le float8,
    value float8
  );

DROP VIEW IF EXISTS neon.neon_perf_counters;
CREATE VIEW neon.neon_perf_counters AS
  SELECT P.metric, P.bucket_le, P.value
  FROM neon.get_perf_counters() AS P (
    metric text,
    bucket_le float8,
    value float8
  );
"#;

pub fn is_opengauss_pgbin(pgbin: &str) -> bool {
    pgbin.contains("gaussdb") || pgbin.contains("openGauss")
}

#[instrument(skip_all)]
pub async fn handle_migrations(
    params: ComputeNodeParams,
    client: &mut Client,
    lakebase_mode: bool,
) -> Result<()> {
    info!("handle migrations");
    let is_opengauss = is_opengauss_pgbin(&params.pgbin);

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // !BE SURE TO ONLY ADD MIGRATIONS TO THE END OF THIS ARRAY. IF YOU DO NOT, VERY VERY BAD THINGS MAY HAPPEN!
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    let migration_0001 = if is_opengauss {
        include_str!("./migrations/0001-add_bypass_rls_to_privileged_role_opengauss.sql")
            .to_string()
    } else {
        format!(
            include_str!("./migrations/0001-add_bypass_rls_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0002 = if is_opengauss {
        format!(
            include_str!("./migrations/0002-alter_roles_opengauss.sql"),
            privileged_role_name = params.privileged_role_name
        )
    } else {
        format!(
            include_str!("./migrations/0002-alter_roles.sql"),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0003 = if is_opengauss {
        format!(
            include_str!(
                "./migrations/0003-grant_pg_create_subscription_to_privileged_role_opengauss.sql"
            ),
            privileged_role_name = params.privileged_role_name
        )
    } else {
        format!(
            include_str!("./migrations/0003-grant_pg_create_subscription_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0004 = if is_opengauss {
        format!(
            include_str!("./migrations/0004-grant_pg_monitor_to_privileged_role_opengauss.sql"),
            privileged_role_name = params.privileged_role_name
        )
    } else {
        format!(
            include_str!("./migrations/0004-grant_pg_monitor_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0010 = if is_opengauss {
        include_str!(
            "./migrations/0010-grant_snapshot_synchronization_funcs_to_privileged_role_opengauss.sql"
        )
        .to_string()
    } else {
        format!(
            include_str!(
                "./migrations/0010-grant_snapshot_synchronization_funcs_to_privileged_role.sql"
            ),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0011 = if is_opengauss {
        include_str!(
            "./migrations/0011-grant_pg_show_replication_origin_status_to_privileged_role_opengauss.sql"
        )
        .to_string()
    } else {
        format!(
            include_str!(
                "./migrations/0011-grant_pg_show_replication_origin_status_to_privileged_role.sql"
            ),
            privileged_role_name = params.privileged_role_name
        )
    };
    let migration_0012 = if is_opengauss {
        format!(
            include_str!(
                "./migrations/0012-grant_pg_signal_backend_to_privileged_role_opengauss.sql"
            ),
            privileged_role_name = params.privileged_role_name
        )
    } else {
        format!(
            include_str!("./migrations/0012-grant_pg_signal_backend_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        )
    };

    // Add new migrations in numerical order.
    let migrations = [
        migration_0001.as_str(),
        migration_0002.as_str(),
        migration_0003.as_str(),
        migration_0004.as_str(),
        &format!(
            include_str!("./migrations/0005-grant_all_on_tables_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        ),
        &format!(
            include_str!("./migrations/0006-grant_all_on_sequences_to_privileged_role.sql"),
            privileged_role_name = params.privileged_role_name
        ),
        &format!(
            include_str!(
                "./migrations/0007-grant_all_on_tables_with_grant_option_to_privileged_role.sql"
            ),
            privileged_role_name = params.privileged_role_name
        ),
        &format!(
            include_str!(
                "./migrations/0008-grant_all_on_sequences_with_grant_option_to_privileged_role.sql"
            ),
            privileged_role_name = params.privileged_role_name
        ),
        include_str!("./migrations/0009-revoke_replication_for_previously_allowed_roles.sql"),
        migration_0010.as_str(),
        migration_0011.as_str(),
        migration_0012.as_str(),
    ];

    MigrationRunner::new(client, &migrations, lakebase_mode, is_opengauss)
        .run_migrations()
        .await?;

    Ok(())
}
