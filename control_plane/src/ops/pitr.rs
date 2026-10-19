//! Timestamp normalization and strict PITR response validation.
use anyhow::{Context, Result, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use utils::lsn::Lsn;

pub fn normalize_timestamp(value: &str) -> Result<String> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .context("invalid --time: use RFC3339 with a timezone, e.g. 2026-10-12T10:00:00+08:00")?;
    Ok(timestamp
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

pub fn select_lsn(response: &Value) -> Result<Lsn> {
    match response.get("kind").and_then(Value::as_str) {
        Some("present") => {}
        Some("past") => bail!(
            "PITR unavailable: target time is before the retained history or ancestor boundary (kind=past)"
        ),
        Some("future") => bail!(
            "PITR unavailable: target time is after the available commit history (kind=future)"
        ),
        Some("nodata") => {
            bail!("PITR unavailable: no commit timestamps are available (kind=nodata)")
        }
        kind => bail!("invalid PITR response kind: {kind:?}"),
    }
    let lsn: Lsn = response
        .get("lsn")
        .and_then(Value::as_str)
        .context("PITR response is missing lsn")?
        .parse()
        .context("invalid PITR lsn")?;
    if lsn == Lsn(0) {
        bail!("PITR returned a zero LSN");
    }
    Ok(lsn)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timezone_conversion() {
        assert_eq!(
            normalize_timestamp("2026-10-12T10:00:00+08:00").unwrap(),
            "2026-10-12T02:00:00Z"
        );
        assert_eq!(
            normalize_timestamp("2026-10-11T23:30:00-03:00").unwrap(),
            "2026-10-12T02:30:00Z"
        );
        assert_eq!(
            normalize_timestamp("2026-10-12T02:00:00.123456Z").unwrap(),
            "2026-10-12T02:00:00.123456Z"
        );
        assert!(normalize_timestamp("2026-10-12T10:00:00").is_err());
        assert!(normalize_timestamp("2026-02-30T10:00:00Z").is_err());
    }
    #[test]
    fn unavailable_history_never_selects_fallback_lsn() {
        for kind in ["past", "future", "nodata", "unknown"] {
            assert!(select_lsn(&serde_json::json!({"kind": kind, "lsn": "0/1238"})).is_err());
        }
        for value in [
            serde_json::json!({"kind":"present"}),
            serde_json::json!({"kind":"present", "lsn":"bad"}),
            serde_json::json!({"kind":"present", "lsn":"0/0"}),
        ] {
            assert!(select_lsn(&value).is_err());
        }
        assert_eq!(
            select_lsn(&serde_json::json!({"kind":"present", "lsn":"0/1238"})).unwrap(),
            Lsn(0x1238)
        );
    }
}
