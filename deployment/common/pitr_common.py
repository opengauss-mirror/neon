"""Shared PITR timestamp, shard, LSN and lease validation for deployment CLIs."""
import re


def pitr_timestamp(value):
    """Accept explicit RFC3339 zones; pageserver's parser requires UTC Z."""
    import datetime
    pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?P<fraction>\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})"
    match = re.fullmatch(pattern, value)
    if not match:
        raise ValueError("invalid --time: use RFC3339 with a timezone, e.g. 2026-10-12T10:00:00+08:00")
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        utc = parsed.astimezone(datetime.timezone.utc)
    except ValueError as exc:
        raise ValueError("invalid --time: " + str(exc)) from exc
    fraction = (match.group("fraction") or "").rstrip("0").rstrip(".")
    return utc.strftime("%Y-%m-%dT%H:%M:%S") + fraction + "Z"


def pitr_select_lsn(response):
    reasons = {
        "past": "target time is before the retained history or ancestor boundary",
        "future": "target time is after the available commit history",
        "nodata": "no commit timestamps are available",
    }
    kind = response.get("kind")
    if kind != "present":
        raise RuntimeError("PITR unavailable: %s (kind=%s)" % (reasons.get(kind, "invalid response"), kind))
    lsn = response.get("lsn")
    if not isinstance(lsn, str) or not re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", lsn):
        raise RuntimeError("invalid or missing PITR lsn")
    if int(lsn.replace("/", ""), 16) == 0:
        raise RuntimeError("PITR returned a zero LSN")
    return lsn


def pitr_shard_zero(shards, tenant_id):
    if not re.fullmatch(r"[0-9a-fA-F]{32}", tenant_id):
        raise RuntimeError("invalid PITR tenant id")
    for shard in shards:
        shard_id = str(shard.get("shard_id", ""))
        if shard_id == tenant_id or (
            re.fullmatch(re.escape(tenant_id) + r"-00[0-9a-fA-F]{2}", shard_id)
        ):
            return shard
    raise RuntimeError("tenant has no attached shard zero")


def pitr_check_lease(response):
    import datetime
    value = response.get("valid_until")
    if not isinstance(value, str):
        raise RuntimeError("pageserver did not grant an LSN lease; recovery branch was not created")
    valid_until = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if valid_until.tzinfo is None or valid_until <= datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10):
        raise RuntimeError("pageserver returned an expired or nearly expired LSN lease")
