local RAW_MIN_SUPPORTED_VERSION = 9;
local CANONICAL_MIN_SUPPORTED_VERSION = 14;
local MAX_SUPPORTED_VERSION = 17;
local SUPPORTED_VERSIONS =
  std.range(RAW_MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION);

# If we receive the pg_version with a leading "v", ditch it.
local pg_version = std.strReplace(std.extVar('pg_version'), 'v', '');
local pg_version_num = std.parseInt(pg_version);

# Allow legacy major versions (9–13) but map them to the lowest supported
# canonical version so downstream consumers (paths, image selection, etc.)
# stay consistent with PG14 layouts.
assert std.setMember(pg_version_num, SUPPORTED_VERSIONS) :
       std.format('%s is an unsupported Postgres version: %s',
                  [pg_version, std.toString(SUPPORTED_VERSIONS)]);
local effective_pg_version_num =
  if pg_version_num < CANONICAL_MIN_SUPPORTED_VERSION
  then CANONICAL_MIN_SUPPORTED_VERSION
  else pg_version_num;

{
  PG_MAJORVERSION: std.toString(effective_pg_version_num),
  PG_MAJORVERSION_NUM: effective_pg_version_num,
}
