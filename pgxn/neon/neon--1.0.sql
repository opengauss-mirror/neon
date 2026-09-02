\echo Use "CREATE EXTENSION neon" to load this file. \quit

CREATE FUNCTION neon_start_oggit_worker()
RETURNS boolean
AS 'MODULE_PATHNAME', 'neon_start_oggit_worker'
LANGUAGE C STRICT;

CREATE FUNCTION neon_oggit_worker_is_ready()
RETURNS boolean
AS 'MODULE_PATHNAME', 'neon_oggit_worker_is_ready'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION neon_start_oggit_worker() FROM PUBLIC;
REVOKE ALL ON FUNCTION neon_oggit_worker_is_ready() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION neon_start_oggit_worker() TO cloud_admin;
GRANT EXECUTE ON FUNCTION neon_oggit_worker_is_ready() TO cloud_admin;

CREATE FUNCTION pg_cluster_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_cluster_size'
LANGUAGE C STRICT;

CREATE FUNCTION backpressure_lsns(
    OUT received_lsn pg_lsn,
    OUT disk_consistent_lsn pg_lsn,
    OUT remote_consistent_lsn pg_lsn
)
RETURNS record
AS 'MODULE_PATHNAME', 'backpressure_lsns'
LANGUAGE C STRICT;

CREATE FUNCTION backpressure_throttling_time()
RETURNS bigint
AS 'MODULE_PATHNAME', 'backpressure_throttling_time'
LANGUAGE C STRICT;

CREATE FUNCTION local_cache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'local_cache_pages'
LANGUAGE C;

CREATE VIEW local_cache AS
	SELECT P.* FROM local_cache_pages() AS P
	(pageoffs int8, relfilenode oid, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, accesscount int4);

CREATE FUNCTION neon_get_lfc_stats()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_lfc_stats'
LANGUAGE C;

CREATE VIEW neon_lfc_stats AS
	SELECT P.* FROM neon_get_lfc_stats() AS P (lfc_key text, lfc_value bigint);

CREATE OR REPLACE VIEW NEON_STAT_FILE_CACHE AS 
   WITH lfc_stats AS (
   SELECT 
     stat_name, 
     count
   FROM neon_get_lfc_stats() AS t(stat_name text, count bigint)
   ),
   lfc_values AS (
   SELECT 
     MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE NULL END) AS file_cache_misses,
     MAX(CASE WHEN stat_name = 'file_cache_hits'   THEN count ELSE NULL END) AS file_cache_hits,
     MAX(CASE WHEN stat_name = 'file_cache_used'   THEN count ELSE NULL END) AS file_cache_used,
     MAX(CASE WHEN stat_name = 'file_cache_writes' THEN count ELSE NULL END) AS file_cache_writes,
     CASE 
        WHEN MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE 0 END) + MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END) = 0 THEN NULL
        ELSE ROUND((MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END)::DECIMAL / 
        (MAX(CASE WHEN stat_name = 'file_cache_hits' THEN count ELSE 0 END) + MAX(CASE WHEN stat_name = 'file_cache_misses' THEN count ELSE 0 END))) * 100, 2)
     END AS file_cache_hit_ratio
   FROM lfc_stats
   )
SELECT file_cache_misses, file_cache_hits, file_cache_used, file_cache_writes, file_cache_hit_ratio from lfc_values;

CREATE FUNCTION approximate_working_set_size(reset bool)
RETURNS integer
AS 'MODULE_PATHNAME', 'approximate_working_set_size'
LANGUAGE C;

CREATE FUNCTION approximate_working_set_size_seconds(duration integer default null)
RETURNS integer
AS 'MODULE_PATHNAME', 'approximate_working_set_size_seconds'
LANGUAGE C;

CREATE FUNCTION get_backend_perf_counters()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_backend_perf_counters'
LANGUAGE C;

CREATE FUNCTION get_perf_counters()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_perf_counters'
LANGUAGE C;

CREATE VIEW neon_backend_perf_counters AS
  SELECT P.procno, P.pid, P.metric, P.bucket_le, P.value
  FROM get_backend_perf_counters() AS P (
    procno integer,
    pid bigint,
    metric text,
    bucket_le float8,
    value float8
  );

CREATE VIEW neon_perf_counters AS
  SELECT P.metric, P.bucket_le, P.value
  FROM get_perf_counters() AS P (
    metric text,
    bucket_le float8,
    value float8
  );

CREATE FUNCTION get_prewarm_info(out total_pages integer, out prewarmed_pages integer, out skipped_pages integer, out active_workers integer)
RETURNS record
AS 'MODULE_PATHNAME', 'get_prewarm_info'
LANGUAGE C STRICT;

CREATE FUNCTION get_local_cache_state(max_chunks integer default null)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_local_cache_state'
LANGUAGE C;

CREATE FUNCTION prewarm_local_cache(state bytea, n_workers integer default 1)
RETURNS void
AS 'MODULE_PATHNAME', 'prewarm_local_cache'
LANGUAGE C STRICT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE proname = 'postgres_fdw_handler'
          AND pg_function_is_visible(oid)
    ) THEN
        EXECUTE $fdw$
            CREATE FUNCTION postgres_fdw_handler()
            RETURNS fdw_handler
            AS '$libdir/postgres_fdw', 'postgres_fdw_handler'
            LANGUAGE C STRICT
        $fdw$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc
        WHERE proname = 'postgres_fdw_validator'
          AND pg_function_is_visible(oid)
    ) THEN
        EXECUTE $fdw$
            CREATE FUNCTION postgres_fdw_validator(text[], oid)
            RETURNS void
            AS '$libdir/postgres_fdw', 'postgres_fdw_validator'
            LANGUAGE C STRICT
        $fdw$;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_foreign_data_wrapper
        WHERE fdwname = 'postgres_fdw'
    ) THEN
        EXECUTE $fdw$
            CREATE FOREIGN DATA WRAPPER postgres_fdw
                HANDLER postgres_fdw_handler
                VALIDATOR postgres_fdw_validator
        $fdw$;
    END IF;
END;
$$;

