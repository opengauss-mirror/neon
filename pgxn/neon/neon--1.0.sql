\echo Use "CREATE EXTENSION neon" to load this file. \quit

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

CREATE OR REPLACE FUNCTION neon_branch_cleanup_source(
    fdw_schema name,
    fdw_server name
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', fdw_server);
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', fdw_schema);
END;
$$;

CREATE OR REPLACE FUNCTION neon_branch_prepare_source(
    fdw_schema name,
    fdw_server name,
    source_host text,
    source_port integer,
    source_database text,
    source_user text,
    source_schema name
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM neon_branch_cleanup_source(fdw_schema, fdw_server);

    EXECUTE format('CREATE SCHEMA %I', fdw_schema);
    EXECUTE format(
        'CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
        fdw_server,
        source_host,
        source_port::text,
        source_database
    );
    EXECUTE format(
        'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L)',
        fdw_server,
        source_user
    );
    -- openGauss postgres_fdw does not support PostgreSQL's
    -- IMPORT FOREIGN SCHEMA syntax. neon_local creates the foreign
    -- tables explicitly after this function prepares the FDW server.
END;
$$;

CREATE OR REPLACE FUNCTION neon_branch_diff(
    source_schema name,
    target_schema name,
    include_tables name[] DEFAULT NULL
) RETURNS TABLE (
    schema_name text,
    table_name text,
    diff_type text,
    row_data text
)
LANGUAGE plpgsql
AS $$
DECLARE
    tbl record;
    src_rel oid;
    tgt_rel oid;
    src_sig text;
    tgt_sig text;
    pk_cond text;
    val_equal_cond text;
BEGIN
    FOR tbl IN
        SELECT relname
        FROM (
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = source_schema::text
              AND c.relkind IN ('r', 'f')
            UNION
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = target_schema::text
              AND c.relkind = 'r'
        ) names
        WHERE include_tables IS NULL OR relname = ANY(include_tables)
        ORDER BY relname
    LOOP
        SELECT max(c.oid)
        INTO src_rel
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = source_schema::text
          AND c.relname = tbl.relname
          AND c.relkind IN ('r', 'f');

        SELECT max(c.oid)
        INTO tgt_rel
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = target_schema::text
          AND c.relname = tbl.relname
          AND c.relkind = 'r';

        IF src_rel IS NULL THEN
            RETURN QUERY EXECUTE format(
                'SELECT %L::text, %L::text, %L::text, row_to_json(t)::text FROM %I.%I t',
                target_schema::text,
                tbl.relname,
                'target_only',
                target_schema,
                tbl.relname
            );
            CONTINUE;
        END IF;

        IF tgt_rel IS NULL THEN
            RETURN QUERY EXECUTE format(
                'SELECT %L::text, %L::text, %L::text, row_to_json(s)::text FROM %I.%I s',
                target_schema::text,
                tbl.relname,
                'source_only',
                source_schema,
                tbl.relname
            );
            CONTINUE;
        END IF;

        SELECT string_agg(attname || ':' || atttypid || ':' || atttypmod || ':' || attnotnull, ',' ORDER BY attnum)
        INTO src_sig
        FROM pg_attribute
        WHERE attrelid = src_rel AND attnum > 0 AND NOT attisdropped;

        SELECT string_agg(attname || ':' || atttypid || ':' || atttypmod || ':' || attnotnull, ',' ORDER BY attnum)
        INTO tgt_sig
        FROM pg_attribute
        WHERE attrelid = tgt_rel AND attnum > 0 AND NOT attisdropped;

        IF src_sig IS DISTINCT FROM tgt_sig THEN
            schema_name := target_schema::text;
            table_name := tbl.relname;
            diff_type := 'schema_mismatch';
            row_data := 'source=' || COALESCE(src_sig, '') || ' target=' || COALESCE(tgt_sig, '');
            RETURN NEXT;
            CONTINUE;
        END IF;

        SELECT string_agg(format('t.%I IS NOT DISTINCT FROM s.%I', a.attname, a.attname), ' AND ' ORDER BY a.attnum)
        INTO pk_cond
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = tgt_rel AND i.indisprimary;

        IF pk_cond IS NULL THEN
            schema_name := target_schema::text;
            table_name := tbl.relname;
            diff_type := 'no_primary_key';
            row_data := NULL;
            RETURN NEXT;
            CONTINUE;
        END IF;

        SELECT COALESCE(string_agg(format('t.%I IS NOT DISTINCT FROM s.%I', a.attname, a.attname), ' AND ' ORDER BY a.attnum), 'TRUE')
        INTO val_equal_cond
        FROM pg_attribute a
        WHERE a.attrelid = tgt_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND NOT EXISTS (
              SELECT 1
              FROM pg_index i
              WHERE i.indrelid = tgt_rel
                AND i.indisprimary
                AND a.attnum = ANY(i.indkey)
          );

        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, %L::text, row_to_json(s)::text
             FROM %I.%I s
             WHERE NOT EXISTS (SELECT 1 FROM %I.%I t WHERE %s)',
            target_schema::text,
            tbl.relname,
            'source_only',
            source_schema,
            tbl.relname,
            target_schema,
            tbl.relname,
            pk_cond
        );

        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, %L::text, row_to_json(t)::text
             FROM %I.%I t
             WHERE NOT EXISTS (SELECT 1 FROM %I.%I s WHERE %s)',
            target_schema::text,
            tbl.relname,
            'target_only',
            target_schema,
            tbl.relname,
            source_schema,
            tbl.relname,
            pk_cond
        );

        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, %L::text,
                    (''source='' || row_to_json(s)::text || '' target='' || row_to_json(t)::text)
             FROM %I.%I t
             JOIN %I.%I s ON %s
             WHERE NOT (%s)',
            target_schema::text,
            tbl.relname,
            'conflict',
            target_schema,
            tbl.relname,
            source_schema,
            tbl.relname,
            pk_cond,
            val_equal_cond
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION neon_branch_merge(
    source_schema name,
    target_schema name,
    strategy text DEFAULT 'fail',
    include_tables name[] DEFAULT NULL,
    copy_source_only_tables boolean DEFAULT true
) RETURNS TABLE (
    schema_name text,
    table_name text,
    inserted_count bigint,
    updated_count bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    tbl record;
    src_rel oid;
    tgt_rel oid;
    src_sig text;
    tgt_sig text;
    pk_cond text;
    val_equal_cond text;
    all_cols text;
    select_cols text;
    update_set text;
    has_conflict boolean;
    copied_count bigint;
BEGIN
    strategy := lower(strategy);
    IF strategy NOT IN ('fail', 'ours', 'theirs') THEN
        RAISE EXCEPTION 'invalid merge strategy %, expected fail, ours, or theirs', strategy;
    END IF;

    FOR tbl IN
        SELECT s.relname
        FROM pg_class s
        JOIN pg_namespace sn ON sn.oid = s.relnamespace
        WHERE sn.nspname = source_schema::text
          AND s.relkind IN ('r', 'f')
          AND (include_tables IS NULL OR s.relname = ANY(include_tables))
        ORDER BY s.relname
    LOOP
        SELECT max(c.oid)
        INTO src_rel
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = source_schema::text
          AND c.relname = tbl.relname
          AND c.relkind IN ('r', 'f');

        SELECT max(c.oid)
        INTO tgt_rel
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = target_schema::text
          AND c.relname = tbl.relname
          AND c.relkind = 'r';
        inserted_count := 0;
        updated_count := 0;

        IF tgt_rel IS NULL THEN
            IF NOT copy_source_only_tables THEN
                RAISE EXCEPTION 'target table %.% does not exist', target_schema, tbl.relname;
            END IF;

            EXECUTE format(
                'CREATE TABLE %I.%I AS SELECT * FROM %I.%I WHERE false',
                target_schema,
                tbl.relname,
                source_schema,
                tbl.relname
            );
            EXECUTE format(
                'INSERT INTO %I.%I SELECT * FROM %I.%I',
                target_schema,
                tbl.relname,
                source_schema,
                tbl.relname
            );
            GET DIAGNOSTICS copied_count = ROW_COUNT;

            schema_name := target_schema::text;
            table_name := tbl.relname;
            inserted_count := copied_count;
            updated_count := 0;
            RETURN NEXT;
            CONTINUE;
        END IF;

        SELECT string_agg(attname || ':' || atttypid || ':' || atttypmod || ':' || attnotnull, ',' ORDER BY attnum)
        INTO src_sig
        FROM pg_attribute
        WHERE attrelid = src_rel AND attnum > 0 AND NOT attisdropped;

        SELECT string_agg(attname || ':' || atttypid || ':' || atttypmod || ':' || attnotnull, ',' ORDER BY attnum)
        INTO tgt_sig
        FROM pg_attribute
        WHERE attrelid = tgt_rel AND attnum > 0 AND NOT attisdropped;

        IF src_sig IS DISTINCT FROM tgt_sig THEN
            RAISE EXCEPTION 'schema mismatch for table %.%', target_schema, tbl.relname;
        END IF;

        SELECT string_agg(format('t.%I IS NOT DISTINCT FROM s.%I', a.attname, a.attname), ' AND ' ORDER BY a.attnum)
        INTO pk_cond
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = tgt_rel AND i.indisprimary;

        IF pk_cond IS NULL THEN
            RAISE EXCEPTION 'table %.% has no primary key', target_schema, tbl.relname;
        END IF;

        SELECT COALESCE(string_agg(format('t.%I IS NOT DISTINCT FROM s.%I', a.attname, a.attname), ' AND ' ORDER BY a.attnum), 'TRUE')
        INTO val_equal_cond
        FROM pg_attribute a
        WHERE a.attrelid = tgt_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND NOT EXISTS (
              SELECT 1
              FROM pg_index i
              WHERE i.indrelid = tgt_rel
                AND i.indisprimary
                AND a.attnum = ANY(i.indkey)
          );

        SELECT string_agg(format('%I', a.attname), ', ' ORDER BY a.attnum),
               string_agg(format('s.%I', a.attname), ', ' ORDER BY a.attnum)
        INTO all_cols, select_cols
        FROM pg_attribute a
        WHERE a.attrelid = tgt_rel AND a.attnum > 0 AND NOT a.attisdropped;

        SELECT string_agg(format('%I = s.%I', a.attname, a.attname), ', ' ORDER BY a.attnum)
        INTO update_set
        FROM pg_attribute a
        WHERE a.attrelid = tgt_rel
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND NOT EXISTS (
              SELECT 1
              FROM pg_index i
              WHERE i.indrelid = tgt_rel
                AND i.indisprimary
                AND a.attnum = ANY(i.indkey)
          );

        EXECUTE format('LOCK TABLE %I.%I IN SHARE ROW EXCLUSIVE MODE', target_schema, tbl.relname);

        IF strategy = 'fail' THEN
            EXECUTE format(
                'SELECT EXISTS (
                    SELECT 1
                    FROM %I.%I t
                    JOIN %I.%I s ON %s
                    WHERE NOT (%s)
                )',
                target_schema,
                tbl.relname,
                source_schema,
                tbl.relname,
                pk_cond,
                val_equal_cond
            )
            INTO has_conflict;

            IF has_conflict THEN
                RAISE EXCEPTION 'merge conflict in table %.%', target_schema, tbl.relname;
            END IF;
        END IF;

        EXECUTE format(
            'INSERT INTO %I.%I (%s)
             SELECT %s
             FROM %I.%I s
             WHERE NOT EXISTS (SELECT 1 FROM %I.%I t WHERE %s)',
            target_schema,
            tbl.relname,
            all_cols,
            select_cols,
            source_schema,
            tbl.relname,
            target_schema,
            tbl.relname,
            pk_cond
        );
        GET DIAGNOSTICS inserted_count = ROW_COUNT;

        IF strategy = 'theirs' AND update_set IS NOT NULL THEN
            EXECUTE format(
                'UPDATE %I.%I t
                 SET %s
                 FROM %I.%I s
                 WHERE %s
                   AND NOT (%s)',
                target_schema,
                tbl.relname,
                update_set,
                source_schema,
                tbl.relname,
                pk_cond,
                val_equal_cond
            );
            GET DIAGNOSTICS updated_count = ROW_COUNT;
        END IF;

        schema_name := target_schema::text;
        table_name := tbl.relname;
        RETURN NEXT;
    END LOOP;
END;
$$;
