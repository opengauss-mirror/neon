CREATE SCHEMA IF NOT EXISTS oggit;

CREATE TABLE IF NOT EXISTS oggit.state (
    id boolean PRIMARY KEY DEFAULT true,
    tenant_id text NOT NULL,
    timeline_id text NOT NULL,
    database_name text,
    database_oid oid,
    ancestor_timeline_id text,
    branch_start_lsn text NOT NULL,
    slot_name text,
    required_lsn text NOT NULL,
    decode_lsn text NOT NULL,
    scanned_lsn text NOT NULL,
    confirmed_lsn text,
    status text NOT NULL DEFAULT 'active',
    last_error text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (id),
    CHECK (status IN ('active', 'paused', 'failed'))
);

ALTER TABLE oggit.state ADD COLUMN IF NOT EXISTS database_name text;
ALTER TABLE oggit.state ADD COLUMN IF NOT EXISTS database_oid oid;

CREATE TABLE IF NOT EXISTS oggit.change_log (
    id bigserial PRIMARY KEY,
    commit_lsn text NOT NULL,
    record_lsn text,
    xid text,
    merge_id uuid,
    ordinal integer NOT NULL,
    op text NOT NULL,
    schema_name text,
    table_name text,
    relid oid,
    identity_kind text NOT NULL,
    key_json jsonb,
    old_row jsonb,
    new_row jsonb,
    changed_cols text[],
    unsupported_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (op IN (
        'INSERT',
        'UPDATE',
        'DELETE',
        'UNSUPPORTED'
    )),
    CHECK (identity_kind IN (
        'primary_key',
        'unique_key',
        'replica_identity_full',
        'unsupported'
    ))
);

ALTER TABLE oggit.change_log ADD COLUMN IF NOT EXISTS merge_id uuid;

CREATE INDEX IF NOT EXISTS change_log_commit_lsn_idx
    ON oggit.change_log (commit_lsn, ordinal);

CREATE INDEX IF NOT EXISTS change_log_table_key_idx
    ON oggit.change_log (schema_name, table_name);

CREATE INDEX IF NOT EXISTS change_log_merge_id_idx
    ON oggit.change_log (merge_id);

CREATE TABLE IF NOT EXISTS oggit.object_change (
    id bigserial PRIMARY KEY,
    commit_lsn text NOT NULL,
    merge_id uuid,
    ordinal integer NOT NULL,
    object_type text NOT NULL,
    schema_name text,
    object_name text,
    action text NOT NULL,
    change_json jsonb NOT NULL,
    safety_class text NOT NULL,
    unsupported_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (object_type IN (
        'TABLE',
        'COLUMN',
        'INDEX',
        'CONSTRAINT',
        'SEQUENCE',
        'TRIGGER',
        'FUNCTION',
        'VIEW',
        'SCHEMA',
        'TRUNCATE',
        'OTHER'
    )),
    CHECK (safety_class IN (
        'safe_additive',
        'requires_validation',
        'destructive',
        'semantic',
        'unsupported'
    ))
);

ALTER TABLE oggit.object_change ADD COLUMN IF NOT EXISTS merge_id uuid;

ALTER TABLE oggit.object_change
    DROP CONSTRAINT IF EXISTS object_change_object_type_check;
ALTER TABLE oggit.object_change
    ADD CONSTRAINT object_change_object_type_check CHECK (object_type IN (
        'TABLE',
        'COLUMN',
        'INDEX',
        'CONSTRAINT',
        'SEQUENCE',
        'TRIGGER',
        'FUNCTION',
        'VIEW',
        'SCHEMA',
        'TRUNCATE',
        'OTHER'
    ));

CREATE INDEX IF NOT EXISTS object_change_commit_lsn_idx
    ON oggit.object_change (commit_lsn, ordinal);

CREATE INDEX IF NOT EXISTS object_change_merge_id_idx
    ON oggit.object_change (merge_id);

CREATE TABLE IF NOT EXISTS oggit.merge_event_marker (
    id bigserial PRIMARY KEY,
    merge_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS oggit.merge_history (
    merge_id uuid PRIMARY KEY,
    child_timeline_id text NOT NULL,
    parent_timeline_id text NOT NULL,
    base_timeline_id text NOT NULL,
    child_meta_schema text,
    base_lsn text NOT NULL,
    child_from_lsn text NOT NULL,
    child_to_lsn text NOT NULL,
    parent_from_lsn text NOT NULL,
    parent_to_lsn text NOT NULL,
    strategy text NOT NULL,
    status text NOT NULL,
    merge_direction text NOT NULL DEFAULT 'child_to_parent',
    merge_commit_lsn text,
    conflict_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    CHECK (strategy IN ('fail', 'ours', 'theirs', 'manual')),
    CHECK (status IN ('planning', 'blocked', 'failed', 'applied', 'aborted')),
    CHECK (merge_direction IN ('child_to_parent', 'parent_to_child'))
);

ALTER TABLE oggit.merge_history ADD COLUMN IF NOT EXISTS child_meta_schema text;
ALTER TABLE oggit.merge_history ADD COLUMN IF NOT EXISTS merge_direction text NOT NULL DEFAULT 'child_to_parent';

CREATE TABLE IF NOT EXISTS oggit.merge_conflict (
    conflict_id bigserial PRIMARY KEY,
    merge_id uuid NOT NULL REFERENCES oggit.merge_history(merge_id),
    conflict_scope text NOT NULL,
    conflict_type text NOT NULL,
    schema_name text,
    table_name text,
    object_name text,
    ours_op text,
    theirs_op text,
    ours_cols text[],
    theirs_cols text[],
    key_json jsonb,
    base_json jsonb,
    ours_json jsonb,
    theirs_json jsonb,
    reason text NOT NULL,
    resolution text,
    custom_sql text,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (conflict_scope IN ('row', 'ddl', 'sequence', 'truncate', 'unsupported')),
    CHECK (status IN ('pending', 'resolved', 'skipped'))
);

ALTER TABLE oggit.merge_conflict ADD COLUMN IF NOT EXISTS ours_op text;
ALTER TABLE oggit.merge_conflict ADD COLUMN IF NOT EXISTS theirs_op text;
ALTER TABLE oggit.merge_conflict ADD COLUMN IF NOT EXISTS ours_cols text[];
ALTER TABLE oggit.merge_conflict ADD COLUMN IF NOT EXISTS theirs_cols text[];
ALTER TABLE oggit.merge_conflict ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

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
