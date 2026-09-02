DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{privileged_role_name}')
        THEN
            -- openGauss does not support the BYPASSRLS role option, nor the
            -- predefined pg_read_all_data / pg_write_all_data roles that vanilla
            -- Postgres grants here. Create the privileged role without them; the
            -- role still gets CREATEDB/CREATEROLE/REPLICATION which is what the
            -- Neon control plane relies on.
            CREATE ROLE {privileged_role_name} CREATEDB CREATEROLE NOLOGIN REPLICATION;
        END IF;
    END
$$;