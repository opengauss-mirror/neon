DO ${outer_tag}$
    DECLARE
        schema_owner TEXT;
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
        )
        THEN
            SELECT r.rolname
            FROM pg_catalog.pg_namespace n
            JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
            WHERE n.nspname = 'public'
            INTO schema_owner;

            IF schema_owner = 'cloud_admin' OR schema_owner = 'zenith_admin'
            THEN
                EXECUTE format('ALTER SCHEMA public OWNER TO %I', {db_owner});
            END IF;
        END IF;
    END
${outer_tag}$;
