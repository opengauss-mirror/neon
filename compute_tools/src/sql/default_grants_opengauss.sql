DO ${outer_tag}$
    BEGIN
        IF EXISTS(
            SELECT nspname
            FROM pg_catalog.pg_namespace
            WHERE nspname = 'public'
        )
        THEN
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {privileged_role_name} WITH GRANT OPTION;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {privileged_role_name} WITH GRANT OPTION;
        END IF;
    END
${outer_tag}$;
