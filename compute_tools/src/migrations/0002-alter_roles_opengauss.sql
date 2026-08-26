-- openGauss does not support PostgreSQL's BYPASSRLS/NOBYPASSRLS role
-- options. Keep the INHERIT repair from the PostgreSQL migration, and skip
-- the NOBYPASSRLS repair because the role option cannot exist here.

DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN
        SELECT rolname
          FROM pg_roles
         WHERE pg_has_role(rolname, '{privileged_role_name}', 'member')
           AND rolname <> '{privileged_role_name}'
    LOOP
        RAISE NOTICE 'EXECUTING ALTER ROLE % INHERIT', quote_ident(role_name);
        EXECUTE 'ALTER ROLE ' || quote_ident(role_name) || ' INHERIT';
    END LOOP;
END $$;
