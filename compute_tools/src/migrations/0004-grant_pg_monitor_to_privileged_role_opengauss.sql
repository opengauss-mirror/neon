DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_monitor') THEN
        EXECUTE 'GRANT pg_monitor TO {privileged_role_name} WITH ADMIN OPTION';
    END IF;
END $$;
