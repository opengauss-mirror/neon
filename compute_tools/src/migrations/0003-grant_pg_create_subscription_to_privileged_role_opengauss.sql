DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_create_subscription') THEN
        EXECUTE 'GRANT pg_create_subscription TO {privileged_role_name}';
    END IF;
END $$;
