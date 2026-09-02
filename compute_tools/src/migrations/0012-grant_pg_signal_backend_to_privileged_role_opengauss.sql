DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_signal_backend') THEN
        EXECUTE 'GRANT pg_signal_backend TO {privileged_role_name} WITH ADMIN OPTION';
    END IF;
END $$;
