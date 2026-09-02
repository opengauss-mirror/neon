DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename = 'health_check'
    )
    THEN
    CREATE TABLE health_check (
        id serial primary key,
        updated_at timestamptz default now()
    );
    -- openGauss does not support PostgreSQL's INSERT ... ON CONFLICT syntax.
    -- Use ON DUPLICATE KEY UPDATE, which is the openGauss-native upsert form.
    INSERT INTO health_check VALUES (1, now())
        ON DUPLICATE KEY UPDATE updated_at = now();
    END IF;
END
$$