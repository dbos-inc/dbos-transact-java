DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_schema = '%1$s'
        AND table_name = 'notifications'
        AND constraint_type = 'PRIMARY KEY'
    ) THEN
        ALTER TABLE "%1$s".notifications ADD PRIMARY KEY (message_uuid);
    END IF;
END $$;