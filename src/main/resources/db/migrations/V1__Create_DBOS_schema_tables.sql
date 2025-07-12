CREATE SCHEMA IF NOT EXISTS dbos;

CREATE TABLE dbos.workflow_status (
    workflow_uuid text NOT NULL PRIMARY KEY,
    status text,
    name text,
    authenticated_user text,
    assumed_role text,
    authenticated_roles text,
    request text,
    output text,
    error text,
    executor_id text,
    created_at bigint NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    updated_at bigint NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    application_version text,
    application_id text,
    class_name character varying(255) DEFAULT NULL::character varying,
    config_name character varying(255) DEFAULT NULL::character varying,
    recovery_attempts bigint DEFAULT '0'::bigint,
    queue_name text,
    workflow_timeout_ms bigint,
    workflow_deadline_epoch_ms bigint,
    inputs text,
    started_at_epoch_ms bigint,
    deduplication_id text,
    priority integer NOT NULL DEFAULT 0,
    UNIQUE (queue_name, deduplication_id)
);

CREATE INDEX workflow_status_created_at_index ON dbos.workflow_status USING btree (created_at);
CREATE INDEX workflow_status_executor_id_index ON dbos.workflow_status USING btree (executor_id);
CREATE INDEX workflow_status_status_index ON dbos.workflow_status USING btree (status);


CREATE TABLE dbos.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    output TEXT,
    error TEXT,
    function_name TEXT NOT NULL DEFAULT ''::text,
    child_workflow_id TEXT,

    -- Primary key constraint
    CONSTRAINT operation_outputs_pkey PRIMARY KEY (workflow_uuid, function_id),

    -- Foreign key constraint
    CONSTRAINT operation_outputs_workflow_uuid_foreign
        FOREIGN KEY (workflow_uuid)
        REFERENCES dbos.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE TABLE dbos.notifications (
    destination_uuid text NOT NULL,
    topic text,
    message text NOT NULL,
    created_at_epoch_ms bigint NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    message_uuid text,

    -- Primary key constraint
    CONSTRAINT notifications_pkey PRIMARY KEY (message_uuid),

    -- Foreign key constraint
    CONSTRAINT notifications_destination_uuid_foreign
        FOREIGN KEY (destination_uuid)
        REFERENCES dbos.workflow_status(workflow_uuid)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE INDEX idx_workflow_topic ON dbos.notifications (destination_uuid, topic);

CREATE TABLE dbos.workflow_events (
    workflow_uuid text NOT NULL,
    key text NOT NULL,
    value text NOT NULL,

    -- Primary key constraint (composite key)
    CONSTRAINT workflow_events_pkey PRIMARY KEY (workflow_uuid, key),

    -- Foreign key constraint
    CONSTRAINT workflow_events_workflow_uuid_foreign
        FOREIGN KEY (workflow_uuid)
        REFERENCES dbos.workflow_status(workflow_uuid)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- Create function for notifications trigger
CREATE OR REPLACE FUNCTION dbos.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for notifications
CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON dbos.notifications
FOR EACH ROW EXECUTE FUNCTION dbos.notifications_function();

-- Create function for workflow events trigger
CREATE OR REPLACE FUNCTION dbos.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for workflow events
CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON dbos.workflow_events
FOR EACH ROW EXECUTE FUNCTION dbos.workflow_events_function();