CREATE SCHEMA IF NOT EXISTS dbos;

CREATE TABLE dbos.workflow_status (
    workflow_uuid TEXT NOT NULL,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    executor_id TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
    application_version TEXT,
    application_id TEXT,
    class_name CHARACTER VARYING(255) DEFAULT NULL::character varying,
    config_name CHARACTER VARYING(255) DEFAULT NULL::character varying,
    recovery_attempts BIGINT DEFAULT '0'::bigint,
    queue_name TEXT,

    -- Primary key constraint
    CONSTRAINT workflow_status_pkey PRIMARY KEY (workflow_uuid)
);


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


CREATE TABLE dbos.workflow_inputs (
    workflow_uuid TEXT NOT NULL,
    inputs TEXT NOT NULL,

    -- Primary key constraint
    CONSTRAINT workflow_inputs_pkey PRIMARY KEY (workflow_uuid),

    -- Foreign key constraint
    CONSTRAINT workflow_inputs_workflow_uuid_foreign
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