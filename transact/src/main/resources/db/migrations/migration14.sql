CREATE FUNCTION "%1$s".enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INTEGER DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL,
    parent_workflow_id TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INTEGER := 0;
    v_priority INTEGER;
    v_existing_name TEXT;
    v_existing_class_name TEXT;
    v_existing_config_name TEXT;
BEGIN

    -- Validate required parameters
    IF enqueue_workflow.workflow_name IS NULL OR enqueue_workflow.workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF enqueue_workflow.queue_name IS NULL OR enqueue_workflow.queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF enqueue_workflow.named_args IS NOT NULL AND jsonb_typeof(enqueue_workflow.named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF enqueue_workflow.workflow_id IS NOT NULL AND enqueue_workflow.workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;

    v_workflow_id := COALESCE(enqueue_workflow.workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(enqueue_workflow.priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', enqueue_workflow.positional_args,
        'namedArgs', enqueue_workflow.named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;

    INSERT INTO "%1$s".workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization
    ) VALUES (
        v_workflow_id, 'ENQUEUED', v_serialized_inputs,
        enqueue_workflow.workflow_name, enqueue_workflow.class_name, enqueue_workflow.config_name,
        enqueue_workflow.queue_name, enqueue_workflow.deduplication_id, v_priority, enqueue_workflow.queue_partition_key,
        enqueue_workflow.app_version,
        v_now, v_now, v_recovery_attempts,
        enqueue_workflow.timeout_ms, enqueue_workflow.deadline_epoch_ms,
        enqueue_workflow.parent_workflow_id, v_owner_xid, 'portable_json'
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at
    RETURNING workflow_status.name, workflow_status.class_name, workflow_status.config_name
    INTO v_existing_name, v_existing_class_name, v_existing_config_name;

    -- Validate workflow metadata matches
    IF v_existing_name IS DISTINCT FROM enqueue_workflow.workflow_name THEN
        RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different function name: %%s, but the provided function name is: %%s', v_existing_name, enqueue_workflow.workflow_name;
    END IF;
    IF v_existing_class_name IS DISTINCT FROM enqueue_workflow.class_name THEN
        RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class name: %%s, but the provided class name is: %%s', v_existing_class_name, enqueue_workflow.class_name;
    END IF;
    IF v_existing_config_name IS DISTINCT FROM enqueue_workflow.config_name THEN
        RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class configuration: %%s, but the provided class configuration is: %%s', v_existing_config_name, enqueue_workflow.config_name;
    END IF;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        IF SQLERRM LIKE 'deduplication_id' THEN
            RAISE EXCEPTION 'DBOS_QUEUE_DUPLICATED: Workflow %%s with queue %%s and deduplication ID %%s already exists', v_workflow_id, COALESCE(enqueue_workflow.queue_name, ''), COALESCE(enqueue_workflow.deduplication_id, '');
        END IF;
        RAISE;
    WHEN OTHERS THEN
        RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION "%1$s".send_message(
    destination_id TEXT,
    message JSON,
    topic TEXT DEFAULT NULL,
    message_id TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_topic TEXT := COALESCE(topic, '__null__topic__');
    v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
BEGIN
    INSERT INTO "%1$s".notifications (
        destination_uuid, topic, message, message_uuid, serialization
    ) VALUES (
        destination_id, v_topic, message, v_message_id, 'portable_json'
    )
    ON CONFLICT (message_uuid) DO NOTHING;
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'DBOSNonExistentWorkflowException: Destination workflow %%s does not exist', destination_id;
END;
$$ LANGUAGE plpgsql;