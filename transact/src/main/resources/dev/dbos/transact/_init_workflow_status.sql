CREATE FUNCTION "dbos"._init_workflow_status(
  p_workflow_uuid TEXT,
  p_status TEXT,
  p_inputs TEXT,
  p_name TEXT,
  p_class_name TEXT,
  p_config_name TEXT,
  p_queue_name TEXT,
  p_deduplication_id TEXT,
  p_priority INTEGER,
  p_queue_partition_key TEXT,
  p_application_version TEXT,
  p_workflow_timeout_ms BIGINT,
  p_workflow_deadline_epoch_ms BIGINT,
  p_parent_workflow_id TEXT,
  p_serialization TEXT
) RETURNS VOID AS $$
--
-- INTERNAL FUNCTION: Only intended to be called by send_message and enqueue_portable_workflow.
-- The underscore prefix indicates this is an internal implementation detail.
--
DECLARE
  v_owner_xid TEXT := gen_random_uuid()::TEXT;
  v_now BIGINT := EXTRACT(epoch FROM now()) * 1000;
  v_recovery_attempts INTEGER := CASE WHEN p_status = 'ENQUEUED' THEN 0 ELSE 1 END;
  v_priority INTEGER := COALESCE(p_priority, 0);
  -- Variables to validate existing workflow metadata
  v_existing_name TEXT;
  v_existing_class_name TEXT;
  v_existing_config_name TEXT;
BEGIN
  -- Validate workflow UUID
  IF p_workflow_uuid IS NULL OR p_workflow_uuid = '' THEN
    RAISE EXCEPTION 'Workflow UUID cannot be null or empty';
  END IF;
  
  -- Validate workflow name
  IF p_name IS NULL OR p_name = '' THEN
    RAISE EXCEPTION 'Workflow name cannot be null or empty';
  END IF;
  
  -- Validate status is one of the allowed values
  IF p_status NOT IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED', 'ENQUEUED') THEN
    RAISE EXCEPTION 'Invalid status: %. Status must be one of: PENDING, SUCCESS, ERROR, MAX_RECOVERY_ATTEMPTS_EXCEEDED, CANCELLED, ENQUEUED', p_status;
  END IF;
  
  -- Atomic insert with conflict resolution
  INSERT INTO "dbos".workflow_status (
    workflow_uuid, status, inputs,
    name, class_name, config_name,
    queue_name, deduplication_id, priority, queue_partition_key,
    application_version,
    created_at, updated_at, recovery_attempts,
    workflow_timeout_ms, workflow_deadline_epoch_ms,
    parent_workflow_id, owner_xid, serialization
  ) VALUES (
    p_workflow_uuid, p_status, p_inputs,
    p_name, p_class_name, p_config_name,
    p_queue_name, p_deduplication_id, v_priority, p_queue_partition_key,
    p_application_version,
    v_now, v_now, v_recovery_attempts,
    p_workflow_timeout_ms, p_workflow_deadline_epoch_ms,
    p_parent_workflow_id, v_owner_xid, p_serialization
  )
  ON CONFLICT (workflow_uuid)
  DO UPDATE SET
    updated_at = EXCLUDED.updated_at
  RETURNING name, class_name, config_name
  INTO v_existing_name, v_existing_class_name, v_existing_config_name;

  -- Validate workflow metadata matches
  IF v_existing_name IS DISTINCT FROM p_name THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different function name: %, but the provided function name is: %', v_existing_name, p_name;
  END IF;
  
  IF v_existing_class_name IS DISTINCT FROM p_class_name THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class name: %, but the provided class name is: %', v_existing_class_name, p_class_name;
  END IF;
  
  IF v_existing_config_name IS DISTINCT FROM p_config_name THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class configuration: %, but the provided class configuration is: %', v_existing_config_name, p_config_name;
  END IF;
  
EXCEPTION
  WHEN unique_violation THEN
    -- Handle duplicate deduplication_id
    IF SQLERRM LIKE '%deduplication_id%' THEN
      RAISE EXCEPTION 'DBOS_QUEUE_DUPLICATED: Workflow % with queue % and deduplication ID % already exists', p_workflow_uuid, COALESCE(p_queue_name, ''), COALESCE(p_deduplication_id, '');
    END IF;
    RAISE;
  WHEN OTHERS THEN
    -- Re-raise custom exceptions and other errors
    RAISE;
END;
$$ LANGUAGE plpgsql;