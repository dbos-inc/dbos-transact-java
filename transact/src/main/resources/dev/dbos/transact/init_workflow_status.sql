CREATE OR REPLACE FUNCTION "dbos".init_workflow_status(
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
  p_authenticated_user TEXT,
  p_assumed_role TEXT,
  p_authenticated_roles TEXT,
  p_executor_id TEXT,
  p_application_version TEXT,
  p_application_id TEXT,
  p_workflow_timeout_ms BIGINT,
  p_workflow_deadline_epoch_ms BIGINT,
  p_parent_workflow_id TEXT,
  p_owner_xid TEXT,
  p_serialization TEXT,
  p_max_retries INTEGER,
  p_is_recovery_request BOOLEAN,
  p_is_dequeued_request BOOLEAN
) RETURNS TABLE (
  workflow_uuid TEXT,
  status TEXT,
  deadline_epoch_ms BIGINT,
  should_execute BOOLEAN,
  serialization TEXT
) AS $$
--
-- NOTE: Transaction behavior differs from original Java implementation.
-- Original Java code would ROLLBACK the insertWorkflowStatus upsert changes when 
-- finding an existing workflow that shouldn't be recovered (wrong owner_xid).
-- This PL/pgSQL version always COMMITs all changes, including any recovery_attempts
-- increments and updated_at timestamps from insert_workflow_status.
-- This could affect workflow recovery counting and metadata for untouched workflows.
--
DECLARE
  v_increment_attempts BOOLEAN := p_is_recovery_request OR p_is_dequeued_request;
  v_insert_result RECORD;
BEGIN
  -- Validate workflow UUID
  IF p_workflow_uuid IS NULL OR p_workflow_uuid = '' THEN
    RAISE EXCEPTION 'Workflow UUID cannot be null or empty';
  END IF;
  
  -- Call insert_workflow_status function
  SELECT * FROM "dbos".insert_workflow_status(
    p_workflow_uuid, p_status, p_inputs, p_name, p_class_name, p_config_name,
    p_queue_name, p_deduplication_id, p_priority, p_queue_partition_key,
    p_authenticated_user, p_assumed_role, p_authenticated_roles,
    p_executor_id, p_application_version, p_application_id,
    p_workflow_timeout_ms, p_workflow_deadline_epoch_ms,
    p_parent_workflow_id, p_owner_xid, p_serialization,
    v_increment_attempts
  ) INTO v_insert_result;
  
  -- Validate workflow metadata matches
  IF v_insert_result.name IS DISTINCT FROM p_name THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different function name: %, but the provided function name is: %', v_insert_result.name, p_name;
  END IF;
  
  IF v_insert_result.class_name IS DISTINCT FROM p_class_name THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class name: %, but the provided class name is: %', v_insert_result.class_name, p_class_name;
  END IF;
  
  IF COALESCE(v_insert_result.config_name, '') IS DISTINCT FROM COALESCE(p_config_name, '') THEN
    RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class configuration: %, but the provided class configuration is: %', v_insert_result.config_name, p_config_name;
  END IF;
  
  -- If there is an existing DB record and we aren't here to recover it, leave it be
  IF v_insert_result.owner_xid != p_owner_xid AND NOT p_is_recovery_request AND NOT p_is_dequeued_request THEN
    IF v_insert_result.status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED' THEN
      RAISE EXCEPTION 'DBOS_MAX_RECOVERY_ATTEMPTS_EXCEEDED: Workflow % exceeded maximum recovery attempts: %', p_workflow_uuid, p_max_retries;
    END IF;
    
    -- Return existing workflow info without executing
    workflow_uuid := p_workflow_uuid;
    status := v_insert_result.status;
    deadline_epoch_ms := v_insert_result.workflow_deadline_epoch_ms;
    should_execute := FALSE;
    serialization := v_insert_result.serialization;
    RETURN NEXT;
    RETURN;
  END IF;
  
  -- Check max retries exceeded
  IF p_max_retries IS NOT NULL AND v_insert_result.recovery_attempts > p_max_retries + 1 THEN
    UPDATE "dbos".workflow_status
    SET 
      status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
      deduplication_id = NULL,
      started_at_epoch_ms = NULL,
      queue_name = NULL
    WHERE workflow_uuid = p_workflow_uuid AND status = 'PENDING';
    
    RAISE EXCEPTION 'DBOS_MAX_RECOVERY_ATTEMPTS_EXCEEDED: Workflow % exceeded maximum recovery attempts: %', p_workflow_uuid, p_max_retries;
  END IF;
  
  -- Return successful result
  workflow_uuid := p_workflow_uuid;
  status := v_insert_result.status;
  deadline_epoch_ms := v_insert_result.workflow_deadline_epoch_ms;
  should_execute := TRUE;
  serialization := v_insert_result.serialization;
  RETURN NEXT;
  
EXCEPTION
  WHEN OTHERS THEN
    -- Re-raise custom exceptions and other errors
    RAISE;
END;
$$ LANGUAGE plpgsql;