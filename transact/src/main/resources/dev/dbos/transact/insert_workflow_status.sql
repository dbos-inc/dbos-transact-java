CREATE FUNCTION "dbos".insert_workflow_status(
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
  p_serialization TEXT
) RETURNS TABLE (
  recovery_attempts INTEGER,
  status TEXT,
  name TEXT,
  class_name TEXT,
  config_name TEXT,
  queue_name TEXT,
  workflow_timeout_ms BIGINT,
  workflow_deadline_epoch_ms BIGINT,
  serialization TEXT,
  owner_xid TEXT
) AS $$
DECLARE
  v_now BIGINT := EXTRACT(epoch FROM now()) * 1000;
  v_recovery_attempts INTEGER := CASE WHEN p_status = 'ENQUEUED' THEN 0 ELSE 1 END;
  v_priority INTEGER := COALESCE(p_priority, 0);
BEGIN
  -- Validate workflow UUID
  IF p_workflow_uuid IS NULL OR p_workflow_uuid = '' THEN
    RAISE EXCEPTION 'Workflow UUID cannot be null or empty';
  END IF;
  
  -- Perform the upsert operation
  INSERT INTO "dbos".workflow_status (
    workflow_uuid, status, inputs,
    name, class_name, config_name,
    queue_name, deduplication_id, priority, queue_partition_key,
    authenticated_user, assumed_role, authenticated_roles,
    executor_id, application_version, application_id,
    created_at, updated_at, recovery_attempts,
    workflow_timeout_ms, workflow_deadline_epoch_ms,
    parent_workflow_id, owner_xid, serialization
  ) VALUES (
    p_workflow_uuid, p_status, p_inputs,
    p_name, p_class_name, p_config_name,
    p_queue_name, p_deduplication_id, v_priority, p_queue_partition_key,
    p_authenticated_user, p_assumed_role, p_authenticated_roles,
    p_executor_id, p_application_version, p_application_id,
    v_now, v_now, v_recovery_attempts,
    p_workflow_timeout_ms, p_workflow_deadline_epoch_ms,
    p_parent_workflow_id, p_owner_xid, p_serialization
  )
  ON CONFLICT (workflow_uuid)
  DO UPDATE SET
    recovery_attempts = workflow_status.recovery_attempts,
    updated_at = EXCLUDED.updated_at,
    executor_id = CASE
        WHEN EXCLUDED.status = 'ENQUEUED'
          THEN workflow_status.executor_id
          ELSE EXCLUDED.executor_id
    END
  RETURNING 
    workflow_status.recovery_attempts,
    workflow_status.status,
    workflow_status.name,
    workflow_status.class_name,
    workflow_status.config_name,
    workflow_status.queue_name,
    workflow_status.workflow_timeout_ms,
    workflow_status.workflow_deadline_epoch_ms,
    workflow_status.serialization,
    workflow_status.owner_xid;
  
EXCEPTION
  WHEN unique_violation THEN
    -- Handle duplicate deduplication_id
    IF SQLERRM LIKE '%deduplication_id%' OR SQLERRM LIKE '%workflow_status_deduplication_idx%' THEN
      RAISE EXCEPTION 'DBOS_QUEUE_DUPLICATED: Workflow % with queue % and deduplication ID % already exists', p_workflow_uuid, COALESCE(p_queue_name, ''), COALESCE(p_deduplication_id, '');
    END IF;
    RAISE;
END;
$$ LANGUAGE plpgsql;