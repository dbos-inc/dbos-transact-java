CREATE OR REPLACE FUNCTION "dbos".enqueue_portable_workflow(
  p_workflow_name TEXT,
  p_queue_name TEXT,
  p_class_name TEXT,
  p_config_name TEXT DEFAULT '',
  p_workflow_id TEXT DEFAULT NULL,
  p_app_version TEXT DEFAULT NULL,
  p_timeout_ms BIGINT DEFAULT NULL,
  p_deadline_epoch_ms BIGINT DEFAULT NULL,
  p_deduplication_id TEXT DEFAULT NULL,
  p_priority INTEGER DEFAULT NULL,
  p_queue_partition_key TEXT DEFAULT NULL,
  p_serialized_inputs TEXT,
  p_serialization_format TEXT,
  p_parent_workflow_id TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
  v_workflow_id TEXT;
  v_init_result RECORD;
BEGIN
  -- Validate required parameters
  IF p_workflow_name IS NULL OR p_workflow_name = '' THEN
    RAISE EXCEPTION 'Workflow name cannot be null or empty';
  END IF;
  
  IF p_queue_name IS NULL OR p_queue_name = '' THEN
    RAISE EXCEPTION 'Queue name cannot be null or empty';
  END IF;
  
  IF p_class_name IS NULL OR p_class_name = '' THEN
    RAISE EXCEPTION 'Class name cannot be null or empty';
  END IF;
  
  IF p_serialized_inputs IS NULL THEN
    RAISE EXCEPTION 'Serialized inputs cannot be null';
  END IF;
  
  IF p_serialization_format IS NULL OR p_serialization_format = '' THEN
    RAISE EXCEPTION 'Serialization format cannot be null or empty';
  END IF;
  
  -- Generate UUID if workflow ID not provided
  v_workflow_id := COALESCE(p_workflow_id, gen_random_uuid()::TEXT);
  
  -- Call init_workflow_status to enqueue the workflow
  SELECT * FROM "dbos".init_workflow_status(
    v_workflow_id,
    'ENQUEUED',
    p_serialized_inputs,
    p_workflow_name,
    p_class_name,
    COALESCE(p_config_name, ''),
    p_queue_name,
    p_deduplication_id,
    COALESCE(p_priority, 0),
    p_queue_partition_key,
    NULL, -- authenticated_user
    NULL, -- assumed_role  
    NULL, -- authenticated_roles
    NULL, -- executor_id
    p_app_version,
    NULL, -- application_id
    p_timeout_ms,
    p_deadline_epoch_ms,
    p_parent_workflow_id,
    NULL, -- owner_xid (will be set by init_workflow_status)
    p_serialization_format,
    NULL, -- max_retries
    FALSE, -- is_recovery_request
    FALSE  -- is_dequeued_request
  ) INTO v_init_result;
  
  -- Return the workflow ID
  RETURN v_workflow_id;
  
EXCEPTION
  WHEN OTHERS THEN
    -- Re-raise any exceptions from init_workflow_status
    RAISE;
END;
$$ LANGUAGE plpgsql;