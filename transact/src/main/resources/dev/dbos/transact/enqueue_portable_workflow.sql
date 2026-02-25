CREATE FUNCTION "dbos".enqueue_portable_workflow(
  p_workflow_name TEXT,
  p_queue_name TEXT,
  p_positional_args JSON[] DEFAULT '[]'::JSON[],
  p_named_args JSON DEFAULT '{}'::JSON,
  p_class_name TEXT DEFAULT NULL,
  p_config_name TEXT DEFAULT NULL,
  p_workflow_id TEXT DEFAULT NULL,
  p_app_version TEXT DEFAULT NULL,
  p_timeout_ms BIGINT DEFAULT NULL,
  p_deadline_epoch_ms BIGINT DEFAULT NULL,
  p_deduplication_id TEXT DEFAULT NULL,
  p_priority INTEGER DEFAULT NULL,
  p_queue_partition_key TEXT DEFAULT NULL,
  p_parent_workflow_id TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
  v_workflow_id TEXT;
  v_init_result RECORD;
  v_serialized_inputs TEXT;
BEGIN
  -- Validate required parameters
  IF p_workflow_name IS NULL OR p_workflow_name = '' THEN
    RAISE EXCEPTION 'Workflow name cannot be null or empty';
  END IF;
  
  IF p_queue_name IS NULL OR p_queue_name = '' THEN
    RAISE EXCEPTION 'Queue name cannot be null or empty';
  END IF;
  
  -- Validate p_named_args is an object if not null
  IF p_named_args IS NOT NULL AND jsonb_typeof(p_named_args::jsonb) != 'object' THEN
    RAISE EXCEPTION 'Named args must be a JSON object';
  END IF;
  
  -- Serialize the arguments in portable format
  v_serialized_inputs := json_build_object(
    'positionalArgs', p_positional_args,
    'namedArgs', p_named_args
  )::TEXT;
  
  -- Generate UUID if workflow ID not provided
  v_workflow_id := COALESCE(p_workflow_id, gen_random_uuid()::TEXT);
  
  -- Call init_workflow_status to enqueue the workflow
  SELECT * FROM "dbos".init_workflow_status(
    v_workflow_id,
    'ENQUEUED',
    v_serialized_inputs,
    p_workflow_name,
    p_class_name,
    p_config_name,
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
    'portable_json' -- serialization_format
  ) INTO v_init_result;
  
  -- Return the workflow ID
  RETURN v_workflow_id;
  
EXCEPTION
  WHEN OTHERS THEN
    -- Re-raise any exceptions from init_workflow_status
    RAISE;
END;
$$ LANGUAGE plpgsql;