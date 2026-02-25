CREATE FUNCTION "dbos".send_message(
  p_destination_id TEXT,
  p_message JSON,
  p_topic TEXT DEFAULT NULL,
  p_idempotency_key TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
  v_idempotency_key TEXT;
  v_workflow_id TEXT;
  v_topic TEXT;
  v_current_time_ms BIGINT := extract(epoch from now()) * 1000;
  v_init_result RECORD;
BEGIN
  -- Validate required parameters
  IF p_destination_id IS NULL OR p_destination_id = '' THEN
    RAISE EXCEPTION 'Destination ID cannot be null or empty';
  END IF;
  
  -- Generate UUID if idempotency key not provided
  v_idempotency_key := COALESCE(p_idempotency_key, gen_random_uuid()::TEXT);
  
  -- Handle null topic
  v_topic := COALESCE(p_topic, '__null__topic__');
  
  -- Create workflow ID by combining destination ID and idempotency key
  v_workflow_id := p_destination_id || '-' || v_idempotency_key;
  
  -- Initialize temporary workflow status for sending
  SELECT * FROM "dbos".init_workflow_status(
    v_workflow_id,
    'SUCCESS', -- WorkflowState.SUCCESS
    NULL, -- inputs (not needed for send workflow)
    'temp_workflow-send-client', -- workflow_name
    NULL, -- class_name
    NULL, -- config_name
    NULL, -- queue_name (not queued)
    NULL, -- deduplication_id
    NULL, -- priority
    NULL, -- queue_partition_key
    NULL, -- authenticated_user
    NULL, -- assumed_role
    NULL, -- authenticated_roles
    NULL, -- executor_id
    NULL, -- app_version
    NULL, -- application_id
    NULL, -- timeout_ms
    NULL, -- deadline_epoch_ms
    NULL, -- parent_workflow_id
    'portable_json' -- serialization_format (always portable JSON)
  ) INTO v_init_result;
  
  -- Send the message by inserting into the notifications table
  INSERT INTO "dbos".notifications (
    destination_uuid,
    topic,
    message,
    serialization
  ) VALUES (
    p_destination_id,
    v_topic,
    p_message::TEXT, -- serialize message as JSON text
    'portable_json'
  );
  
  -- Record this send operation as a step in the temporary workflow
  INSERT INTO "dbos".operation_outputs (
    workflow_uuid,
    function_id,
    function_name,
    output,
    error,
    child_workflow_id,
    started_at_epoch_ms,
    completed_at_epoch_ms,
    serialization
  ) VALUES (
    v_workflow_id,
    0, -- function_id (step 0 for send operation)
    'DBOS.send', -- function_name
    NULL, -- output (send doesn't return anything)
    NULL, -- error
    NULL, -- child_workflow_id
    v_current_time_ms, -- started_at_epoch_ms
    v_current_time_ms, -- completed_at_epoch_ms
    'portable_json' -- serialization
  );
  
  -- Return the workflow ID used for this send operation
  RETURN v_workflow_id;
  
EXCEPTION
  WHEN OTHERS THEN
    -- Re-raise any exceptions
    RAISE;
END;
$$ LANGUAGE plpgsql;