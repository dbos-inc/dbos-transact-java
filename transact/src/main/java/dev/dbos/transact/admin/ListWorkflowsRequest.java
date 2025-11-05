package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

public record ListWorkflowsRequest(
    List<String> workflow_uuids,
    String authenticated_user,
    String start_time,
    String end_time,
    String status,
    String application_version,
    String workflow_name,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    String workflow_id_prefix,
    Boolean load_input,
    Boolean load_output,
    String queue_name) {

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput(
        workflow_uuids,
        status != null ? List.of(status) : null,
        start_time != null ? OffsetDateTime.parse(start_time) : null,
        end_time != null ? OffsetDateTime.parse(end_time) : null,
        workflow_name,
        null, // class_name,
        null, // instance_name
        application_version,
        authenticated_user,
        limit,
        offset,
        sort_desc,
        workflow_id_prefix,
        load_input,
        load_output,
        queue_name,
        queue_name != null ? true : false,
        null // Executor IDs
        );
  }
}
