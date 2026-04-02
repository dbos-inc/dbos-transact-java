package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

public record ListWorkflowsRequest(
    List<String> workflow_uuids,
    String workflow_name,
    String authenticated_user,
    String start_time,
    String end_time,
    String status,
    String application_version,
    String fork_from,
    String parent_workflow_id,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    String workflow_id_prefix,
    Boolean load_input,
    Boolean load_output) {

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput(
        workflow_uuids != null ? workflow_uuids.toArray(String[]::new) : null,
        status != null ? new String[] {status} : null,
        start_time != null ? OffsetDateTime.parse(start_time) : null,
        end_time != null ? OffsetDateTime.parse(end_time) : null,
        workflow_name != null ? new String[] {workflow_name} : null,
        null, // class_name,
        null, // instance_name
        application_version != null ? new String[] {application_version} : null,
        authenticated_user != null ? new String[] {authenticated_user} : null,
        limit,
        offset,
        sort_desc,
        workflow_id_prefix,
        load_input,
        load_output,
        null, // queueName
        false, // queuesOnly
        null, // Executor IDs
        fork_from != null ? new String[] {fork_from} : null,
        parent_workflow_id != null ? new String[] {parent_workflow_id} : null);
  }
}
