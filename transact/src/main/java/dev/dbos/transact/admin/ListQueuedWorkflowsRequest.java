package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;

public record ListQueuedWorkflowsRequest(
    String workflow_name,
    String start_time,
    String end_time,
    String status,
    String fork_from,
    String parent_workflow_id,
    String queue_name,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    Boolean load_input) {

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput(
        null, // workflow ids
        status != null ? new String[] {status} : null,
        start_time != null ? OffsetDateTime.parse(start_time) : null,
        end_time != null ? OffsetDateTime.parse(end_time) : null,
        workflow_name != null ? new String[] {workflow_name} : null,
        null, // class_name,
        null, // instance_name
        null, // app version
        null, // auth user
        limit,
        offset,
        sort_desc,
        null, // wf id prefix
        load_input,
        false, // load output
        queue_name != null ? new String[] {queue_name} : null,
        true, // queuesOnly: only list queued workflows
        null, // Executor IDs
        fork_from != null ? new String[] {fork_from} : null,
        parent_workflow_id != null ? new String[] {parent_workflow_id} : null);
  }
}
