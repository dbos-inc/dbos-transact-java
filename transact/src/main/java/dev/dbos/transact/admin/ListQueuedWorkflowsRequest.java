package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

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
        status != null ? List.of(status) : null,
        start_time != null ? OffsetDateTime.parse(start_time) : null,
        end_time != null ? OffsetDateTime.parse(end_time) : null,
        workflow_name != null ? List.of(workflow_name) : null,
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
        queue_name != null ? List.of(queue_name) : null,
        true, // queuesOnly: only list queued workflows
        null, // Executor IDs
        fork_from != null ? List.of(fork_from) : null,
        parent_workflow_id != null ? List.of(parent_workflow_id) : null);
  }
}
