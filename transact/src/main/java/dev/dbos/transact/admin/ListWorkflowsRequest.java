package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;
import java.util.List;

// TODO: Analyze admin server support for /queues request fields and confirm
// this request maps all required admin filters/options into ListWorkflowsInput.
// https://github.com/dbos-inc/dbos-transact-java/issues/345?reload=1
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
        workflow_uuids,
        status != null ? List.of(WorkflowState.valueOf(status)) : null,
        start_time != null ? Instant.parse(start_time) : null,
        end_time != null ? Instant.parse(end_time) : null,
        workflow_name != null ? List.of(workflow_name) : null,
        null, // className
        null, // instanceName
        application_version != null ? List.of(application_version) : null,
        authenticated_user != null ? List.of(authenticated_user) : null,
        limit,
        offset,
        sort_desc,
        workflow_id_prefix != null ? List.of(workflow_id_prefix) : null,
        load_input,
        load_output,
        null, // queueName
        false, // queuesOnly
        null, // executorIds
        fork_from != null ? List.of(fork_from) : null,
        parent_workflow_id != null ? List.of(parent_workflow_id) : null,
        null, // wasForkedFrom
        null // hasParent
        );
  }
}
