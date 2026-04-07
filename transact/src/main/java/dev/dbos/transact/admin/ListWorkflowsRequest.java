package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

// TODO: admin server gap analysis
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
    return ListWorkflowsInput.builder()
        .workflowIds(workflow_uuids)
        .status(status != null ? List.of(status) : null)
        .startTime(start_time != null ? OffsetDateTime.parse(start_time) : null)
        .endTime(end_time != null ? OffsetDateTime.parse(end_time) : null)
        .workflowName(workflow_name != null ? List.of(workflow_name) : null)
        .applicationVersion(application_version != null ? List.of(application_version) : null)
        .authenticatedUser(authenticated_user != null ? List.of(authenticated_user) : null)
        .limit(limit)
        .offset(offset)
        .sortDesc(sort_desc)
        .workflowIdPrefix(workflow_id_prefix)
        .loadInput(load_input)
        .loadOutput(load_output)
        .queuesOnly(false)
        .forkedFrom(fork_from != null ? List.of(fork_from) : null)
        .parentWorkflowId(parent_workflow_id != null ? List.of(parent_workflow_id) : null)
        .build();
  }
}
