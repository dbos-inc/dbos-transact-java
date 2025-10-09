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
    String workflow_id_prefix,
    String queue_name,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    Boolean load_input,
    Boolean load_output) {

  public ListWorkflowsInput asInput() {
    var builder =
        new ListWorkflowsInput.Builder()
            .workflowIDs(workflow_uuids)
            .workflowName(workflow_name)
            .authenticatedUser(authenticated_user)
            .startTime(start_time != null ? OffsetDateTime.parse(start_time) : null)
            .endTime(end_time != null ? OffsetDateTime.parse(end_time) : null)
            .status(status)
            .applicationVersion(application_version)
            .workflowIdPrefix(workflow_id_prefix)
            .queueName(queue_name)
            .limit(limit)
            .offset(offset)
            .sortDesc(sort_desc)
            .loadInput(load_input)
            .loadOutput(load_output);

    return builder.build();
  }
}
