package dev.dbos.transact.admin;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.Instant;
import java.util.List;

// TODO: Complete the admin server gap analysis for queued workflow listing by verifying
// that this request shape and its `asInput()` mapping cover the intended queue-specific
// filters and behavior parity with the underlying workflow listing API.
// Tracking issue: https://github.com/dbos-inc/dbos-transact-java/issues/345?reload=1
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
    return ListWorkflowsInput.builder()
        .status(status != null ? List.of(status) : null)
        .startTime(start_time != null ? Instant.parse(start_time) : null)
        .endTime(end_time != null ? Instant.parse(end_time) : null)
        .workflowName(workflow_name != null ? List.of(workflow_name) : null)
        .limit(limit)
        .offset(offset)
        .sortDesc(sort_desc)
        .loadInput(load_input)
        .loadOutput(false)
        .queueName(queue_name != null ? List.of(queue_name) : null)
        .queuesOnly(true)
        .forkedFrom(fork_from != null ? List.of(fork_from) : null)
        .parentWorkflowId(parent_workflow_id != null ? List.of(parent_workflow_id) : null)
        .build();
  }
}
