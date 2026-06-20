package dev.dbos.transact.admin;

import dev.dbos.transact.conductor.protocol.StringOrListDeserializer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import tools.jackson.databind.annotation.JsonDeserialize;

public record ListQueuedWorkflowsRequest(
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> workflow_name,
    String start_time,
    String end_time,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> status,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> fork_from,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> parent_workflow_id,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> queue_name,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    Boolean load_input) {

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput(
        null, // workflowIds
        status != null
            ? status.stream().map(WorkflowState::valueOf).collect(Collectors.toList())
            : null,
        start_time != null ? Instant.parse(start_time) : null,
        end_time != null ? Instant.parse(end_time) : null,
        workflow_name,
        null, // className
        null, // instanceName
        null, // applicationVersion
        null, // authenticatedUser
        limit,
        offset,
        sort_desc,
        null, // workflowIdPrefix
        load_input,
        false, // loadOutput
        queue_name,
        true, // queuesOnly
        null, // executorIds
        fork_from,
        parent_workflow_id,
        null, // wasForkedFrom
        null, // hasParent
        null, // attributes
        null, // completedAfter
        null, // completedBefore
        null, // dequeuedAfter
        null // dequeuedBefore
        );
  }
}
