package dev.dbos.transact.admin;

import dev.dbos.transact.conductor.protocol.StringOrListDeserializer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import tools.jackson.databind.annotation.JsonDeserialize;

public record ListWorkflowsRequest(
    List<String> workflow_uuids,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> workflow_name,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> authenticated_user,
    String start_time,
    String end_time,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> status,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> application_version,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> fork_from,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> parent_workflow_id,
    Integer limit,
    Integer offset,
    Boolean sort_desc,
    @JsonDeserialize(using = StringOrListDeserializer.class) List<String> workflow_id_prefix,
    Boolean load_input,
    Boolean load_output) {

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput(
        workflow_uuids,
        status != null
            ? status.stream().map(WorkflowState::valueOf).collect(Collectors.toList())
            : null,
        start_time != null ? Instant.parse(start_time) : null,
        end_time != null ? Instant.parse(end_time) : null,
        workflow_name,
        null, // className
        null, // instanceName
        application_version,
        authenticated_user,
        limit,
        offset,
        sort_desc,
        workflow_id_prefix,
        load_input,
        load_output,
        null, // queueName
        false, // queuesOnly
        null, // executorIds
        fork_from,
        parent_workflow_id,
        null, // wasForkedFrom
        null // hasParent
        );
  }
}
