package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class ListWorkflowsRequest extends BaseMessage {
  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {
    public List<String> workflow_uuids;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> workflow_name;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> authenticated_user;

    public String start_time;
    public String end_time;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> status;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> application_version;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> forked_from;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> parent_workflow_id;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> queue_name;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> workflow_id_prefix;

    @JsonDeserialize(using = StringOrListDeserializer.class)
    public List<String> executor_id;

    public Integer limit;
    public Integer offset;
    public Boolean sort_desc;
    public Boolean load_input;
    public Boolean load_output;
    public Boolean queues_only;
    public Boolean was_forked_from;
    public Boolean has_parent;
  }

  public ListWorkflowsInput asInput() {
    return ListWorkflowsInput.builder()
        .workflowIds(body.workflow_uuids)
        .workflowName(body.workflow_name)
        .authenticatedUser(body.authenticated_user)
        .startTime(body.start_time != null ? OffsetDateTime.parse(body.start_time) : null)
        .endTime(body.end_time != null ? OffsetDateTime.parse(body.end_time) : null)
        .status(body.status)
        .applicationVersion(body.application_version)
        .forkedFrom(body.forked_from)
        .parentWorkflowId(body.parent_workflow_id)
        .queueName(body.queue_name)
        .limit(body.limit)
        .offset(body.offset)
        .sortDesc(body.sort_desc)
        .workflowIdPrefix(
            body.workflow_id_prefix != null && !body.workflow_id_prefix.isEmpty()
                ? body.workflow_id_prefix.get(0)
                : null)
        .loadInput(body.load_input)
        .loadOutput(body.load_output)
        .executorId(body.executor_id)
        .queuesOnly(body.queues_only)
        .wasForkedFrom(body.was_forked_from)
        .hasParent(body.has_parent)
        .build();
  }
}
