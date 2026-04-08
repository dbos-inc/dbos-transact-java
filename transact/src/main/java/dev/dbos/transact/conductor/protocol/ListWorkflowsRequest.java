package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

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
    return new ListWorkflowsInput(
        body.workflow_uuids,
        body.status != null
            ? body.status.stream().map(WorkflowState::valueOf).collect(Collectors.toList())
            : null,
        body.start_time != null ? Instant.parse(body.start_time) : null,
        body.end_time != null ? Instant.parse(body.end_time) : null,
        body.workflow_name,
        null, // className
        null, // instanceName
        body.application_version,
        body.authenticated_user,
        body.limit,
        body.offset,
        body.sort_desc,
        body.workflow_id_prefix,
        body.load_input,
        body.load_output,
        body.queue_name,
        body.queues_only,
        body.executor_id,
        body.forked_from,
        body.parent_workflow_id,
        body.was_forked_from,
        body.has_parent);
  }
}
