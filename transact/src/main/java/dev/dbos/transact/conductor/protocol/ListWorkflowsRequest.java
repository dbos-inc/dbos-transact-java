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
  }

  public static class Builder {
    private List<String> workflow_uuids;
    private List<String> workflow_name;
    private List<String> authenticated_user;
    private String start_time;
    private String end_time;
    private List<String> status;
    private List<String> application_version;
    private List<String> forked_from;
    private List<String> parent_workflow_id;
    private List<String> queue_name;
    private Integer limit;
    private Integer offset;
    private Boolean sort_desc;
    private List<String> workflow_id_prefix;
    private Boolean load_input;
    private Boolean load_output;
    private List<String> executor_id;
    private Boolean queues_only;
    private Boolean was_forked_from;

    public Builder workflowIds(List<String> workflow_ids) {
      this.workflow_uuids = workflow_ids;
      return this;
    }

    public Builder workflowId(String workflow_id) {
      this.workflow_uuids = workflow_id == null ? null : List.of(workflow_id);
      return this;
    }

    public Builder workflowName(String workflow_name) {
      this.workflow_name = workflow_name == null ? null : List.of(workflow_name);
      return this;
    }

    public Builder workflowNames(List<String> workflow_name) {
      this.workflow_name = workflow_name;
      return this;
    }

    public Builder authenticatedUser(String authenticated_user) {
      this.authenticated_user = authenticated_user == null ? null : List.of(authenticated_user);
      return this;
    }

    public Builder authenticatedUsers(List<String> authenticated_user) {
      this.authenticated_user = authenticated_user;
      return this;
    }

    public Builder startTime(String start_time) {
      this.start_time = start_time;
      return this;
    }

    public Builder endTime(String end_time) {
      this.end_time = end_time;
      return this;
    }

    public Builder status(String status) {
      this.status = status == null ? null : List.of(status);
      return this;
    }

    public Builder statuses(List<String> status) {
      this.status = status;
      return this;
    }

    public Builder forkedFrom(String forked_from) {
      this.forked_from = forked_from == null ? null : List.of(forked_from);
      return this;
    }

    public Builder forkedFrom(List<String> forked_from) {
      this.forked_from = forked_from;
      return this;
    }

    public Builder parentWorkflowId(String parentWorkflowId) {
      this.parent_workflow_id = parentWorkflowId == null ? null : List.of(parentWorkflowId);
      return this;
    }

    public Builder parentWorkflowIds(List<String> parentWorkflowId) {
      this.parent_workflow_id = parentWorkflowId;
      return this;
    }

    public Builder applicationVersion(String application_version) {
      this.application_version = application_version == null ? null : List.of(application_version);
      return this;
    }

    public Builder applicationVersions(List<String> application_version) {
      this.application_version = application_version;
      return this;
    }

    public Builder limit(Integer limit) {
      this.limit = limit;
      return this;
    }

    public Builder offset(Integer offset) {
      this.offset = offset;
      return this;
    }

    public Builder sortDesc(Boolean sort_desc) {
      this.sort_desc = sort_desc;
      return this;
    }

    public Builder queueName(String queue_name) {
      this.queue_name = queue_name == null ? null : List.of(queue_name);
      return this;
    }

    public Builder queueNames(List<String> queue_name) {
      this.queue_name = queue_name;
      return this;
    }

    public Builder workflowIdPrefix(String workflow_id_prefix) {
      this.workflow_id_prefix = workflow_id_prefix == null ? null : List.of(workflow_id_prefix);
      return this;
    }

    public Builder workflowIdPrefixes(List<String> workflow_id_prefix) {
      this.workflow_id_prefix = workflow_id_prefix;
      return this;
    }

    public Builder executorId(String executor_id) {
      this.executor_id = executor_id == null ? null : List.of(executor_id);
      return this;
    }

    public Builder executorIds(List<String> executor_id) {
      this.executor_id = executor_id;
      return this;
    }

    public Builder queuesOnly(Boolean queues_only) {
      this.queues_only = queues_only;
      return this;
    }

    public Builder wasForkedFrom(Boolean was_forked_from) {
      this.was_forked_from = was_forked_from;
      return this;
    }

    public ListWorkflowsRequest build(String requestId) {
      ListWorkflowsRequest request = new ListWorkflowsRequest();
      request.type = MessageType.LIST_WORKFLOWS.getValue();
      request.request_id = requestId;

      Body body = new Body();
      body.workflow_uuids = this.workflow_uuids;
      body.workflow_name = this.workflow_name;
      body.authenticated_user = this.authenticated_user;
      body.start_time = this.start_time;
      body.end_time = this.end_time;
      body.status = this.status;
      body.application_version = this.application_version;
      body.forked_from = this.forked_from;
      body.parent_workflow_id = this.parent_workflow_id;
      body.queue_name = this.queue_name;
      body.limit = this.limit;
      body.offset = this.offset;
      body.sort_desc = this.sort_desc;
      body.workflow_id_prefix = this.workflow_id_prefix;
      body.load_input = this.load_input;
      body.load_output = this.load_output;
      body.executor_id = this.executor_id;
      body.queues_only = this.queues_only;
      body.was_forked_from = this.was_forked_from;
      request.body = body;
      return request;
    }
  }

  public ListWorkflowsInput asInput() {
    return new ListWorkflowsInput()
        .withWorkflowIds(body.workflow_uuids)
        .withWorkflowNames(body.workflow_name)
        .withAuthenticatedUsers(body.authenticated_user)
        .withStartTime(body.start_time != null ? OffsetDateTime.parse(body.start_time) : null)
        .withEndTime(body.end_time != null ? OffsetDateTime.parse(body.end_time) : null)
        .withStatuses(body.status)
        .withApplicationVersions(body.application_version)
        .withForkedFrom(body.forked_from)
        .withParentWorkflowIds(body.parent_workflow_id)
        .withQueueNames(body.queue_name)
        .withLimit(body.limit)
        .withOffset(body.offset)
        .withSortDesc(body.sort_desc)
        .withWorkflowIdPrefix(
            body.workflow_id_prefix != null && !body.workflow_id_prefix.isEmpty()
                ? body.workflow_id_prefix.get(0)
                : null)
        .withLoadInput(body.load_input)
        .withLoadOutput(body.load_output)
        .withExecutorIds(body.executor_id)
        .withQueuesOnly(body.queues_only);
  }
}
