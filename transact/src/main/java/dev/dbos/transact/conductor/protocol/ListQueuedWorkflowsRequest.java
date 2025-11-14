package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class ListQueuedWorkflowsRequest extends BaseMessage {
  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {
    public String workflow_name;
    public String start_time;
    public String end_time;
    public String status;
    public String forked_from;
    public String queue_name;
    public Integer limit;
    public Integer offset;
    public Boolean sort_desc;
    public Boolean load_input;
  }

  public static class Builder {
    private String workflow_name;
    private String start_time;
    private String end_time;
    private String status;
    private String forked_from;
    private String queue_name;
    private Integer limit;
    private Integer offset;
    private Boolean sort_desc;
    private Boolean load_input;

    public Builder workflowName(String workflowName) {
      workflow_name = workflowName;
      return this;
    }

    public Builder startTime(String startTime) {
      start_time = startTime;
      return this;
    }

    public Builder endTime(String endTime) {
      end_time = endTime;
      return this;
    }

    public Builder status(String status) {
      this.status = status;
      return this;
    }

    public Builder forkedFrom(String forkedFrom) {
      this.forked_from = forkedFrom;
      return this;
    }

    public Builder queueName(String queueName) {
      this.queue_name = queueName;
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

    public Builder sortDesc(Boolean sortDesc) {
      this.sort_desc = sortDesc;
      return this;
    }

    public Builder loadInput(Boolean loadInput) {
      this.load_input = loadInput;
      return this;
    }

    public ListQueuedWorkflowsRequest build(String requestId) {
      ListQueuedWorkflowsRequest request = new ListQueuedWorkflowsRequest();
      request.type = MessageType.LIST_QUEUED_WORKFLOWS.getValue();
      request.request_id = requestId;

      Body body = new Body();
      body.workflow_name = this.workflow_name;
      body.start_time = this.start_time;
      body.end_time = this.end_time;
      body.status = this.status;
      body.forked_from = this.forked_from;
      body.queue_name = this.queue_name;
      body.limit = this.limit;
      body.offset = this.offset;
      body.sort_desc = this.sort_desc;
      body.load_input = this.load_input;
      request.body = body;
      return request;
    }
  }

  public ListWorkflowsInput asInput() {
    Objects.requireNonNull(body);

    return new ListWorkflowsInput()
        .withQueuesOnly(true)
        .withWorkflowName(body.workflow_name)
        .withStartTime(body.start_time != null ? OffsetDateTime.parse(body.start_time) : null)
        .withEndTime(body.end_time != null ? OffsetDateTime.parse(body.end_time) : null)
        .withStatus(body.status)
        .withForkedFrom(body.forked_from)
        .withQueueName(body.queue_name)
        .withLimit(body.limit)
        .withOffset(body.offset)
        .withSortDesc(body.sort_desc)
        .withLoadInput(body.load_input);
  }
}
