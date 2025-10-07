package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class ListWorkflowsRequest extends BaseMessage {
  public Body body;

  public static class Body {
    public List<String> workflow_uuids;
    public String workflow_name;
    // public String class_name; // Future support
    // public String instance_name;
    public String authenticated_user;
    public String start_time;
    public String end_time;
    public String status;
    public String application_version;
    public Integer limit;
    public Integer offset;
    public Boolean sort_desc;
    public Boolean load_input;
    public Boolean load_output;
  }

  public static class Builder {
    private List<String> workflow_uuids = new ArrayList<String>();
    private String workflow_name;
    // private String class_name;
    // private String instance_name;
    private String authenticated_user;
    private String start_time;
    private String end_time;
    private String status;
    private String application_version;
    private Integer limit;
    private Integer offset;
    private Boolean sort_desc;
    private Boolean load_input;
    private Boolean load_output;

    public Builder workflowUuids(List<String> workflow_uuids) {
      this.workflow_uuids.addAll(workflow_uuids);
      return this;
    }

    public Builder workflowId(String workflow_id) {
      this.workflow_uuids.add(workflow_id);
      return this;
    }

    /* Future
    public Builder className(String class_name) {
      this.class_name = class_name;
      return this;
    }

    public Builder instanceName(String instance_name) {
      this.instance_name = instance_name;
      return this;
    }
    */

    public Builder workflowName(String workflow_name) {
      this.workflow_name = workflow_name;
      return this;
    }

    public Builder authenticatedUser(String authenticated_user) {
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
      this.status = status;
      return this;
    }

    public Builder applicationVersion(String application_version) {
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

    public ListWorkflowsRequest build(String requestId) {
      ListWorkflowsRequest request = new ListWorkflowsRequest();
      request.type = MessageType.LIST_WORKFLOWS.getValue();
      request.request_id = requestId;

      Body body = new Body();
      body.workflow_uuids = this.workflow_uuids;
      // body.class_name = this.class_name;
      // body.instance_name = this.instance_name;
      body.workflow_name = this.workflow_name;
      body.authenticated_user = this.authenticated_user;
      body.start_time = this.start_time;
      body.end_time = this.end_time;
      body.status = this.status;
      body.application_version = this.application_version;
      body.limit = this.limit;
      body.offset = this.offset;
      body.sort_desc = this.sort_desc;
      body.load_input = this.load_input;
      body.load_output = this.load_output;
      request.body = body;
      return request;
    }
  }

  public ListWorkflowsInput asInput() {
    var builder =
        new ListWorkflowsInput.Builder()
            .workflowIDs(body.workflow_uuids)
            .workflowName(body.workflow_name)
            .authenticatedUser(body.authenticated_user)
            .startTime(body.start_time != null ? OffsetDateTime.parse(body.start_time) : null)
            .endTime(body.end_time != null ? OffsetDateTime.parse(body.end_time) : null)
            .status(body.status)
            .applicationVersion(body.application_version)
            .limit(body.limit)
            .offset(body.offset)
            .sortDesc(body.sort_desc)
            .loadInput(body.load_input)
            .loadOutput(body.load_output);

            return builder.build();
  }
}
