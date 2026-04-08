package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetWorkflowAggregatesRequest extends BaseMessage {

  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {
    public boolean group_by_status;
    public boolean group_by_name;
    public boolean group_by_queue_name;
    public boolean group_by_executor_id;
    public boolean group_by_application_version;
    public List<String> status;
    public String start_time;
    public String end_time;
    public List<String> name;
    public List<String> app_version;
    public List<String> executor_id;
    public List<String> queue_name;
    public List<String> workflow_id_prefix;
  }

  public GetWorkflowAggregatesInput toInput() {
    if (body == null) {
      return new GetWorkflowAggregatesInput();
    }
    return new GetWorkflowAggregatesInput(
        body.group_by_status,
        body.group_by_name,
        body.group_by_queue_name,
        body.group_by_executor_id,
        body.group_by_application_version,
        body.name,
        body.status,
        body.queue_name,
        body.executor_id,
        body.app_version,
        body.workflow_id_prefix,
        body.start_time != null ? Instant.parse(body.start_time) : null,
        body.end_time != null ? Instant.parse(body.end_time) : null);
  }
}
