package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ScheduleStatus;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ListSchedulesRequest extends BaseMessage {
  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Body(
      @JsonDeserialize(using = StringOrListDeserializer.class) List<String> status,
      @JsonDeserialize(using = StringOrListDeserializer.class) List<String> workflow_name,
      @JsonDeserialize(using = StringOrListDeserializer.class) List<String> schedule_name_prefix,
      Boolean load_context) {}

  public ListSchedulesRequest() {}

  public ListSchedulesRequest(String requestId, Body body) {
    this.type = MessageType.LIST_SCHEDULES.getValue();
    this.request_id = requestId;
    this.body = body;
  }

  public List<ScheduleStatus> statuses() {
    if (body == null || body.status() == null) {
      return null;
    }
    return body.status().stream().map(ScheduleStatus::valueOf).toList();
  }

  public List<String> workflowNames() {
    return body == null ? null : body.workflow_name();
  }

  public List<String> scheduleNamePrefixes() {
    return body == null ? null : body.schedule_name_prefix();
  }

  public boolean loadContext() {
    return body != null && body.load_context() != null && body.load_context();
  }
}
