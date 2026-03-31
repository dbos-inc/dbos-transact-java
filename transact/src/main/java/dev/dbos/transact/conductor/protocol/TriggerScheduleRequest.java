package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TriggerScheduleRequest extends BaseMessage {
  public String schedule_name;

  public TriggerScheduleRequest() {}

  public TriggerScheduleRequest(String requestId, String scheduleName) {
    this.type = MessageType.TRIGGER_SCHEDULE.getValue();
    this.request_id = requestId;
    this.schedule_name = scheduleName;
  }
}
