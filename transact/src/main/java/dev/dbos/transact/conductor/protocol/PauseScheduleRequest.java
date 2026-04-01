package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PauseScheduleRequest extends BaseMessage {
  public String schedule_name;

  public PauseScheduleRequest() {}

  public PauseScheduleRequest(String requestId, String scheduleName) {
    this.type = MessageType.PAUSE_SCHEDULE.getValue();
    this.request_id = requestId;
    this.schedule_name = scheduleName;
  }
}
