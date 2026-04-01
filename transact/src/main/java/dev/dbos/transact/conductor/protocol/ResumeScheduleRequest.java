package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResumeScheduleRequest extends BaseMessage {
  public String schedule_name;

  public ResumeScheduleRequest() {}

  public ResumeScheduleRequest(String requestId, String scheduleName) {
    this.type = MessageType.RESUME_SCHEDULE.getValue();
    this.request_id = requestId;
    this.schedule_name = scheduleName;
  }
}
