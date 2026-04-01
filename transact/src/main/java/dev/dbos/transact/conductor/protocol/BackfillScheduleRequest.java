package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BackfillScheduleRequest extends BaseMessage {
  public String schedule_name;
  public String start;
  public String end;

  public BackfillScheduleRequest() {}

  public BackfillScheduleRequest(String requestId, String scheduleName, String start, String end) {
    this.type = MessageType.BACKFILL_SCHEDULE.getValue();
    this.request_id = requestId;
    this.schedule_name = scheduleName;
    this.start = start;
    this.end = end;
  }
}
