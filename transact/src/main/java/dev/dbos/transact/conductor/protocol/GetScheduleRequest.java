package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetScheduleRequest extends BaseMessage {
  public String schedule_name;
  public Boolean load_context;

  public GetScheduleRequest() {}

  public GetScheduleRequest(String requestId, String scheduleName, Boolean loadContext) {
    this.type = MessageType.GET_SCHEDULE.getValue();
    this.request_id = requestId;
    this.schedule_name = scheduleName;
    this.load_context = loadContext;
  }

  public boolean loadContext() {
    return load_context != null && load_context;
  }
}
