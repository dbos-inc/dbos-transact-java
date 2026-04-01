package dev.dbos.transact.conductor.protocol;

import java.util.Collections;
import java.util.List;

public class ListSchedulesResponse extends BaseResponse {
  public List<ScheduleOutput> output;

  public ListSchedulesResponse() {}

  public ListSchedulesResponse(BaseMessage message, List<ScheduleOutput> output) {
    super(message.type, message.request_id);
    this.output = output;
  }

  public ListSchedulesResponse(BaseMessage message, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.output = Collections.emptyList();
  }
}
