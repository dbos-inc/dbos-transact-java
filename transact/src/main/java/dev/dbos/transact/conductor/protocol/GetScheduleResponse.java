package dev.dbos.transact.conductor.protocol;

public class GetScheduleResponse extends BaseResponse {
  public ScheduleOutput output;

  public GetScheduleResponse() {}

  public GetScheduleResponse(BaseMessage message, ScheduleOutput output) {
    super(message.type, message.request_id);
    this.output = output;
  }

  public GetScheduleResponse(BaseMessage message, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.output = null;
  }
}
