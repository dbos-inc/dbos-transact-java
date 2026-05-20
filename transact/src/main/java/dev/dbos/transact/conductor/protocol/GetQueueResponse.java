package dev.dbos.transact.conductor.protocol;

public class GetQueueResponse extends BaseResponse {
  public QueueOutput output;

  public GetQueueResponse() {}

  public GetQueueResponse(BaseMessage message, QueueOutput output) {
    super(message.type, message.request_id);
    this.output = output;
  }

  public GetQueueResponse(BaseMessage message, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.output = null;
  }
}
