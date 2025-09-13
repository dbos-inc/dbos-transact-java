package dev.dbos.transact.conductor.protocol;

public class ExistPendingWorkflowsResponse extends BaseResponse {
  public boolean exist;

  public ExistPendingWorkflowsResponse() {}

  public ExistPendingWorkflowsResponse(BaseMessage message, boolean exist) {
    super(message.type, message.request_id);
    this.exist = exist;
  }

  public ExistPendingWorkflowsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
  }
}
