package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class ForkFromFailureResponse extends BaseResponse {
  public List<String> forked_workflow_ids;

  public ForkFromFailureResponse() {}

  public ForkFromFailureResponse(
      BaseMessage message, List<String> forked_workflow_ids, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.forked_workflow_ids = forked_workflow_ids;
  }

  public ForkFromFailureResponse(BaseMessage message, List<String> forked_workflow_ids) {
    this(message, forked_workflow_ids, null);
  }

  public ForkFromFailureResponse(BaseMessage message, Exception ex) {
    this(message, null, ex.getMessage());
  }
}
