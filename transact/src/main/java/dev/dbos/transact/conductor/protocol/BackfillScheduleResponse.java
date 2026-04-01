package dev.dbos.transact.conductor.protocol;

import java.util.Collections;
import java.util.List;

public class BackfillScheduleResponse extends BaseResponse {
  public List<String> workflow_ids;

  public BackfillScheduleResponse() {}

  public BackfillScheduleResponse(BaseMessage message, List<String> workflowIds) {
    super(message.type, message.request_id);
    this.workflow_ids = workflowIds;
  }

  public BackfillScheduleResponse(BaseMessage message, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.workflow_ids = Collections.emptyList();
  }
}
