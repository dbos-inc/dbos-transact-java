package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.NotificationInfo;

import java.util.Collections;
import java.util.List;

public class GetWorkflowNotificationsResponse extends BaseResponse {

  public record NotificationOutput(
      String topic, String message, long created_at_epoch_ms, boolean consumed) {
    public static NotificationOutput from(NotificationInfo info) {
      return new NotificationOutput(
          info.topic(), JSONUtil.toJson(info.message()), info.createdAtEpochMs(), info.consumed());
    }
  }

  public List<NotificationOutput> notifications;

  public GetWorkflowNotificationsResponse() {}

  public GetWorkflowNotificationsResponse(BaseMessage message, List<NotificationInfo> infos) {
    super(message.type, message.request_id);
    this.notifications = infos.stream().map(NotificationOutput::from).toList();
  }

  public GetWorkflowNotificationsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.notifications = Collections.emptyList();
  }
}
