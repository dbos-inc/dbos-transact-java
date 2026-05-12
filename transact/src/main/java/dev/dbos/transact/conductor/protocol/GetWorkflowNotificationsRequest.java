package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetWorkflowNotificationsRequest extends BaseMessage {
  public String workflow_id;
}
