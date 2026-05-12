package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetWorkflowEventsRequest extends BaseMessage {
  public String workflow_id;
}
