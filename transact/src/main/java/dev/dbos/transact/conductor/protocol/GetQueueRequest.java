package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetQueueRequest extends BaseMessage {
  public String name;

  public GetQueueRequest() {}
}
