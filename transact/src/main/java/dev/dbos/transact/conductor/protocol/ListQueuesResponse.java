package dev.dbos.transact.conductor.protocol;

import java.util.Collections;
import java.util.List;

public class ListQueuesResponse extends BaseResponse {
  public List<QueueOutput> output;

  public ListQueuesResponse() {}

  public ListQueuesResponse(BaseMessage message, List<QueueOutput> output) {
    super(message.type, message.request_id);
    this.output = output;
  }

  public ListQueuesResponse(BaseMessage message, String errorMessage) {
    super(message.type, message.request_id, errorMessage);
    this.output = Collections.emptyList();
  }
}
