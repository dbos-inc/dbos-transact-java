package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetWorkflowEventsResponse extends BaseResponse {

  public record EventOutput(String key, String value) {
    public static EventOutput from(Map.Entry<String, Object> entry) {
      return new EventOutput(entry.getKey(), JSONUtil.toJson(entry.getValue()));
    }
  }

  public List<EventOutput> events;

  public GetWorkflowEventsResponse() {}

  public GetWorkflowEventsResponse(BaseMessage message, Map<String, Object> events) {
    super(message.type, message.request_id);
    this.events = events.entrySet().stream().map(EventOutput::from).toList();
  }

  public GetWorkflowEventsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.events = Collections.emptyList();
  }
}
