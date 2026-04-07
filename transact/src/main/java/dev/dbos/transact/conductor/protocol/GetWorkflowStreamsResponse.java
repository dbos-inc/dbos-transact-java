package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetWorkflowStreamsResponse extends BaseResponse {

  public record StreamEntryOutput(String key, List<String> values) {
    public static StreamEntryOutput from(String key, List<Object> values) {
      return new StreamEntryOutput(key, values.stream().map(JSONUtil::toJson).toList());
    }
  }

  public List<StreamEntryOutput> streams;

  public GetWorkflowStreamsResponse() {}

  public GetWorkflowStreamsResponse(BaseMessage message, Map<String, List<Object>> streamData) {
    super(message.type, message.request_id);
    this.streams =
        streamData.entrySet().stream()
            .map(e -> StreamEntryOutput.from(e.getKey(), e.getValue()))
            .toList();
  }

  public GetWorkflowStreamsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.streams = Collections.emptyList();
  }
}
