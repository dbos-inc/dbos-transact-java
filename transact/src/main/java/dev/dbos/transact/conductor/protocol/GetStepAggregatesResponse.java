package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.StepAggregateRow;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetStepAggregatesResponse extends BaseResponse {

  public record StepAggregateOutput(Map<String, String> group, Long count, Long max_duration_ms) {

    public static StepAggregateOutput from(StepAggregateRow row) {
      return new StepAggregateOutput(row.group(), row.count(), row.maxDurationMs());
    }
  }

  public List<StepAggregateOutput> output;

  public GetStepAggregatesResponse() {}

  public GetStepAggregatesResponse(BaseMessage message, List<StepAggregateRow> rows) {
    super(message.type, message.request_id);
    this.output = rows.stream().map(StepAggregateOutput::from).toList();
  }

  public GetStepAggregatesResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.output = Collections.emptyList();
  }
}
