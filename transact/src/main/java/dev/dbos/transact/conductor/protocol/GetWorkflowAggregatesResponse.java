package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.WorkflowAggregateRow;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GetWorkflowAggregatesResponse extends BaseResponse {

  public record WorkflowAggregateOutput(
      Map<String, String> group,
      Long count,
      Long min_created_at,
      Long max_queue_wait_ms,
      Long max_total_latency_ms) {

    public static WorkflowAggregateOutput from(WorkflowAggregateRow row) {
      return new WorkflowAggregateOutput(
          row.group(),
          row.count(),
          row.minCreatedAt() != null ? row.minCreatedAt().toEpochMilli() : null,
          row.maxQueueWait() != null ? row.maxQueueWait().toMillis() : null,
          row.maxTotalLatency() != null ? row.maxTotalLatency().toMillis() : null);
    }
  }

  public List<WorkflowAggregateOutput> output;

  public GetWorkflowAggregatesResponse() {}

  public GetWorkflowAggregatesResponse(BaseMessage message, List<WorkflowAggregateRow> rows) {
    super(message.type, message.request_id);
    this.output = rows.stream().map(WorkflowAggregateOutput::from).toList();
  }

  public GetWorkflowAggregatesResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.output = Collections.emptyList();
  }
}
