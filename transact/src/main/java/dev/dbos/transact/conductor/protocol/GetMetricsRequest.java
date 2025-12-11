package dev.dbos.transact.conductor.protocol;

import java.time.Instant;

public class GetMetricsRequest extends BaseMessage {
  public String start_time;
  public String end_time;
  public String metric_class;

  GetMetricsRequest() {}

  public GetMetricsRequest(
      String requestId, Instant startTime, Instant endTime, String metricClass) {
    this.type = MessageType.LIST_STEPS.getValue();
    this.request_id = requestId;
    this.start_time = startTime == null ? null : startTime.toString();
    this.end_time = endTime == null ? null : endTime.toString();
    this.metric_class = metricClass;
  }

  public Instant startTime() {
    return start_time == null ? null : Instant.parse(start_time);
  }

  public Instant endTime() {
    return end_time == null ? null : Instant.parse(end_time);
  }
}
