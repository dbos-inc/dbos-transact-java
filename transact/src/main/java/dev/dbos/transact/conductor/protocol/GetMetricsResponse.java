package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.database.MetricData;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class GetMetricsResponse extends BaseResponse {
  public static record MetricsDataOutput(String metric_type, String metric_name, long value) {}

  public List<MetricsDataOutput> metrics;

  public GetMetricsResponse() {}

  public GetMetricsResponse(BaseMessage message, List<MetricData> metrics) {
    super(message.type, message.request_id);
    this.metrics =
        metrics.stream()
            .map(m -> new MetricsDataOutput(m.metricType(), m.metricName(), m.value()))
            .collect(Collectors.toList());
  }

  public GetMetricsResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
    this.metrics = Collections.emptyList();
  }
}
