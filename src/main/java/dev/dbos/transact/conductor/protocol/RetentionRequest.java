package dev.dbos.transact.conductor.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class RetentionRequest extends BaseMessage {
  public RetentionBody body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RetentionBody {
    public Long gc_cutoff_epoch_ms;
    public Long gc_rows_threshold;
    public Long timeout_cutoff_epoch_ms;
  }

  public RetentionRequest() {}

  public RetentionRequest(
      String requestId, Long gcCutoff, Long gcRowsThreshold, Long timeoutCutoff) {
    this.type = MessageType.RETENTION.getValue();
    this.request_id = requestId;
    this.body = new RetentionBody();
    this.body.gc_cutoff_epoch_ms = gcCutoff;
    this.body.gc_rows_threshold = gcRowsThreshold;
    this.body.timeout_cutoff_epoch_ms = timeoutCutoff;
  }
}
