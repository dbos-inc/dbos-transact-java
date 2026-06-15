package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.GetStepAggregatesInput;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetStepAggregatesRequest extends BaseMessage {

  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {
    public boolean group_by_function_name;
    public boolean group_by_status;
    public Boolean select_count;
    public Boolean select_max_duration_ms;
    public Long time_bucket_size_ms;
    public List<String> status;
    public List<String> function_name;
    public List<String> workflow_id_prefix;
    public String completed_after;
    public String completed_before;
  }

  public GetStepAggregatesInput toInput() {
    if (body == null) {
      return new GetStepAggregatesInput();
    }
    boolean anySelect =
        Boolean.TRUE.equals(body.select_count) || Boolean.TRUE.equals(body.select_max_duration_ms);
    boolean selectCount = anySelect ? Boolean.TRUE.equals(body.select_count) : true;
    return new GetStepAggregatesInput(
        body.group_by_function_name,
        body.group_by_status,
        selectCount,
        Boolean.TRUE.equals(body.select_max_duration_ms),
        body.time_bucket_size_ms != null ? Duration.ofMillis(body.time_bucket_size_ms) : null,
        body.status != null
            ? body.status.stream().map(GetStepAggregatesInput.Status::valueOf).collect(Collectors.toList())
            : null,
        body.function_name,
        body.workflow_id_prefix,
        body.completed_after != null ? Instant.parse(body.completed_after) : null,
        body.completed_before != null ? Instant.parse(body.completed_before) : null);
  }
}
