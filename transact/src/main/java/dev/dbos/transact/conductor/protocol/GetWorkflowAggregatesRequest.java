package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetWorkflowAggregatesRequest extends BaseMessage {

  public Body body;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Body {
    public boolean group_by_status;
    public boolean group_by_name;
    public boolean group_by_queue_name;
    public boolean group_by_executor_id;
    public boolean group_by_application_version;
    // select metric flags
    public Boolean select_count;
    public Boolean select_min_created_at;
    public Boolean select_max_queue_wait_ms;
    public Boolean select_max_total_latency_ms;
    // optional time bucketing
    public Long time_bucket_size_ms;
    // filters
    public List<String> status;
    public String start_time;
    public String end_time;
    public String completed_after;
    public String completed_before;
    public String dequeued_after;
    public String dequeued_before;
    public List<String> name;
    public List<String> app_version;
    public List<String> executor_id;
    public List<String> queue_name;
    public List<String> workflow_id_prefix;
  }

  public GetWorkflowAggregatesInput toInput() {
    if (body == null) {
      return new GetWorkflowAggregatesInput();
    }
    // Default select_count=true when no select flags are specified (backward compat)
    boolean anySelect =
        Boolean.TRUE.equals(body.select_count)
            || Boolean.TRUE.equals(body.select_min_created_at)
            || Boolean.TRUE.equals(body.select_max_queue_wait_ms)
            || Boolean.TRUE.equals(body.select_max_total_latency_ms);
    boolean selectCount = anySelect ? Boolean.TRUE.equals(body.select_count) : true;
    return new GetWorkflowAggregatesInput(
        body.group_by_status,
        body.group_by_name,
        body.group_by_queue_name,
        body.group_by_executor_id,
        body.group_by_application_version,
        selectCount,
        Boolean.TRUE.equals(body.select_min_created_at),
        Boolean.TRUE.equals(body.select_max_queue_wait_ms),
        Boolean.TRUE.equals(body.select_max_total_latency_ms),
        body.time_bucket_size_ms != null ? Duration.ofMillis(body.time_bucket_size_ms) : null,
        body.name,
        body.status != null
            ? body.status.stream()
                .map(
                    s -> {
                      try {
                        return WorkflowState.valueOf(s);
                      } catch (IllegalArgumentException e) {
                        var valid =
                            java.util.Arrays.stream(WorkflowState.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", "));
                        throw new IllegalArgumentException(
                            "Invalid workflow status '" + s + "'. Valid values: " + valid);
                      }
                    })
                .collect(Collectors.toList())
            : null,
        body.queue_name,
        body.executor_id,
        body.app_version,
        body.workflow_id_prefix,
        body.start_time != null ? Instant.parse(body.start_time) : null,
        body.end_time != null ? Instant.parse(body.end_time) : null,
        body.completed_after != null ? Instant.parse(body.completed_after) : null,
        body.completed_before != null ? Instant.parse(body.completed_before) : null,
        body.dequeued_after != null ? Instant.parse(body.dequeued_after) : null,
        body.dequeued_before != null ? Instant.parse(body.dequeued_before) : null);
  }
}
