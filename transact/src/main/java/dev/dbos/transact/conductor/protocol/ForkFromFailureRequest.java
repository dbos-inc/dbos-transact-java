package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ForkFromFailureOptions;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class ForkFromFailureRequest extends BaseMessage {
  public ForkFromFailureBody body;

  public ForkFromFailureRequest() {}

  public ForkFromFailureOptions toOptions() {
    Objects.requireNonNull(body, "ForkFromFailureRequest body must not be null");
    Objects.requireNonNull(body.workflow_ids, "workflow_ids must not be null");

    int modeCount =
        (Boolean.TRUE.equals(body.from_last_failure) ? 1 : 0)
            + (Boolean.TRUE.equals(body.from_last_step) ? 1 : 0)
            + (body.from_step != null ? 1 : 0)
            + (body.from_step_name != null ? 1 : 0);
    if (modeCount == 0) {
      throw new IllegalArgumentException(
          "exactly one of from_last_failure, from_last_step, from_step, or from_step_name must be set");
    }
    if (modeCount > 1) {
      throw new IllegalArgumentException(
          "exactly one of from_last_failure, from_last_step, from_step, or from_step_name must be set; got "
              + modeCount);
    }

    ForkFromFailureOptions options =
        Boolean.TRUE.equals(body.from_last_failure)
            ? new ForkFromFailureOptions.FromLastFailure()
            : Boolean.TRUE.equals(body.from_last_step)
                ? new ForkFromFailureOptions.FromLastStep()
                : body.from_step != null
                    ? new ForkFromFailureOptions.FromStep(body.from_step)
                    : new ForkFromFailureOptions.FromStepName(body.from_step_name);

    return options
        .withApplicationVersion(body.application_version)
        .withQueue(body.queue_name)
        .withQueuePartitionKey(body.queue_partition_key);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ForkFromFailureBody {
    public List<String> workflow_ids;
    public String application_version; // optional
    public String queue_name; // optional
    public String queue_partition_key; // optional
    public Boolean from_last_failure; // optional
    public Boolean from_last_step; // optional
    public Integer from_step; // optional
    public String from_step_name; // optional
  }
}
