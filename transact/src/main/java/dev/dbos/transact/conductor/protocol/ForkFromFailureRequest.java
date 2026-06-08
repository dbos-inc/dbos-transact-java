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
    Objects.requireNonNull(body.workflow_ids, "ForkFromFailureBody workflow_ids must not be null");

    var options =
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
