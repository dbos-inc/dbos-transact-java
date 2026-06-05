package dev.dbos.transact.conductor.protocol;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class ForkFromFailureRequest extends BaseMessage {
  public ForkFromFailureBody body;

  public ForkFromFailureRequest() {}

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
