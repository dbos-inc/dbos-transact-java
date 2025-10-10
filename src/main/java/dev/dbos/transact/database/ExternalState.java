package dev.dbos.transact.database;

import java.util.Objects;

public record ExternalState(
    String service,
    String workflowName,
    String key,
    String value,
    Long updateTime,
    Long updateSeq) {

  public ExternalState {
    Objects.requireNonNull(service);
    Objects.requireNonNull(workflowName);
    Objects.requireNonNull(key);
  }

  public ExternalState(String service, String workflowName, String key) {
    this(service, workflowName, key, null, null, null);
  }

  public ExternalState withValue(String value) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  public ExternalState withUpdateTime(long updateTime) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  public ExternalState withUpdateSeq(long updateSeq) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }
}
