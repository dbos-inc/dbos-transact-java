package dev.dbos.transact.database;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

public record ExternalState(
    String service,
    String workflowName,
    String key,
    String value,
    BigDecimal updateTime,
    BigInteger updateSeq) {

  public ExternalState {
    Objects.requireNonNull(service, "service must not be null");
    Objects.requireNonNull(workflowName, "workflowName must not be null");
    Objects.requireNonNull(key, "key must not be null");
  }

  public ExternalState(String service, String workflowName, String key) {
    this(service, workflowName, key, null, null, null);
  }

  public ExternalState withValue(String value) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  public ExternalState withUpdateTime(BigDecimal updateTime) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }

  public ExternalState withUpdateSeq(BigInteger updateSeq) {
    return new ExternalState(service, workflowName, key, value, updateTime, updateSeq);
  }
}
