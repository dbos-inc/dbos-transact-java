package dev.dbos.transact.workflow;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public record NotificationInfo(String topic, Object message, Instant createdAt, boolean consumed) {

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Long createdAtMs() {
    return createdAt == null ? null : createdAt.toEpochMilli();
  }
}
