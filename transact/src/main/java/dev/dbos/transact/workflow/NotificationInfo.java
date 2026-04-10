package dev.dbos.transact.workflow;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record NotificationInfo(String topic, Object message, Instant createdAt, boolean consumed) {

  @JsonIgnore
  public Long createdAtEpochMs() {
    return createdAt == null ? null : createdAt.toEpochMilli();
  }
}
