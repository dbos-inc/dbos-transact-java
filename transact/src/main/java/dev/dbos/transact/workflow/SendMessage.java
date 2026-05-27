package dev.dbos.transact.workflow;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record SendMessage(
    @NonNull String destinationId,
    @NonNull Object message,
    @Nullable String topic,
    @Nullable String idempotencyKey) {

  public SendMessage(@NonNull String destinationId, @NonNull Object message) {
    this(destinationId, message, null, null);
  }

  public SendMessage(
      @NonNull String destinationId, @NonNull Object message, @Nullable String topic) {
    this(destinationId, message, topic, null);
  }
}
