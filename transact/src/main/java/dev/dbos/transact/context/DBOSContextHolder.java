package dev.dbos.transact.context;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

public class DBOSContextHolder {
  private static final ThreadLocal<DBOSContext> contextHolder =
      ThreadLocal.withInitial(DBOSContext::new);

  public static @NonNull DBOSContext get() {
    // contextHolder should always be non null. Even if cleared, it should be set to the initial
    // value.
    return Objects.requireNonNull(contextHolder.get());
  }

  public static void clear() {
    contextHolder.remove();
  }

  public static void set(@NonNull DBOSContext context) {
    contextHolder.set(Objects.requireNonNull(context));
  }
}
