package dev.dbos.transact.context;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

public class DBOSContextHolder {
  private static final ThreadLocal<DBOSContext> contextHolder =
      ThreadLocal.withInitial(DBOSContext::new);

  public static @NonNull DBOSContext get() {
    // contextHolder is never null. Even if cleared, it gets set back to the initial value.
    return Objects.requireNonNull(contextHolder.get());
  }

  public static void clear() {
    contextHolder.remove();
  }

  public static void set(@NonNull DBOSContext context) {
    contextHolder.set(Objects.requireNonNull(context));
  }
}
