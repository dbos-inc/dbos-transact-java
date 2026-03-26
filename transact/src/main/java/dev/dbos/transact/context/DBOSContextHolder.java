package dev.dbos.transact.context;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

public class DBOSContextHolder {
  private static final ThreadLocal<DBOSContext> contextHolder =
      ThreadLocal.withInitial(DBOSContext::new);

  public static @NonNull DBOSContext get() {
    return contextHolder.get();
  }

  public static void clear() {
    contextHolder.remove();
  }

  public static void set(@NonNull DBOSContext context) {
    contextHolder.set(Objects.requireNonNull(context));
  }
}
