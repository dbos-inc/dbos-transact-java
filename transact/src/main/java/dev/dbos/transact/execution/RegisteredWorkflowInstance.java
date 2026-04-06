package dev.dbos.transact.execution;

import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record RegisteredWorkflowInstance(String className, String instanceName, Object target) {

  public static String fullyQualifiedInstName(
      @NonNull String className, @Nullable String instanceName) {
    return String.format(
        "%s/%s", Objects.requireNonNull(className), Objects.requireNonNullElse(instanceName, ""));
  }
}
