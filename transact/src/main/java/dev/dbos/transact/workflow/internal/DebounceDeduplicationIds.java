package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The deduplication IDs a debounced workflow holds its debounce key under.
 *
 * <p>The key's spelling is changing from {@link #current} to {@link #qualified}, one step per
 * release, so that no rolling upgrade runs two coalescers for one key: a release that reads a
 * spelling ships before the release that writes it. This release bounces under both, the qualified
 * one first, and still writes the current one, which the previous release reads.
 */
public final class DebounceDeduplicationIds {

  private DebounceDeduplicationIds() {}

  /**
   * The spelling written today: {@code workflowName-debounceKey}. Workflows that share a name
   * across classes or instances share it too; the bounce's match on name, class and instance is
   * what keeps such a collision from extending another workflow.
   */
  public static @NonNull String current(@NonNull String workflowName, @NonNull String debounceKey) {
    return Objects.requireNonNull(workflowName, "workflowName must not be null")
        + "-"
        + Objects.requireNonNull(debounceKey, "debounceKey must not be null");
  }

  /**
   * The qualified spelling: the workflow's {@linkplain RegisteredWorkflow#fullyQualifiedName fully
   * qualified name}, {@code workflowName/className/instanceName}, then {@code -debounceKey}. An
   * absent instance name is the empty string, as in the fully qualified name.
   */
  public static @NonNull String qualified(
      @NonNull String workflowName,
      @NonNull String className,
      @Nullable String instanceName,
      @NonNull String debounceKey) {
    return RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName)
        + "-"
        + Objects.requireNonNull(debounceKey, "debounceKey must not be null");
  }
}
