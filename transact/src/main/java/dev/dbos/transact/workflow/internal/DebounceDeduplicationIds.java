package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The deduplication IDs a debounced workflow holds its debounce key under.
 *
 * <p>The key's spelling is changing from {@link #current} to {@link #qualified}, one step per
 * release: a release that reads a spelling ships before the release that writes it, so a call
 * during a rolling upgrade extends the row an earlier call left under either spelling. This release
 * bounces under both, the qualified one first, and still writes the current one, which the previous
 * release reads.
 *
 * <p>The ordering doesn't cover two first calls for one key that run at once on this release and
 * the next: neither finds a row to extend, each enqueues under its own spelling, and since the
 * spellings differ the deduplication constraint doesn't stop the second. Both rows then run, the
 * one under the current spelling with the arguments of the call that created it. Later calls extend
 * the qualified row only. The window closes once no node runs this release.
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
