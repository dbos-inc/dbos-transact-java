package dev.dbos.transact.workflow;

import dev.dbos.transact.Constants;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The workflow currently holding a {@code (queue_name, deduplication_id)} pair, the application
 * that owns it, and what kind of workflow it is.
 *
 * <p>The deduplication index is global across the applications sharing a system database, so a
 * holder may belong to a peer. Callers that mean to coordinate with the holder — the debouncers —
 * must check before they do, since a peer's holder answers to that peer alone.
 *
 * <p>A debounce key can be held by two kinds of workflow. A debouncer service workflow absorbs
 * calls for the key and starts the user workflow when the period elapses. A debounced user workflow
 * waits DELAYED on its own, holding the key until its delay expires. The debouncers tell them apart
 * with {@link #isDebouncerService()} and {@link #isDebouncedInstanceOf}.
 *
 * @param workflowId the holding workflow
 * @param applicationName the application that owns it, or null if the row is unclaimed
 * @param workflowName the holding workflow's name, or null if unknown
 * @param className the holding workflow's class name, or null if unknown
 * @param instanceName the holding workflow's instance name, or null if it has none or is unknown
 * @param status the holding workflow's status, or null if unknown
 * @param isDebounced whether the holder is a debounced workflow whose deduplication ID is a
 *     debounce key
 */
public record DeduplicationHolder(
    String workflowId,
    @Nullable String applicationName,
    @Nullable String workflowName,
    @Nullable String className,
    @Nullable String instanceName,
    @Nullable WorkflowState status,
    boolean isDebounced) {

  /**
   * Whether this holder answers to some other application than {@code appName}. An unclaimed holder
   * belongs to everyone, and a caller acting for no application in particular is in no position to
   * call anything foreign, so neither case is foreign.
   */
  public boolean isForeignTo(@Nullable String appName) {
    return appName != null && applicationName != null && !applicationName.equals(appName);
  }

  /**
   * Whether this holder is a debouncer service workflow: one that absorbs debounce calls for its
   * key over messages and starts the user workflow itself. The service workflow has a fixed name
   * and class, and both must match; a user workflow that shares its name is not one. A holder whose
   * name is unknown was recorded before debounced workflows existed, when the service workflow was
   * the only thing that ever held a debounce key, so it counts as one.
   */
  @JsonIgnore // derived, not a component: keep it out of the recorded step
  public boolean isDebouncerService() {
    return workflowName == null
        || (Constants.DEBOUNCER_WORKFLOW_NAME.equals(workflowName)
            && Constants.DEBOUNCER_CLASS_NAME.equals(className));
  }

  /**
   * Whether this holder is a debounced instance of the given workflow: the user workflow itself,
   * holding its debounce key while it waits. Matches the name, class and instance, so a
   * debounce-key collision between different workflows is never mistaken for a holder to extend. No
   * instance is null, as the row spells it.
   */
  public boolean isDebouncedInstanceOf(
      @NonNull String workflowName, @NonNull String className, @Nullable String instanceName) {
    return isDebounced
        && Objects.equals(this.workflowName, workflowName)
        && Objects.equals(this.className, className)
        && Objects.equals(this.instanceName, instanceName);
  }
}
