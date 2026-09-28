package dev.dbos.transact.workflow;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * A workflow as export writes it and import reads it back.
 *
 * <p>The payloads (the workflow's inputs, output and error, and each step's output and error)
 * travel in {@code payloads} exactly as the system database stores them, never deserialized. Export
 * and import go through JSON that carries no type information, so a payload deserialized into
 * {@code status} or {@code steps} would come back as generic maps and lists. On export, the payload
 * fields of {@code status} and {@code steps} are therefore left null.
 *
 * @param payloads the stored payloads, or null in an export written before they were carried this
 *     way; import then falls back to re-serializing the payload fields of {@code status} and {@code
 *     steps}
 */
public record ExportedWorkflow(
    WorkflowStatus status,
    List<StepInfo> steps,
    List<WorkflowEvent> events,
    List<WorkflowEventHistory> eventHistory,
    List<WorkflowStream> streams,
    @Nullable SerializedPayloads payloads) {

  /**
   * A workflow's payloads exactly as stored: the serialized strings and the format that wrote them.
   *
   * @param serialization the workflow's serialization format
   * @param steps one entry per step, in step ID order
   */
  public record SerializedPayloads(
      @Nullable String serialization,
      @Nullable String inputs,
      @Nullable String output,
      @Nullable String error,
      List<SerializedStep> steps) {}

  /** A step's output and error exactly as stored in {@code operation_outputs}. */
  public record SerializedStep(int stepId, @Nullable String output, @Nullable String error) {}
}
