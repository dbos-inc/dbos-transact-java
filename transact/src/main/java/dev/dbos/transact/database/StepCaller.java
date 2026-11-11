package dev.dbos.transact.database;

/**
 * The workflow execution a DBOS operation, such as a send or a cancel, is made from. The operation
 * lands only while that execution still owns the workflow.
 */
public sealed interface StepCaller {
  /** The calling workflow. */
  String workflowId();

  /** The token of the calling workflow's execution. */
  String ownerXid();

  /** A call from the workflow body, recorded as the workflow's step {@code stepId}. */
  record FromWorkflow(String workflowId, int stepId, String ownerXid) implements StepCaller {}

  /**
   * A call from inside a step, which records nothing of its own but is still checked against the
   * owner.
   */
  record FromStep(String workflowId, String ownerXid) implements StepCaller {}
}
