package dev.dbos.transact.conductor;

import java.util.Objects;

/**
 * One fork in a batch: the workflow to fork, the ID its fork is written under, and the step the
 * fork starts from. Steps before {@code startStep} are copied from the original; the fork runs the
 * rest.
 *
 * @param workflowId the workflow to fork
 * @param forkedWorkflowId the fork's ID
 * @param startStep the step the fork starts from
 */
public record WorkflowFork(String workflowId, String forkedWorkflowId, int startStep) {
  public WorkflowFork {
    Objects.requireNonNull(workflowId, "workflowId must not be null");
    Objects.requireNonNull(forkedWorkflowId, "forkedWorkflowId must not be null");
  }
}
