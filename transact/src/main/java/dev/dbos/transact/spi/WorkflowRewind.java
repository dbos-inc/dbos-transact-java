package dev.dbos.transact.spi;

import java.util.Objects;

/**
 * One rewind in a batch: the workflow to rewind, and the step it is rewound to. The workflow's
 * history from {@code startStep} on is discarded, and it runs again from there under its own ID.
 *
 * @param workflowId the workflow to rewind
 * @param startStep the step to rewind to; 0 is the whole history
 */
public record WorkflowRewind(String workflowId, int startStep) {
  public WorkflowRewind {
    Objects.requireNonNull(workflowId, "workflowId must not be null");
  }
}
