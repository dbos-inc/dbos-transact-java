package dev.dbos.transact.exceptions;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED_STEP;

public class UnexpectedStepException extends DBOSException {
    private final String workflowId;
    private final int stepId;
    private final String expectedName;
    private final String recordedName;

    public UnexpectedStepException(String workflowId, int stepId, String expectedName,
            String recordedName) {
        super(UNEXPECTED_STEP.getCode(), String.format(
                "During execution of workflow %s step %s, function %s was recorded when %s was expected. Check that your workflow is deterministic.",
                workflowId,
                stepId,
                recordedName,
                expectedName));
        this.workflowId = workflowId;
        this.stepId = stepId;
        this.expectedName = expectedName;
        this.recordedName = recordedName;
    }
}
