package dev.dbos.transact.exceptions;

public class WorkflowFunctionNotFoundException extends DBOSException {
    private String workflowName ;
    public WorkflowFunctionNotFoundException(String name) {
        super(ErrorCode.WORKFLOW_FUNCTION_NOT_FOUND.getCode(),
                String.format("Workflow function does not exist for workflow %s.", name));
        this.workflowName = name;

    }

    public String getWorkflowName() {
        return workflowName;
    }
}
