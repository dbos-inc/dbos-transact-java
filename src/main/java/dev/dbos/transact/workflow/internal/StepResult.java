package dev.dbos.transact.workflow.internal;

public class StepResult {
    private String workflowId;
    private int functionId;
    private String functionName;
    private String output;
    private String error;

    public StepResult() {
    }

    public StepResult(String workflowId, int functionID, String functionName, String output, String error) {
        this.workflowId = workflowId;
        this.functionId = functionID;
        this.functionName = functionName;
        this.output = output;
        this.error = error;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public int getFunctionId() {
        return functionId;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getOutput() {
        return output;
    }

    public String getError() {
        return error;
    }

    public void setWorkflowId(String workflowUUID) {
        this.workflowId = workflowUUID;
    }

    public void setFunctionId(int functionID) {
        this.functionId = functionID;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public void setError(String error) {
        this.error = error;
    }
}
