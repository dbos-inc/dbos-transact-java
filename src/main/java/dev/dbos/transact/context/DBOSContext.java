package dev.dbos.transact.context;

public class DBOSContext {
    private volatile String workflowId;
    private String user;
    private int functionId;
    private String stepId;

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getFunctionId() {
        return functionId;
    }

    public int getAndIncrementFunctionId() {
        return functionId++;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }
}

