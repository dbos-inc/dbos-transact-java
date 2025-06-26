package dev.dbos.transact.context;

public class DBOSContext {
    private volatile String workflowId;
    private String user;
    private volatile int functionId;
    private String stepId;

    public DBOSContext() {

    }
    public DBOSContext(String workflowId, int functionId) {
        this.workflowId = workflowId;
        this.functionId = functionId ;
    }

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

    public DBOSContext copy() {
        return new DBOSContext(workflowId, functionId);
    }
}

