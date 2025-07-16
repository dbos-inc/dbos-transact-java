package dev.dbos.transact.context;

import dev.dbos.transact.Constants;

public class DBOSContext {

    private String executorId = Constants.DEFAULT_EXECUTORID ;

    private String parentWorkflowId;
    private int parentFunctionId;
    private volatile String workflowId;
    private volatile int functionId;

    private int currStepFunctionId ;

    private String authenticatedUser;
    private String authenticateRole;
    private String assumedRole;

    private String appVersion;

    // workflow timeouts
    private int workflowTimeoutMs ;
    private int workflowDeadlineEpochMs ;

    // Queues
    private String deduplicationId ;
    private int priority ;

    //
    private volatile boolean inWorkflow = false;

    private String stepId;

    public DBOSContext() {

    }
    public DBOSContext(String workflowId, int functionId) {
        this.workflowId = workflowId;
        this.functionId = functionId ;
        this.inWorkflow = false;
    }

    public DBOSContext(String workflowId, int functionId, String parentWorkflowId, int parentFunctionId,boolean inWorkflow) {
        this.workflowId = workflowId;
        this.functionId = functionId ;
        this.inWorkflow = inWorkflow;
        this.parentWorkflowId = parentWorkflowId;
        this.parentFunctionId = parentFunctionId;

    }

    private DBOSContext(String childWorkflowId, String parentWorkflowId, int parentFunctionId) {
        this.workflowId = childWorkflowId;
        this.parentWorkflowId = parentWorkflowId;
        this.parentFunctionId = parentFunctionId;
        this.inWorkflow = true;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getParentWorkflowId() { return parentWorkflowId ;}

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getAuthenticatedUser() {
        return authenticatedUser;
    }

    public void setAuthenticatedUser(String user) {
        this.authenticatedUser = user;
    }

    public int getFunctionId() {
        return functionId;
    }

    public int getParentFunctionId() { return parentFunctionId ;}

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
        return new DBOSContext(workflowId, functionId, parentWorkflowId, parentFunctionId, inWorkflow);
    }

    public DBOSContext createChild(String childWorkflowId) {
        return new DBOSContext(childWorkflowId, workflowId, this.getAndIncrementFunctionId());
    }

    public boolean hasParent() {
        return this.parentWorkflowId != null;
    }

    public boolean isInWorkflow() {
        return inWorkflow;
    }

    public void setInWorkflow(boolean in) {
        this.inWorkflow = true;
    }
}

