package dev.dbos.transact.workflow;

public class StepInfo {
    private final int functionId;
    private final String functionName;
    private final Object output;
    private final Exception error;
    private final String childWorkflowId;

    public StepInfo(int functionId, String functionName, Object output, Exception error, String childWorkflowId) {
        this.functionId = functionId;
        this.functionName = functionName;
        this.output = output;
        this.error = error;
        this.childWorkflowId = childWorkflowId;
    }

    public int getFunctionId() {
        return functionId;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Object getOutput() {
        return output;
    }

    public Exception getError() {
        return error;
    }

    public String getChildWorkflowId() {
        return childWorkflowId;
    }

    @Override
    public String toString() {
        return "StepInfo{" + "functionId=" + functionId + ", functionName='" + functionName + '\'' + ", output="
                + output + ", error=" + error + ", childWorkflowId='" + childWorkflowId + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        StepInfo stepInfo = (StepInfo) o;

        if (functionId != stepInfo.functionId)
            return false;
        if (!functionName.equals(stepInfo.functionName))
            return false;
        if (output != null ? !output.equals(stepInfo.output) : stepInfo.output != null)
            return false;
        if (error != null ? !error.equals(stepInfo.error) : stepInfo.error != null)
            return false;
        return childWorkflowId != null
                ? childWorkflowId.equals(stepInfo.childWorkflowId)
                : stepInfo.childWorkflowId == null;
    }

    @Override
    public int hashCode() {
        int result = functionId;
        result = 31 * result + functionName.hashCode();
        result = 31 * result + (output != null ? output.hashCode() : 0);
        result = 31 * result + (error != null ? error.hashCode() : 0);
        result = 31 * result + (childWorkflowId != null ? childWorkflowId.hashCode() : 0);
        return result;
    }
}
