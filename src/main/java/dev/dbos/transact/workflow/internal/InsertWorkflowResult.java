package dev.dbos.transact.workflow.internal;

public class InsertWorkflowResult {

    private int recoveryAttempts;
    private String status;
    private String name;
    private String className;
    private String configName;
    private String queueName;
    private Long workflowDeadlineEpochMs;

    public InsertWorkflowResult(int recoveryAttempts, String status, String name,
            String className, String configName, String queueName,
            Long workflowDeadlineEpochMs) {
        this.recoveryAttempts = recoveryAttempts;
        this.status = status;
        this.name = name;
        this.className = className;
        this.configName = configName;
        this.queueName = queueName;
        this.workflowDeadlineEpochMs = workflowDeadlineEpochMs;
    }

    public int getRecoveryAttempts() {
        return recoveryAttempts;
    }

    public String getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getConfigName() {
        return configName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Long getWorkflowDeadlineEpochMs() {
        return workflowDeadlineEpochMs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + recoveryAttempts;
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((className == null) ? 0 : className.hashCode());
        result = prime * result + ((configName == null) ? 0 : configName.hashCode());
        result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
        result = prime * result + ((workflowDeadlineEpochMs == null) ? 0
                : workflowDeadlineEpochMs.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InsertWorkflowResult other = (InsertWorkflowResult) obj;
        if (recoveryAttempts != other.recoveryAttempts)
            return false;
        if (status == null) {
            if (other.status != null)
                return false;
        }
        else if (!status.equals(other.status))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        }
        else if (!name.equals(other.name))
            return false;
        if (className == null) {
            if (other.className != null)
                return false;
        }
        else if (!className.equals(other.className))
            return false;
        if (configName == null) {
            if (other.configName != null)
                return false;
        }
        else if (!configName.equals(other.configName))
            return false;
        if (queueName == null) {
            if (other.queueName != null)
                return false;
        }
        else if (!queueName.equals(other.queueName))
            return false;
        if (workflowDeadlineEpochMs == null) {
            if (other.workflowDeadlineEpochMs != null)
                return false;
        }
        else if (!workflowDeadlineEpochMs.equals(other.workflowDeadlineEpochMs))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "InsertWorkflowResult{" + "recoveryAttempts=" + recoveryAttempts
                + ", status='" + status + '\'' + ", name='" + name + '\''
                + ", className='" + className + '\'' + ", configName='" + configName
                + '\'' + ", queueName='" + queueName + '\'' + ", workflowDeadlineEpochMs="
                + workflowDeadlineEpochMs + '}';
    }
}
