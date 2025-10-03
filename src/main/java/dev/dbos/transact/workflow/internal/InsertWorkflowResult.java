package dev.dbos.transact.workflow.internal;

public class InsertWorkflowResult {

  private int recoveryAttempts;
  private String status;
  private String name;
  private String className;
  private String instanceName;
  private String queueName;
  private Long workflowDeadlineEpochMs;

  public InsertWorkflowResult(
      int recoveryAttempts,
      String status,
      String name,
      String className,
      String instanceName,
      String queueName,
      Long workflowDeadlineEpochMs) {
    this.recoveryAttempts = recoveryAttempts;
    this.status = status;
    this.name = name;
    this.className = className;
    this.instanceName = instanceName;
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

  public String getInstanceName() {
    return instanceName;
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
    result = prime * result + ((instanceName == null) ? 0 : instanceName.hashCode());
    result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
    result =
        prime * result
            + ((workflowDeadlineEpochMs == null) ? 0 : workflowDeadlineEpochMs.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    InsertWorkflowResult other = (InsertWorkflowResult) obj;
    if (recoveryAttempts != other.recoveryAttempts) return false;
    if (status == null) {
      if (other.status != null) return false;
    } else if (!status.equals(other.status)) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (className == null) {
      if (other.className != null) return false;
    } else if (!className.equals(other.className)) return false;
    if (instanceName == null) {
      if (other.instanceName != null) return false;
    } else if (!instanceName.equals(other.instanceName)) return false;
    if (queueName == null) {
      if (other.queueName != null) return false;
    } else if (!queueName.equals(other.queueName)) return false;
    if (workflowDeadlineEpochMs == null) {
      if (other.workflowDeadlineEpochMs != null) return false;
    } else if (!workflowDeadlineEpochMs.equals(other.workflowDeadlineEpochMs)) return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format(
        "InsertWorkflowResult{recoveryAttempts=%d, status='%s', name='%s', className='%s', instanceName='%s', queueName='%s', workflowDeadlineEpochMs=%s}",
        recoveryAttempts,
        status,
        name,
        className,
        instanceName,
        queueName,
        workflowDeadlineEpochMs);
  }
}
