package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;

public record Invocation(
    DBOSExecutor executor,
    String workflowName,
    String className,
    String instanceName,
    Object[] args) {

  public String fqName() {
    return RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
  }
}
