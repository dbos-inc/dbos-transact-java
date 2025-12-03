package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

public record Invocation(
    String className, String instanceName, String workflowName, Object[] args) {
  public String fqName() {
    return RegisteredWorkflow.fullyQualifiedWFName(className, instanceName, workflowName);
  }
}
