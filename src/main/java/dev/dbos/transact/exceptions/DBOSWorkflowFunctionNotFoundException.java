package dev.dbos.transact.exceptions;

public class DBOSWorkflowFunctionNotFoundException extends RuntimeException {
  private String workflowId;
  private String workflowName;

  public DBOSWorkflowFunctionNotFoundException(String id, String name) {
    super(String.format("Workflow function %s does not exist for workflow id %s.", name, id));
    this.workflowName = name;
    this.workflowId = id;
  }

  public String workflowName() {
    return workflowName;
  }

  public String workflowId() {
    return workflowId;
  }
}
