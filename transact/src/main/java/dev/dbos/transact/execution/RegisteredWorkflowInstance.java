package dev.dbos.transact.execution;

public record RegisteredWorkflowInstance(
    String className, String instanceName, Class<?> targetInterface, Object target) {

  public static String fullyQualifiedInstName(String className, String instanceName) {
    return String.format("%s/%s", className, instanceName);
  }
}
