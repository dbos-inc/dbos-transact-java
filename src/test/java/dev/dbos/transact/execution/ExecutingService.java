package dev.dbos.transact.execution;

public interface ExecutingService {

  String workflowMethod(String input);

  String workflowMethodWithStep(String input);

  String stepOne(String input);

  String stepTwo(String input);

  void sleepingWorkflow(float seconds);

  void setExecutingService(ExecutingService service);
}
