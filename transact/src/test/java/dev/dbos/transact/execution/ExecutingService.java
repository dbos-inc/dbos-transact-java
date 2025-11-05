package dev.dbos.transact.execution;

public interface ExecutingService {

  String workflowMethod(String input);

  String workflowMethodWithStep(String input);

  String stepOne(String input);

  String stepTwo(String input);

  void sleepingWorkflow(double seconds);

  void setExecutingService(ExecutingService service);

  void stepWithNoReturn();

  public static class MyAppException extends Exception {
    public MyAppException() {
      super("You asked for it");
    }
  }

  void stepThatThrows() throws MyAppException;

  void workflowWithNoResultSteps();
}
