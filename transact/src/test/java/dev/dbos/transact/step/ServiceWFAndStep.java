package dev.dbos.transact.step;

public interface ServiceWFAndStep {

  void setSelf(ServiceWFAndStep serviceWFAndStep);

  String aWorkflow(String input);

  String stepOne(String input);

  String stepTwo(String input);

  String aWorkflowWithInlineSteps(String input);

  String stepWith2Retries(String input) throws Exception;

  String stepWithNoRetriesAllowed(String input) throws Exception;

  String stepWithLongRetry(String input) throws Exception;

  String stepRetryWorkflow(String input);

  String inlineStepRetryWorkflow(String input);
}
