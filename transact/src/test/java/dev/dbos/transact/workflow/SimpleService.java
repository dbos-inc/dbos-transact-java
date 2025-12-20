package dev.dbos.transact.workflow;

public interface SimpleService {

  public void setSimpleService(SimpleService service);

  public String workWithString(String input);

  public void workWithError() throws Exception;

  public void workWithNonSerializableException();

  public void workWithNonSerializableExceptionInStep();

  public String parentWorkflowWithoutSet(String input);

  public String workflowWithMultipleChildren(String input) throws Exception;

  public String childWorkflow(String input);

  public String childWorkflow2(String input);

  public String childWorkflow3(String input);

  public String childWorkflow4(String input) throws Exception;

  public String grandchildWorkflow(String input);

  public String grandParent(String input) throws Exception;

  String syncWithQueued();

  String longWorkflow(String input);

  void stepWithSleep(long sleepSeconds);

  void stepWithNonSerializableException();

  String longParent(String input, long sleepSeconds, long timeoutSeconds)
      throws InterruptedException;

  String childWorkflowWithSleep(String input, long sleepSeconds) throws InterruptedException;

  String getResultInStep(String wfid);

  String getStatus(String wfid);

  String getStatusInStep(String wfid);

  void startWfInStep();

  void startWfInStepById(String childId);
}
