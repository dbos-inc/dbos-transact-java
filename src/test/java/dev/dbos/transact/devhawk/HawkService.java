package dev.dbos.transact.devhawk;

public interface HawkService {

  String simpleWorkflow(String workflowId);

  String sleepWorkflow(long sleepSeconds);

  String parentWorkflow(String workflowId);

  String parentSleepWorkflow(long sleepSeconds);
}
