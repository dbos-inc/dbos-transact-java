package dev.dbos.transact.directinvocation;

public interface HawkService {

  String simpleWorkflow();

  String sleepWorkflow(long sleepSec);

  String parentWorkflow();

  String parentSleepWorkflow(Long timeoutSec, long sleepSec);
}
