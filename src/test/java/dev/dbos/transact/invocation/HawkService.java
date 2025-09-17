package dev.dbos.transact.invocation;

import java.time.Instant;

public interface HawkService {

  String simpleWorkflow();

  String sleepWorkflow(long sleepSec);

  String parentWorkflow();

  String parentSleepWorkflow(Long timeoutSec, long sleepSec);

  Instant nowStep();

  Instant stepWorkflow();

  String illegalStep();

  String illegalWorkflow();
}
