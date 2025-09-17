package dev.dbos.transact.devhawk;

import java.time.Duration;

public interface HawkService {

  String simpleWorkflow();

  String sleepWorkflow(long sleepSec);

  String parentWorkflow();

  String parentSleepWorkflow(Long timeoutSec, long sleepSec);
}
