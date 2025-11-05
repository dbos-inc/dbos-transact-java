package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

interface InvalidCron {
  void scheduledWF(Instant scheduled, Instant actual);
}

class InvalidCronImpl implements InvalidCron {

  @Override
  @Workflow
  @Scheduled(cron = "* * * * *")
  public void scheduledWF(Instant scheduled, Instant actual) {}
}
