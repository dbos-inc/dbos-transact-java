package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Scheduled;
import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

interface InvalidSig {
  void scheduledWF(Instant scheduled, String actual);
}

class InvalidSigImpl implements InvalidSig {

  @Override
  @Workflow
  @Scheduled(cron = "0/1 * * * * ?")
  public void scheduledWF(Instant scheduled, String actual) {}
}
