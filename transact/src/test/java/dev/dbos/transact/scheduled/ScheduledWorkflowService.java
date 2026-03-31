package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;

interface ScheduledWorkflowService {
  void scheduledRun(Instant scheduled, Object context);
}

class ScheduledWorkflowImpl implements ScheduledWorkflowService {

  volatile int counter = 0;
  volatile Instant lastScheduled = null;
  volatile Object lastContext = null;

  @Override
  @Workflow
  public void scheduledRun(Instant scheduled, Object context) {
    ++counter;
    lastScheduled = scheduled;
    lastContext = context;
  }
}
