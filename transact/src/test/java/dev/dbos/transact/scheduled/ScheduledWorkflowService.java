package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

interface ScheduledWorkflowService {
  void scheduledRun(Instant scheduled, Object context);
}

class ScheduledWorkflowImpl implements ScheduledWorkflowService {

  volatile int counter = 0;
  volatile Instant lastScheduled = null;
  volatile Object lastContext = null;
  final List<Instant> allScheduledTimes = new CopyOnWriteArrayList<>();

  @Override
  @Workflow
  public void scheduledRun(Instant scheduled, Object context) {
    ++counter;
    lastScheduled = scheduled;
    lastContext = context;
    allScheduledTimes.add(scheduled);
  }

  void reset() {
    counter = 0;
    lastScheduled = null;
    lastContext = null;
    allScheduledTimes.clear();
  }
}
