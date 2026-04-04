package dev.dbos.transact.scheduled;

import dev.dbos.transact.workflow.Workflow;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

interface ScheduledWorkflowService {
  void scheduledRun(Instant scheduled, Object context);

  void latchedRun(Instant scheduled, Object context);
}

class ScheduledWorkflowImpl implements ScheduledWorkflowService {

  volatile int counter = 0;
  volatile Instant lastScheduled = null;
  volatile Object lastContext = null;
  final List<Instant> allScheduledTimes = new CopyOnWriteArrayList<>();
  final CountDownLatch latch = new CountDownLatch(3);

  @Override
  @Workflow
  public void scheduledRun(Instant scheduled, Object context) {
    ++counter;
    lastScheduled = scheduled;
    lastContext = context;
    allScheduledTimes.add(scheduled);
  }

  @Override
  @Workflow
  public void latchedRun(Instant scheduled, Object context) {
    latch.countDown();
  }

  void reset() {
    counter = 0;
    lastScheduled = null;
    lastContext = null;
    allScheduledTimes.clear();
  }
}
