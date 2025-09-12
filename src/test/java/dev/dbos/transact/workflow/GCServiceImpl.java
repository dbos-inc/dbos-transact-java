package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContext;

import java.util.concurrent.CountDownLatch;

public class GCServiceImpl implements GCService {

  private final DBOS dbos;
  GCService self;
  CountDownLatch gcLatch = new CountDownLatch(1);
  CountDownLatch timeoutLatch = new CountDownLatch(1);

  public GCServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public void setGCService(GCService service) {
    this.self = service;
  }

  @Step
  public int testStep(int x) {
    return x;
  }

  @Workflow
  public int testWorkflow(int x) {
    self.testStep(x);
    return x;
  }

  @Workflow
  public String gcBlockedWorkflow() {
    try {
      gcLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    return DBOSContext.workflowId().get();
  }

  @Workflow
  public String timeoutBlockedWorkflow() {
    while (timeoutLatch.getCount() > 0) {
      dbos.sleep(0.1f);
    }
    return DBOSContext.workflowId().get();
  }
}
