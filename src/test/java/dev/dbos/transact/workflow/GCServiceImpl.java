package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class GCServiceImpl implements GCService {

  GCService self;
  CountDownLatch gcLatch = new CountDownLatch(1);
  CountDownLatch timeoutLatch = new CountDownLatch(1);

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

    return DBOS.workflowId();
  }

  @Workflow
  public String timeoutBlockedWorkflow() {
    while (timeoutLatch.getCount() > 0) {
      DBOS.sleep(Duration.ofMillis(100));
    }
    return DBOS.workflowId();
  }
}
