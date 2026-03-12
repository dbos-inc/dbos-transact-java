package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

interface GCTestService {

  int testWorkflow(int x);

  String gcBlockedWorkflow() throws InterruptedException;

  String timeoutBlockedWorkflow();
}

class GCTestServiceImpl implements GCTestService {

  private final DBOS dbos;

  public CountDownLatch gcLatch = new CountDownLatch(1);
  public CountDownLatch timeoutLatch = new CountDownLatch(1);

  public GCTestServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public int testWorkflow(int x) {
    dbos.runStep(() -> x, "testStep");
    return x;
  }

  @Workflow
  @Override
  public String gcBlockedWorkflow() throws InterruptedException {
    gcLatch.await();
    return DBOS.workflowId();
  }

  @Workflow
  @Override
  public String timeoutBlockedWorkflow() {
    while (timeoutLatch.getCount() > 0) {
      dbos.sleep(Duration.ofMillis(100));
    }
    return DBOS.workflowId();
  }
}
