package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

interface MgmtService {
  int simpleWorkflow(int input) throws InterruptedException;

  void stepTimingWorkflow() throws InterruptedException;

  String helloWorkflow(String name);
}

class MgmtServiceImpl implements MgmtService {

  private final DBOS dbos;
  private int stepsExecuted;
  public CountDownLatch mainLatch = new CountDownLatch(1);
  public CountDownLatch workLatch = new CountDownLatch(1);

  public MgmtServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public int stepsExecuted() {
    return this.stepsExecuted;
  }

  @Override
  @Workflow(name = "myworkflow")
  public int simpleWorkflow(int input) throws InterruptedException {

    dbos.runStep(() -> ++stepsExecuted, "stepOne");
    mainLatch.countDown();
    workLatch.await();
    dbos.runStep(() -> ++stepsExecuted, "stepTwo");
    dbos.runStep(() -> ++stepsExecuted, "stepThree");

    return input;
  }

  @Override
  @Workflow
  public void stepTimingWorkflow() throws InterruptedException {
    for (var i = 0; i < 5; i++) {
      dbos.runStep(() -> Thread.sleep(100), "stepTimingStep");
    }

    dbos.setEvent("key", "value");
    dbos.listWorkflows(new ListWorkflowsInput());
    dbos.recv(null, Duration.ofSeconds(1));
  }

  @Override
  @Workflow
  public String helloWorkflow(String name) {
    return "Hello, %s!".formatted(name);
  }
}
