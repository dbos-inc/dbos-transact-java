package dev.dbos.transact.workflow;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MgmtServiceImpl implements MgmtService {

  private static final Logger logger = LoggerFactory.getLogger(MgmtServiceImpl.class);

  private volatile int stepsExecuted;
  CountDownLatch mainThreadEvent;
  CountDownLatch workflowEvent;

  MgmtService service;

  public MgmtServiceImpl(CountDownLatch mainLatch, CountDownLatch workLatch) {
    this.mainThreadEvent = mainLatch;
    this.workflowEvent = workLatch;
  }

  public void setMgmtService(MgmtService m) {
    service = m;
  }

  @Workflow(name = "myworkflow")
  public int simpleWorkflow(int input) {

    try {
      service.stepOne();
      mainThreadEvent.countDown();
      workflowEvent.await();
      service.stepTwo();
      service.stepThree();

    } catch (InterruptedException e) {
      logger.error("simpleWorkflow interrupted", e);
    }

    return input;
  }

  @Step(name = "one")
  public void stepOne() {
    ++stepsExecuted;
  }

  @Step(name = "two")
  public void stepTwo() {
    ++stepsExecuted;
  }

  @Step(name = "three")
  public void stepThree() {
    ++stepsExecuted;
  }

  public synchronized int getStepsExecuted() {
    return stepsExecuted;
  }
}
