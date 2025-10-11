package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class EventsServiceImpl implements EventsService {

  private final CountDownLatch getReadyLatch = new CountDownLatch(1);

  EventsServiceImpl() {}

  @Workflow(name = "setEventWorkflow")
  public String setEventWorkflow(String key, String value) {
    DBOS.setEvent(key, value);
    DBOS.runStep(
        () -> {
          DBOS.setEvent(key + "-fromstep", value);
        },
        "stepSetEvent");
    return DBOS.runStep(
        () -> {
          return (String)
              DBOS.getEvent(DBOS.workflowId(), key + "-fromstep", Duration.ofSeconds(0));
        },
        "getEventInStep");
  }

  @Workflow(name = "getEventWorkflow")
  public Object getEventWorkflow(String workflowId, String key, Duration timeOut) {
    return DBOS.getEvent(workflowId, key, timeOut);
  }

  @Workflow(name = "setMultipleEvents")
  public void setMultipleEvents() {
    DBOS.setEvent("key1", "value1");
    DBOS.setEvent("key2", Double.valueOf(241.5));
    DBOS.setEvent("key3", null);
  }

  @Workflow(name = "setWithLatch")
  public void setWithLatch(String key, String value) {
    try {
      System.out.printf("workflowId is %s %b%n", DBOS.workflowId(), DBOS.inWorkflow());
      getReadyLatch.await();
      Thread.sleep(1000); // delay so that get goes and awaits notification
      DBOS.setEvent(key, value);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
  }

  @Workflow(name = "getWithlatch")
  public Object getWithlatch(String workflowId, String key, Duration timeOut) {
    getReadyLatch.countDown();
    return DBOS.getEvent(workflowId, key, timeOut);
  }

  private CountDownLatch advanceSetLatch = new CountDownLatch(1);

  public void advanceSet() {
    advanceSetLatch.countDown();
  }

  private CountDownLatch advanceGetLatch1 = new CountDownLatch(1);

  public void advanceGet1() {
    advanceGetLatch1.countDown();
  }

  private CountDownLatch advanceGetLatch2 = new CountDownLatch(1);

  public void advanceGet2() {
    advanceGetLatch2.countDown();
  }

  private CountDownLatch doneSetLatch1 = new CountDownLatch(1);

  public void awaitSetLatch1() throws InterruptedException {
    doneSetLatch1.await();
  }

  private CountDownLatch doneSetLatch2 = new CountDownLatch(1);

  public void awaitSetLatch2() throws InterruptedException {
    doneSetLatch2.await();
  }

  private CountDownLatch doneGetLatch1 = new CountDownLatch(1);

  public void awaitGetLatch1() throws InterruptedException {
    doneGetLatch1.await();
  }

  @Workflow(name = "setEventTwice")
  public void setEventTwice(String key, String v1, String v2) throws InterruptedException {
    DBOS.setEvent(key, v1);
    doneSetLatch1.countDown();
    advanceSetLatch.await();
    DBOS.setEvent(key, v2);
    doneSetLatch2.countDown();
  }

  @Workflow(name = "getEventTwice")
  public String getEventTwice(String wfid, String key) throws InterruptedException {
    advanceGetLatch1.await();
    var v1 = (String) DBOS.getEvent(wfid, key, Duration.ofSeconds(0));
    doneGetLatch1.countDown();
    advanceGetLatch2.await();
    var v2 = (String) DBOS.getEvent(wfid, key, Duration.ofSeconds(0));
    return v1 + v2;
  }

  public void resetCounts() {
    advanceGetLatch1 = new CountDownLatch(1);
    advanceGetLatch2 = new CountDownLatch(1);
    advanceSetLatch = new CountDownLatch(1);
    doneGetLatch1 = new CountDownLatch(1);
    doneSetLatch1 = new CountDownLatch(1);
    doneSetLatch2 = new CountDownLatch(1);
  }
}
