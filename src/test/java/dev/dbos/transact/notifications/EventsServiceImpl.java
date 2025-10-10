package dev.dbos.transact.notifications;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class EventsServiceImpl implements EventsService {

  private final CountDownLatch getReadyLatch = new CountDownLatch(1);

  EventsServiceImpl() {}

  @Workflow(name = "setEventWorkflow")
  public void setEventWorkflow(String key, Object value) {
    DBOS.setEvent(key, value);
    DBOS.runStep(
        () -> {
          DBOS.setEvent(key + "-fromstep", value);
        },
        "stepSetEvent");
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
}
