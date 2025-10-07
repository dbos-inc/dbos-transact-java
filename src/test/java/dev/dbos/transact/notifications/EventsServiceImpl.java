package dev.dbos.transact.notifications;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class EventsServiceImpl implements EventsService {

  private final CountDownLatch getReadyLatch = new CountDownLatch(1);

  EventsServiceImpl() {}

  @Workflow(name = "setEventWorkflow")
  public void setEventWorkflow(String key, Object value) {
    DBOSContext.dbosInstance().setEvent(key, value);
  }

  @Workflow(name = "getEventWorkflow")
  public Object getEventWorkflow(String workflowId, String key, Duration timeOut) {
    return DBOSContext.dbosInstance().getEvent(workflowId, key, timeOut);
  }

  @Workflow(name = "setMultipleEvents")
  public void setMultipleEvents() {
    var dbos = DBOSContext.dbosInstance();
    dbos.setEvent("key1", "value1");
    dbos.setEvent("key2", Double.valueOf(241.5));
    dbos.setEvent("key3", null);
  }

  @Workflow(name = "setWithLatch")
  public void setWithLatch(String key, String value) {
    try {
      System.out.printf(
          "workflowId is %s %b%n", DBOSContext.workflowId(), DBOSContext.inWorkflow());
      getReadyLatch.await();
      Thread.sleep(1000); // delay so that get goes and awaits notification
      DBOSContext.dbosInstance().setEvent(key, value);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
  }

  @Workflow(name = "getWithlatch")
  public Object getWithlatch(String workflowId, String key, Duration timeOut) {
    getReadyLatch.countDown();
    return DBOSContext.dbosInstance().getEvent(workflowId, key, timeOut);
  }
}
