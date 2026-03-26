package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface EventsService {

  String setEventWorkflow(String key, String value);

  Object getEventWorkflow(String workflowId, String key, Duration timeout);

  void setMultipleEvents();

  void setWithLatch(String key, String value);

  Object getWithlatch(String workflowId, String key, Duration timeOut);

  String getEventTwice(String wfid, String key) throws InterruptedException;

  void setEventTwice(String key, String v1, String v2) throws InterruptedException;

  void setMultipleEventsWorkflow();
}

class EventsServiceImpl implements EventsService {

  private final DBOS dbos;

  CountDownLatch getReadyLatch = new CountDownLatch(1);
  CountDownLatch advanceSetLatch = new CountDownLatch(1);
  CountDownLatch advanceGetLatch1 = new CountDownLatch(1);
  CountDownLatch advanceGetLatch2 = new CountDownLatch(1);
  CountDownLatch doneSetLatch1 = new CountDownLatch(1);
  CountDownLatch doneSetLatch2 = new CountDownLatch(1);
  CountDownLatch doneGetLatch1 = new CountDownLatch(1);

  EventsServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public String setEventWorkflow(String key, String value) {
    dbos.setEvent(key, value);
    dbos.runStep(
        () -> {
          dbos.setEvent(key + "-fromstep", value);
        },
        "stepSetEvent");
    return dbos.runStep(
        () -> {
          return (String)
              dbos.getEvent(DBOSContext.workflowId(), key + "-fromstep", Duration.ofSeconds(0));
        },
        "getEventInStep");
  }

  @Workflow
  @Override
  public Object getEventWorkflow(String workflowId, String key, Duration timeOut) {
    return dbos.getEvent(workflowId, key, timeOut);
  }

  @Workflow
  @Override
  public void setMultipleEvents() {
    dbos.setEvent("key1", "value1");
    dbos.setEvent("key2", Double.valueOf(241.5));
    dbos.setEvent("key3", null);
  }

  @Workflow
  @Override
  public void setWithLatch(String key, String value) {
    try {
      System.out.printf(
          "workflowId is %s %b%n", DBOSContext.workflowId(), DBOSContext.inWorkflow());
      getReadyLatch.await();
      Thread.sleep(1000); // delay so that get goes and awaits notification
      dbos.setEvent(key, value);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for recv signal", e);
    }
  }

  @Workflow
  @Override
  public Object getWithlatch(String workflowId, String key, Duration timeOut) {
    getReadyLatch.countDown();
    return dbos.getEvent(workflowId, key, timeOut);
  }

  @Workflow
  @Override
  public void setEventTwice(String key, String v1, String v2) throws InterruptedException {
    dbos.setEvent(key, v1);
    doneSetLatch1.countDown();
    advanceSetLatch.await();
    dbos.setEvent(key, v2);
    doneSetLatch2.countDown();
  }

  @Workflow
  @Override
  public String getEventTwice(String wfid, String key) throws InterruptedException {
    advanceGetLatch1.await();
    var v1 = (String) dbos.getEvent(wfid, key, Duration.ofSeconds(0));
    doneGetLatch1.countDown();
    advanceGetLatch2.await();
    var v2 = (String) dbos.getEvent(wfid, key, Duration.ofSeconds(0));
    return v1 + v2;
  }

  @Workflow
  @Override
  public void setMultipleEventsWorkflow() {
    dbos.setEvent("key1", "value1");
    dbos.setEvent("key2", "value2");
    dbos.setEvent("key3", null);

    dbos.runStep(
        () -> {
          dbos.setEvent("key4", "badvalue");
          dbos.setEvent("key4", "value4");
        },
        "setEventStep");
  }

  public void resetLatches() {
    advanceGetLatch1 = new CountDownLatch(1);
    advanceGetLatch2 = new CountDownLatch(1);
    advanceSetLatch = new CountDownLatch(1);
    doneGetLatch1 = new CountDownLatch(1);
    doneSetLatch1 = new CountDownLatch(1);
    doneSetLatch2 = new CountDownLatch(1);
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class EventsTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  private DBOSConfig dbosConfig;
  @AutoClose private DBOS dbos;
  @AutoClose private HikariDataSource dataSource;

  private EventsService proxy;
  private EventsServiceImpl impl;

  @BeforeEach
  void setup() {
    this.dbosConfig = pgContainer.dbosConfig();
    this.dbos = new DBOS(dbosConfig);
    this.dataSource = pgContainer.dataSource();
    this.impl = new EventsServiceImpl(dbos);
    this.proxy = dbos.registerWorkflows(EventsService.class, impl);
    dbos.launch();
  }

  @Test
  public void setGetEvents() throws Exception {
    var wfid = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.setMultipleEventsWorkflow();
    }
    try (var ctx = new WorkflowOptions(wfid).setContext()) {
      proxy.setMultipleEventsWorkflow();
    }

    var timeout = Duration.ofSeconds(5);
    assertEquals("value1", proxy.getEventWorkflow(wfid, "key1", timeout));
    assertEquals("value2", proxy.getEventWorkflow(wfid, "key2", timeout));

    // Run getEvent outside of a workflow
    assertEquals("value1", dbos.getEvent(wfid, "key1", timeout));
    assertEquals("value2", dbos.getEvent(wfid, "key2", timeout));

    assertNull(proxy.getEventWorkflow(wfid, "key3", timeout));

    assertEquals("value4", dbos.getEvent(wfid, "key4", timeout));

    var steps = dbos.listWorkflowSteps(wfid);
    assertEquals(4, steps.size());
    for (var i = 0; i < 3; i++) {
      assertEquals(steps.get(i).functionName(), "DBOS.setEvent");
    }
    assertEquals(steps.get(3).functionName(), "setEventStep");

    // test OAOO
    var timeoutWFID = UUID.randomUUID().toString();
    try (var ctx = new WorkflowOptions(timeoutWFID).setContext()) {
      var begin = System.currentTimeMillis();
      var result = proxy.getEventWorkflow("nonexistent-wfid", timeoutWFID, Duration.ofSeconds(2));
      var end = System.currentTimeMillis();
      assert (end - begin > 1500);
      assertNull(result);
    }

    try (var ctx = new WorkflowOptions(timeoutWFID).setContext()) {
      var begin = System.currentTimeMillis();
      var result = proxy.getEventWorkflow("nonexistent-wfid", timeoutWFID, Duration.ofSeconds(2));
      var end = System.currentTimeMillis();
      assert (end - begin < 300);
      assertNull(result);
    }

    // No OAOO for getEvent outside of a workflow
    {
      var begin = System.currentTimeMillis();
      var result = dbos.getEvent("nonexistent-wfid", timeoutWFID, Duration.ofSeconds(2));
      var end = System.currentTimeMillis();
      assert (end - begin > 1500);
      assertNull(result);
    }

    {
      var begin = System.currentTimeMillis();
      var result = dbos.getEvent("nonexistent-wfid", timeoutWFID, Duration.ofSeconds(2));
      var end = System.currentTimeMillis();
      assert (end - begin > 1500);
      assertNull(result);
    }

    assertThrows(IllegalStateException.class, () -> dbos.setEvent("key", "value"));
  }

  @Test
  public void basic_set_get() throws Exception {
    try (var id = new WorkflowOptions("id1").setContext()) {
      proxy.setEventWorkflow("key1", "value1");
    }

    var steps = dbos.listWorkflowSteps("id1");
    String[] stepNames = {"DBOS.setEvent", "stepSetEvent", "getEventInStep"};
    assertEquals(3, steps.size());
    for (var i = 0; i < steps.size(); i++) {
      var step = steps.get(i);
      assertEquals(stepNames[i], step.functionName());
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = proxy.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    String val = (String) dbos.getEvent("id1", "key1", Duration.ofSeconds(3));
    assertEquals("value1", val);
    assertThrows(IllegalStateException.class, () -> dbos.setEvent("a", "b"));
  }

  @Test
  public void multipleEvents() throws Exception {
    try (var id = new WorkflowOptions("id1").setContext()) {
      proxy.setMultipleEvents();
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = proxy.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    Double val = (Double) dbos.getEvent("id1", "key2", Duration.ofSeconds(3));
    assertEquals(241.5, val);
  }

  @Test
  public void async_set_get() throws Exception {
    var setwfh =
        dbos.startWorkflow(
            () -> proxy.setEventWorkflow("key1", "value1"), new StartWorkflowOptions("id1"));
    dbos.startWorkflow(
        () -> proxy.getEventWorkflow("id1", "key1", Duration.ofSeconds(3)),
        new StartWorkflowOptions("id2"));

    String event = (String) dbos.retrieveWorkflow("id2").getResult();
    String stepEvent = (String) dbos.getEvent("id1", "key1-fromstep", Duration.ofMillis(1000));
    assertEquals("value1", event);
    assertEquals("value1", stepEvent);
    assertEquals("value1", setwfh.getResult());

    List<StepInfo> steps = dbos.listWorkflowSteps(setwfh.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.setEvent", steps.get(0).functionName());
    assertEquals("stepSetEvent", steps.get(1).functionName());
    assertEquals("getEventInStep", steps.get(2).functionName());
  }

  @Test
  public void set_twice() throws Exception {
    var setwfh =
        dbos.startWorkflow(
            () -> proxy.setEventTwice("key1", "value1", "value2"), new StartWorkflowOptions("id1"));
    var getwfh =
        dbos.startWorkflow(
            () -> proxy.getEventTwice(setwfh.workflowId(), "key1"),
            new StartWorkflowOptions("id2"));

    // Make these things both happen
    impl.doneSetLatch1.await();
    impl.advanceGetLatch1.countDown();
    impl.doneGetLatch1.await();
    impl.advanceSetLatch.countDown();
    impl.doneSetLatch2.await();
    impl.advanceGetLatch2.countDown();
    String res = (String) getwfh.getResult();
    assertEquals("value1value2", res);

    // See if it stuck
    impl.resetLatches();
    impl.advanceGetLatch1.countDown();
    impl.advanceGetLatch2.countDown();
    DBUtils.setWorkflowState(dataSource, getwfh.workflowId(), WorkflowState.PENDING.name());
    getwfh =
        DBOSTestAccess.getDbosExecutor(dbos).executeWorkflowById(getwfh.workflowId(), true, false);
    res = (String) getwfh.getResult();
    assertEquals("value1value2", res);

    var events = DBUtils.getWorkflowEvents(dataSource, "id1");
    assertEquals(1, events.size());

    var eventHistory = DBUtils.getWorkflowEventHistory(dataSource, "id1");
    assertEquals(2, eventHistory.size());
  }

  @Test
  public void notification() throws Exception {
    dbos.startWorkflow(
        () -> proxy.getWithlatch("id1", "key1", Duration.ofSeconds(5)),
        new StartWorkflowOptions("id2"));
    dbos.startWorkflow(() -> proxy.setWithLatch("key1", "value1"), new StartWorkflowOptions("id1"));

    String event = (String) dbos.retrieveWorkflow("id2").getResult();
    assertEquals("value1", event);

    List<StepInfo> steps = dbos.listWorkflowSteps("id1");
    assertEquals(1, steps.size());
    assertEquals("DBOS.setEvent", steps.get(0).functionName());

    steps = dbos.listWorkflowSteps("id2");
    assertEquals(2, steps.size());
    assertEquals("DBOS.getEvent", steps.get(0).functionName());
    assertEquals("DBOS.sleep", steps.get(1).functionName());
  }

  @Test
  public void timeout() {
    long start = System.currentTimeMillis();
    dbos.getEvent("nonexistingid", "fake_key", Duration.ofMillis(10));
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 1000);
  }

  @Test
  public void concurrency() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Object> future1 =
          executor.submit(() -> dbos.getEvent("id1", "key1", Duration.ofSeconds(5)));
      Future<Object> future2 =
          executor.submit(() -> dbos.getEvent("id1", "key1", Duration.ofSeconds(5)));

      String expectedMessage = "test message";
      try (var id = new WorkflowOptions("id1").setContext()) {
        proxy.setEventWorkflow("key1", expectedMessage);
        ;
      }

      // Both should return the same message
      String result1 = (String) future1.get();
      String result2 = (String) future2.get();

      assertEquals(result1, result2);
      assertEquals(expectedMessage, result1);

    } finally {
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
