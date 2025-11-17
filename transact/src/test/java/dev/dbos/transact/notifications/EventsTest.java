package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class EventsTest {

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    EventsTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    DBOS.reinitialize(dbosConfig);
    EventsTest.dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void basic_set_get() throws Exception {

    EventsService eventService =
        DBOS.registerWorkflows(EventsService.class, new EventsServiceImpl());
    DBOS.launch();

    try (var id = new WorkflowOptions("id1").setContext()) {
      eventService.setEventWorkflow("key1", "value1");
    }

    var steps = DBOS.listWorkflowSteps("id1");
    String[] stepNames = {"DBOS.setEvent", "stepSetEvent", "getEventInStep"};
    assertEquals(3, steps.size());
    for (var i = 0; i < steps.size(); i++) {
      var step = steps.get(i);
      assertEquals(stepNames[i], step.functionName());
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    String val = (String) DBOS.getEvent("id1", "key1", Duration.ofSeconds(3));
    assertEquals("value1", val);
    assertThrows(IllegalStateException.class, () -> DBOS.setEvent("a", "b"));
  }

  @Test
  public void multipleEvents() throws Exception {

    EventsService eventService =
        DBOS.registerWorkflows(EventsService.class, new EventsServiceImpl());
    DBOS.launch();

    try (var id = new WorkflowOptions("id1").setContext()) {
      eventService.setMultipleEvents();
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    Double val = (Double) DBOS.getEvent("id1", "key2", Duration.ofSeconds(3));
    assertEquals(241.5, val);
  }

  @Test
  public void async_set_get() throws Exception {

    EventsService eventService =
        DBOS.registerWorkflows(EventsService.class, new EventsServiceImpl());
    DBOS.launch();

    var setwfh =
        DBOS.startWorkflow(
            () -> eventService.setEventWorkflow("key1", "value1"), new StartWorkflowOptions("id1"));
    DBOS.startWorkflow(
        () -> eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3)),
        new StartWorkflowOptions("id2"));

    String event = (String) DBOS.retrieveWorkflow("id2").getResult();
    String stepEvent = (String) DBOS.getEvent("id1", "key1-fromstep", Duration.ofMillis(1000));
    assertEquals("value1", event);
    assertEquals("value1", stepEvent);
    assertEquals("value1", setwfh.getResult());

    List<StepInfo> steps = DBOS.listWorkflowSteps(setwfh.workflowId());
    assertEquals(3, steps.size());
    assertEquals("DBOS.setEvent", steps.get(0).functionName());
    assertEquals("stepSetEvent", steps.get(1).functionName());
    assertEquals("getEventInStep", steps.get(2).functionName());
  }

  @Test
  public void set_twice() throws Exception {
    var impl = new EventsServiceImpl();
    EventsService eventService = DBOS.registerWorkflows(EventsService.class, impl);
    DBOS.launch();

    var setwfh =
        DBOS.startWorkflow(
            () -> eventService.setEventTwice("key1", "value1", "value2"),
            new StartWorkflowOptions("id1"));
    var getwfh =
        DBOS.startWorkflow(
            () -> eventService.getEventTwice(setwfh.workflowId(), "key1"),
            new StartWorkflowOptions("id2"));

    // Make these things both happen
    impl.awaitSetLatch1();
    impl.advanceGet1();
    impl.awaitGetLatch1();
    impl.advanceSet();
    impl.awaitSetLatch2();
    impl.advanceGet2();
    String res = (String) getwfh.getResult();
    assertEquals("value1value2", res);

    // See if it stuck
    impl.resetCounts();
    impl.advanceGet1();
    impl.advanceGet2();
    DBUtils.setWorkflowState(dataSource, getwfh.workflowId(), WorkflowState.PENDING.name());
    getwfh = DBOSTestAccess.getDbosExecutor().executeWorkflowById(getwfh.workflowId());
    res = (String) getwfh.getResult();
    assertEquals("value1value2", res);

    // check event history
    String sql1 = "SELECT count(*) FROM dbos.workflow_events WHERE workflow_uuid = ?";
    String sql2 = "SELECT count(*) FROM dbos.workflow_events_history WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt1 = conn.prepareStatement(sql1);
        var stmt2 = conn.prepareStatement(sql2)) {
      stmt1.setString(1, "id1");
      try (var rs = stmt1.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("count"));
      }

      stmt2.setString(1, "id1");
      try (var rs = stmt2.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("count"));
      }
    }
  }

  @Test
  public void notification() throws Exception {

    EventsService eventService =
        DBOS.registerWorkflows(EventsService.class, new EventsServiceImpl());
    DBOS.launch();

    DBOS.startWorkflow(
        () -> eventService.getWithlatch("id1", "key1", Duration.ofSeconds(5)),
        new StartWorkflowOptions("id2"));
    DBOS.startWorkflow(
        () -> eventService.setWithLatch("key1", "value1"), new StartWorkflowOptions("id1"));

    String event = (String) DBOS.retrieveWorkflow("id2").getResult();
    assertEquals("value1", event);

    List<StepInfo> steps = DBOS.listWorkflowSteps("id1");
    assertEquals(1, steps.size());
    assertEquals("DBOS.setEvent", steps.get(0).functionName());

    steps = DBOS.listWorkflowSteps("id2");
    assertEquals(2, steps.size());
    assertEquals("DBOS.getEvent", steps.get(0).functionName());
    assertEquals("DBOS.sleep", steps.get(1).functionName());
  }

  @Test
  public void timeout() {

    DBOS.launch();

    long start = System.currentTimeMillis();
    DBOS.getEvent("nonexistingid", "fake_key", Duration.ofMillis(10));
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 1000);
  }

  @Test
  public void concurrency() throws Exception {

    EventsService eventService =
        DBOS.registerWorkflows(EventsService.class, new EventsServiceImpl());
    DBOS.launch();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Object> future1 =
          executor.submit(() -> DBOS.getEvent("id1", "key1", Duration.ofSeconds(5)));
      Future<Object> future2 =
          executor.submit(() -> DBOS.getEvent("id1", "key1", Duration.ofSeconds(5)));

      String expectedMessage = "test message";
      try (var id = new WorkflowOptions("id1").setContext()) {
        eventService.setEventWorkflow("key1", expectedMessage);
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
