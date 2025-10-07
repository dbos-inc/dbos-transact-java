package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.StepInfo;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class EventsTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    EventsTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  public void basic_set_get() throws Exception {

    EventsService eventService =
        dbos.registerWorkflows(EventsService.class, new EventsServiceImpl());
    dbos.launch();

    try (var id = new WorkflowOptions("id1").setContext()) {
      eventService.setEventWorkflow("key1", "value1");
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    String val = (String) dbos.getEvent("id1", "key1", Duration.ofSeconds(3));
    assertEquals("value1", val);
  }

  @Test
  public void multipleEvents() throws Exception {

    EventsService eventService =
        dbos.registerWorkflows(EventsService.class, new EventsServiceImpl());
    dbos.launch();

    try (var id = new WorkflowOptions("id1").setContext()) {
      eventService.setMultipleEvents();
    }

    try (var id = new WorkflowOptions("id2").setContext()) {
      Object event = eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3));
      assertEquals("value1", (String) event);
    }

    // outside workflow
    Double val = (Double) dbos.getEvent("id1", "key2", Duration.ofSeconds(3));
    assertEquals(241.5, val);
  }

  @Test
  public void async_set_get() throws Exception {

    EventsService eventService =
        dbos.registerWorkflows(EventsService.class, new EventsServiceImpl());
    dbos.launch();

    dbos.startWorkflow(
        () -> eventService.setEventWorkflow("key1", "value1"), new StartWorkflowOptions("id1"));
    dbos.startWorkflow(
        () -> eventService.getEventWorkflow("id1", "key1", Duration.ofSeconds(3)),
        new StartWorkflowOptions("id2"));

    String event = (String) dbos.retrieveWorkflow("id2").getResult();
    assertEquals("value1", event);
  }

  @Test
  public void notification() throws Exception {

    EventsService eventService =
        dbos.registerWorkflows(EventsService.class, new EventsServiceImpl());
    dbos.launch();

    dbos.startWorkflow(
        () -> eventService.getWithlatch("id1", "key1", Duration.ofSeconds(5)),
        new StartWorkflowOptions("id2"));
    dbos.startWorkflow(
        () -> eventService.setWithLatch("key1", "value1"), new StartWorkflowOptions("id1"));

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

    dbos.launch();

    long start = System.currentTimeMillis();
    dbos.getEvent("nonexistingid", "fake_key", Duration.ofSeconds(2));
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 3000);
  }

  @Test
  public void concurrency() throws Exception {

    EventsService eventService =
        dbos.registerWorkflows(EventsService.class, new EventsServiceImpl());
    dbos.launch();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Object> future1 =
          executor.submit(() -> dbos.getEvent("id1", "key1", Duration.ofSeconds(5)));
      Future<Object> future2 =
          executor.submit(() -> dbos.getEvent("id1", "key1", Duration.ofSeconds(5)));

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
