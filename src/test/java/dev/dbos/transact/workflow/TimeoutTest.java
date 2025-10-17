package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.*;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junitpioneer.jupiter.RetryingTest;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class TimeoutTest {

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    TimeoutTest.dbosConfig =
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
    TimeoutTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws SQLException, Exception {
    DBOS.shutdown();
  }

  @Test
  public void async() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    var options = new StartWorkflowOptions(wfid1).withTimeout(3, TimeUnit.SECONDS);
    var handle = DBOS.startWorkflow(() -> simpleService.longWorkflow("12345"), options);
    result = handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());
  }

  @Test
  public void asyncTimedOut() {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // make it timeout
    String wfid1 = "wf-125";
    var options = new StartWorkflowOptions(wfid1).withTimeout(1, TimeUnit.SECONDS);
    var handle = DBOS.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Exception t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertNotNull(s);
    assertEquals(WorkflowState.CANCELLED.name(), s.status());
  }

  @Test
  public void queued() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);
    Queue simpleQ = DBOS.Queue("simpleQ").build();

    DBOS.launch();

    // queued

    String wfid1 = "wf-126";
    String result;

    var options =
        new StartWorkflowOptions(wfid1).withQueue(simpleQ).withTimeout(3, TimeUnit.SECONDS);
    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());
  }

  @Test
  public void queuedTimedOut() {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);
    Queue simpleQ = DBOS.Queue("simpleQ").build();

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // make it timeout
    String wfid1 = "wf-127";

    var options =
        new StartWorkflowOptions(wfid1).withQueue(simpleQ).withTimeout(1, TimeUnit.SECONDS);
    var handle = DBOS.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Exception t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertNotNull(s);
    assertEquals(WorkflowState.CANCELLED.name(), s.status());
  }

  @Test
  public void sync() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // synchronous

    String wfid1 = "wf-128";
    String result;

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(3, TimeUnit.SECONDS);

    try (var id = options.setContext()) {
      result = simpleService.longWorkflow("12345");
    }
    assertEquals("1234512345", result);

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertNotNull(s);
    assertEquals(WorkflowState.SUCCESS.name(), s.status());
  }

  @Test
  public void syncTimeout() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // synchronous

    String wfid1 = "wf-128";
    String result = null;

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(1, TimeUnit.SECONDS);

    try {
      try (var id = options.setContext()) {
        result = simpleService.longWorkflow("12345");
      }
    } catch (Exception t) {
      assertNull(result);
      assertTrue(t instanceof CancellationException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertTrue(s != null);
    assertEquals(WorkflowState.CANCELLED.name(), s.status());
  }

  @Test
  public void recovery() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    // synchronous

    String wfid1 = "wf-128";

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(3, TimeUnit.SECONDS);

    try (var id = options.setContext()) {
      simpleService.workWithString("12345");
    }

    setDelayEpoch(dataSource, wfid1);

    var handle = dbosExecutor.executeWorkflowById(wfid1);
    assertEquals(WorkflowState.CANCELLED.name(), handle.getStatus().status());
  }

  @Test
  public void parentChild() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions(wfid1);

    try (var id = options.setContext()) {
      result = simpleService.longParent("12345", 1, 2);
    }

    assertEquals("1234512345", result);

    var handle = DBOS.retrieveWorkflow(wfid1);

    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());
  }

  @Test
  public void parentChildTimeOut() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    String wfid1 = "wf-124";

    WorkflowOptions options = new WorkflowOptions(wfid1);

    // TODO: https://github.com/dbos-inc/dbos-transact-java/issues/86
    assertThrows(
        Exception.class,
        () -> {
          try (var id = options.setContext()) {
            simpleService.longParent("12345", 3, 1);
          }
        });

    var parentStatus = DBOS.retrieveWorkflow(wfid1).getStatus();
    assertEquals(WorkflowState.ERROR.name(), parentStatus.status());
    assertEquals("Awaited workflow childwf was cancelled.", parentStatus.error().message());

    String childStatus = DBOS.retrieveWorkflow("childwf").getStatus().status();
    assertEquals(WorkflowState.CANCELLED.name(), childStatus);
  }

  @Test
  @Disabled("setting timeout to zero clears the deadline, logic of this test incorrect")
  public void parentTimeoutInheritedByChild() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    String wfid1 = "wf-124";

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(1, TimeUnit.SECONDS);
    assertThrows(
        Exception.class,
        () -> {
          try (var id = options.setContext()) {
            simpleService.longParent("12345", 3, 0);
          }
        });

    String parentStatus = DBOS.retrieveWorkflow(wfid1).getStatus().status();
    assertEquals(WorkflowState.ERROR.name(), parentStatus);

    String childStatus = DBOS.retrieveWorkflow("childwf").getStatus().status();
    assertEquals(WorkflowState.CANCELLED.name(), childStatus);
  }

  @Test
  @RetryingTest(3)
  public void parentAsyncTimeoutInheritedByChild() throws Exception {
    // TOFIX : fails at times

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    String wfid1 = "wf-124";

    var options = new StartWorkflowOptions(wfid1).withTimeout(2, TimeUnit.SECONDS);

    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(() -> simpleService.longParent("12345", 3, 0), options);

    try {
      handle.getResult();
    } catch (Exception e) {
      System.out.println(e.getClass().getName());
      assertTrue(e instanceof DBOSAwaitedWorkflowCancelledException);
    } catch (Throwable t) {
      System.out.println("a throwable " + t.getClass().getName());
    }
  }

  private void setDelayEpoch(DataSource ds, String workflowId) throws SQLException {

    String sql =
        "UPDATE dbos.workflow_status SET status = ?, updated_at = ?, workflow_deadline_epoch_ms = ? WHERE workflow_uuid = ?";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.name());
      pstmt.setLong(2, Instant.now().toEpochMilli());

      long newEpoch = System.currentTimeMillis() - 10000;
      pstmt.setLong(3, newEpoch);
      pstmt.setString(4, workflowId);

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      assertEquals(1, rowsAffected);
    }
  }
}
