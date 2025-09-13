package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.AwaitedWorkflowCancelledException;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.*;
import java.time.Instant;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimeoutTest {

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    TimeoutTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();

    String dbUrl =
        String.format(
            "jdbc:postgresql://%s:%d/%s",
            dbosConfig.getDbHost(), dbosConfig.getDbPort(), "postgres");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    TimeoutTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws SQLException, Exception {
    dbos.shutdown();
  }

  @Test
  public void async() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(3).build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"));
    }
    // assertNull(result);

    // WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(wfid1); ;
    result = handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().getStatus());
  }

  @Test
  public void asyncTimedOut() {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // make it timeout
    String wfid1 = "wf-125";
    String result;
    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(1).build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"));
    }
    // assertNull(result);
    // WorkflowHandle handle = dbosExecutor.retrieveWorkflow(wfid1);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Throwable t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof AwaitedWorkflowCancelledException);
    }

    WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
    assertEquals(WorkflowState.CANCELLED.name(), s.getStatus());
  }

  @Test
  public void queued() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);
    Queue simpleQ = dbos.Queue("simpleQ").build();

    dbos.launch();

    // queued

    String wfid1 = "wf-126";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).queue(simpleQ).timeout(3).build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"));
    }
    // assertNull(result);

    // WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(wfid1); ;
    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().getStatus());
  }

  @Test
  public void queuedTimedOut() {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);
    Queue simpleQ = dbos.Queue("simpleQ").build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // make it timeout
    String wfid1 = "wf-127";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).queue(simpleQ).timeout(1).build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"));
    }
    // assertNull(result);
    // WorkflowHandle handle = dbosExecutor.retrieveWorkflow(wfid1);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Throwable t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof AwaitedWorkflowCancelledException);
    }

    WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
    assertEquals(WorkflowState.CANCELLED.name(), s.getStatus());
  }

  @Test
  public void sync() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // synchronous

    String wfid1 = "wf-128";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(3).build();

    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      result = simpleService.longWorkflow("12345");
    }
    assertEquals("1234512345", result);

    WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
    assertEquals(WorkflowState.SUCCESS.name(), s.getStatus());
  }

  @Test
  public void syncTimeout() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // synchronous

    String wfid1 = "wf-128";
    String result = null;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(1).build();

    try {
      try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
        result = simpleService.longWorkflow("12345");
      }
    } catch (Throwable t) {
      assertNull(result);
      assertTrue(t instanceof AwaitedWorkflowCancelledException);
    }

    WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
    assertEquals(WorkflowState.CANCELLED.name(), s.getStatus());
  }

  @Test
  public void recovery() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    // synchronous

    String wfid1 = "wf-128";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(3).build();

    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      result = simpleService.workWithString("12345");
    }

    setDelayEpoch(dataSource, wfid1);

    WorkflowHandle handle = dbosExecutor.executeWorkflowById(wfid1);
    assertEquals(WorkflowState.CANCELLED.name(), handle.getStatus().getStatus());
  }

  @Test
  public void parentChild() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).build();
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      result = simpleService.longParent("12345", 1, 2);
    }

    assertEquals("1234512345", result);

    WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(wfid1);
    ;
    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().getStatus());
  }

  @Test
  public void parentChildTimeOut() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).build();
    try {
      try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
        result = simpleService.longParent("12345", 3, 1);
      }
    } catch (Exception e) {
      assertTrue(e instanceof AwaitedWorkflowCancelledException);
      assertEquals("childwf", ((AwaitedWorkflowCancelledException) e).getWorkflowId());
    }

    String parentStatus = dbosExecutor.retrieveWorkflow(wfid1).getStatus().getStatus();
    assertEquals(WorkflowState.ERROR.name(), parentStatus);

    String childStatus = dbosExecutor.retrieveWorkflow("childwf").getStatus().getStatus();
    assertEquals(WorkflowState.CANCELLED.name(), childStatus);
  }

  @Test
  public void parentTimeoutInheritedByChild() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(1).build();
    try {
      try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
        result = simpleService.longParent("12345", 3, 0);
      }
    } catch (Exception e) {
      assertTrue(e instanceof AwaitedWorkflowCancelledException);
      assertEquals("childwf", ((AwaitedWorkflowCancelledException) e).getWorkflowId());
    }

    String parentStatus = dbosExecutor.retrieveWorkflow(wfid1).getStatus().getStatus();
    assertEquals(WorkflowState.ERROR.name(), parentStatus);

    String childStatus = dbosExecutor.retrieveWorkflow("childwf").getStatus().getStatus();
    assertEquals(WorkflowState.CANCELLED.name(), childStatus);
  }

  @Test
  public void parentAsyncTimeoutInheritedByChild() throws Exception {
    // TOFIX : fails at times

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();
    simpleService.setSimpleService(simpleService);

    dbos.launch();

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions.Builder(wfid1).timeout(2).build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.longParent("12345", 3, 0));
    }

    try {
      handle.getResult();
    } catch (Exception e) {
      System.out.println(e.getClass().getName());
      assertTrue(e instanceof AwaitedWorkflowCancelledException);
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
