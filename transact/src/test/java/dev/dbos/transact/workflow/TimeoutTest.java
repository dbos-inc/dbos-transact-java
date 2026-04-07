package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class TimeoutTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  public void async() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    var options = new StartWorkflowOptions(wfid1).withTimeout(3, TimeUnit.SECONDS);
    var handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);
    result = handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.workflowId());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void asyncTimedOut() {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // make it time out
    String wfid1 = "wf-125";
    var options = new StartWorkflowOptions(wfid1).withTimeout(1, TimeUnit.SECONDS);
    var handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    String wfid2 = "wf-125b";
    var options2 =
        new StartWorkflowOptions(wfid2)
            .withDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 1000));
    var handle2 = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options2);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Exception t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertNotNull(s);
    assertEquals(WorkflowState.CANCELLED, s.status());

    try {
      handle2.getResult();
      fail("Expected Exception to be thrown");
    } catch (Exception t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s2 = systemDatabase.getWorkflowStatus(wfid2);
    assertNotNull(s2);
    assertEquals(WorkflowState.CANCELLED, s2.status());

    // Negative test
    assertThrows(
        IllegalArgumentException.class,
        () -> new StartWorkflowOptions().withTimeout(Duration.ofSeconds(-1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StartWorkflowOptions()
                .withDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 100))
                .withTimeout(Duration.ofSeconds(1)));
  }

  @Test
  public void queued() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    Queue simpleQ = new Queue("simpleQ");
    dbos.registerQueue(simpleQ);

    dbos.launch();

    // queued

    String wfid1 = "wf-126";
    String result;

    var options =
        new StartWorkflowOptions(wfid1).withQueue(simpleQ).withTimeout(3, TimeUnit.SECONDS);
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.workflowId());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void queuedTimedOut() {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    Queue simpleQ = new Queue("simpleQ");
    dbos.registerQueue(simpleQ);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // make it timeout
    String wfid1 = "wf-127";

    var options =
        new StartWorkflowOptions(wfid1).withQueue(simpleQ).withTimeout(1, TimeUnit.SECONDS);
    var handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);

    try {
      handle.getResult();
      fail("Expected Exception to be thrown");
    } catch (Exception t) {
      System.out.println(t.getClass().toString());
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertNotNull(s);
    assertEquals(WorkflowState.CANCELLED, s.status());
  }

  @Test
  public void sync() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

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
    assertEquals(WorkflowState.SUCCESS, s.status());
  }

  @Test
  public void syncTimeout() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

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
      assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
    }

    var s = systemDatabase.getWorkflowStatus(wfid1);
    assertTrue(s != null);
    assertEquals(WorkflowState.CANCELLED, s.status());
  }

  @Test
  public void recovery() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    // synchronous

    String wfid1 = "wf-128";

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(3, TimeUnit.SECONDS);

    try (var id = options.setContext()) {
      simpleService.workWithString("12345");
    }

    setDelayEpoch(dataSource, wfid1);

    var handle = dbosExecutor.executeWorkflowById(wfid1, true, false);
    assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
  }

  @Test
  public void parentChild() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();

    // asynchronous

    String wfid1 = "wf-124";
    String result;

    WorkflowOptions options = new WorkflowOptions(wfid1);

    try (var id = options.setContext()) {
      result = simpleService.longParent("12345", 1, 2);
    }

    assertEquals("1234512345", result);

    var handle = dbos.retrieveWorkflow(wfid1);

    result = (String) handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.workflowId());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void parentChildTimeOut() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();

    String wfid1 = "wf-124";

    WorkflowOptions options = new WorkflowOptions(wfid1);

    assertThrows(
        Exception.class,
        () -> {
          try (var id = options.setContext()) {
            simpleService.longParent("12345", 3, 1);
          }
        });

    var parentStatus = dbos.retrieveWorkflow(wfid1).getStatus();
    assertEquals(WorkflowState.ERROR, parentStatus.status());
    assertEquals("Awaited workflow childwf was cancelled.", parentStatus.error().message());

    var childStatus = dbos.retrieveWorkflow("childwf").getStatus().status();
    assertEquals(WorkflowState.CANCELLED, childStatus);
  }

  private static final Logger logger = LoggerFactory.getLogger(TimeoutTest.class);

  @Test
  public void parentTimeoutInheritedByChild() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();

    String wfid1 = "wf-124";

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(1, TimeUnit.SECONDS);
    assertThrows(
        Exception.class,
        () -> {
          try (var id = options.setContext()) {
            simpleService.longParent("12345", 10, 0);
          }
        });

    try {
      var parentStatus = dbos.retrieveWorkflow(wfid1).getStatus().status();
      assertEquals(WorkflowState.CANCELLED, parentStatus);
    } finally {
      var row = DBUtils.getWorkflowRow(dataSource, wfid1);
      if (!WorkflowState.CANCELLED.name().equals(row.status())) {
        logger.warn("{}: {}", wfid1, row);
      }
    }

    var childWfId = "childwf";
    var handle = dbos.retrieveWorkflow(childWfId);
    assertThrows(Exception.class, () -> handle.getResult());

    try {
      var childStatus = dbos.retrieveWorkflow(childWfId).getStatus().status();
      assertEquals(WorkflowState.CANCELLED, childStatus);
    } finally {
      var row = DBUtils.getWorkflowRow(dataSource, childWfId);
      if (!WorkflowState.CANCELLED.name().equals(row.status())) {
        logger.warn("{}: {}", childWfId, row);
      }
    }
  }

  @Test
  public void parentAsyncTimeoutInheritedByChild() throws Exception {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);

    dbos.launch();

    String wfid1 = "wf-124";

    var options = new StartWorkflowOptions(wfid1).withTimeout(2, TimeUnit.SECONDS);

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> simpleService.longParent("12345", 10, 0), options);

    assertThrows(DBOSAwaitedWorkflowCancelledException.class, () -> handle.getResult());
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
