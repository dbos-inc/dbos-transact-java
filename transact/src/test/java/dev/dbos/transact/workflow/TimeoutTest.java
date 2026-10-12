package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    var options = new StartWorkflowOptions(wfid1).withTimeout(15, TimeUnit.SECONDS);
    var handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);
    result = handle.getResult();
    assertEquals("1234512345", result);
    assertEquals(wfid1, handle.workflowId());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void aTimeoutCenturiesAwayStartsAndRuns() throws Exception {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    // Further away than a long count of nanoseconds reaches (about 292 years).
    var options =
        new StartWorkflowOptions("wf-far-timeout").withTimeout(Duration.ofDays(365L * 1_000));
    var handle = dbos.startWorkflow(() -> simpleService.longWorkflow("12345"), options);
    assertEquals("1234512345", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void aStartWhoseInsertWaitsPastItsTimeoutIsCancelledAtDispatch() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "relies on Postgres waiting on the conflicting row");
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    // Another transaction inserts the same workflow ID and holds it uncommitted, so the start's
    // insert waits on it. The start's transaction, and with it the deadline, began before the wait.
    var workflowId = "wf-insert-waits";
    var blocker = dataSource.getConnection();
    blocker.setAutoCommit(false);
    try (var stmt =
        blocker.prepareStatement(
            """
            INSERT INTO "dbos".workflow_status
                (workflow_uuid, status, name, class_name, executor_id, recovery_attempts, priority)
            VALUES (?, 'PENDING', 'blocker', 'com.example.Blocker', 'blocker', 0, 0)
            """)) {
      stmt.setString(1, workflowId);
      stmt.executeUpdate();
    }
    var release =
        new Thread(
            () -> {
              try (blocker) {
                Thread.sleep(1_500);
                blocker.rollback();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    release.start();

    var handle =
        dbos.startWorkflow(
            () -> simpleService.workWithString("late"),
            new StartWorkflowOptions(workflowId).withTimeout(Duration.ofSeconds(1)));
    release.join();

    // The insert came back after the deadline, so the dispatch check cancels instead of running.
    assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
  }

  @Test
  @SuppressWarnings("removal") // exercises the deprecated deadline option
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
  }

  @Test
  public void queued() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    String simpleQ = "simpleQ";

    dbos.launch();
    dbos.registerQueue(simpleQ, new QueueOptions());

    // queued

    String wfid1 = "wf-126";
    String result;

    var options =
        new StartWorkflowOptions(wfid1).withQueue(simpleQ).withTimeout(15, TimeUnit.SECONDS);
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
    String simpleQ = "simpleQ";

    dbos.launch();
    dbos.registerQueue(simpleQ, new QueueOptions());
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

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(15, TimeUnit.SECONDS);

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

    WorkflowOptions options = new WorkflowOptions(wfid1).withTimeout(15, TimeUnit.SECONDS);

    try (var id = options.setContext()) {
      simpleService.workWithString("12345");
    }

    setWorkflowDeadlinePassed(dataSource, wfid1);

    var handle = dbosExecutor.executeWorkflowById(wfid1);
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

  /**
   * A queued child of a timed parent carries the parent's deadline and no timeout of its own. With
   * the parent's timeout it would start a fresh copy of that budget on dequeue and could outlive
   * the parent (#561).
   */
  @Test
  public void queuedChildInheritsTheParentsDeadlineNotItsTimeout() throws Exception {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();
    dbos.registerQueue("childQ", new QueueOptions());

    var parentId = "wf-queued-parent";
    try (var o = new WorkflowOptions(parentId).withTimeout(Duration.ofMinutes(5)).setContext()) {
      assertEquals("QueuedChildren", simpleService.syncWithQueued());
    }

    var parent = DBUtils.getWorkflowRow(dataSource, parentId);
    assertEquals(Duration.ofMinutes(5).toMillis(), parent.timeoutMs());
    assertNotNull(parent.deadlineEpochMs());
    for (var childId : List.of("child0", "child1", "child2")) {
      dbos.retrieveWorkflow(childId).getResult();
      var child = DBUtils.getWorkflowRow(dataSource, childId);
      assertNull(child.timeoutMs(), childId);
      assertEquals(parent.deadlineEpochMs(), child.deadlineEpochMs(), childId);
    }
  }

  @Test
  public void queuedChildIsCanceledAtTheParentsDeadlineNotAfterAFreshTimeout() throws Exception {
    // #561 end to end: the children wait on a paused queue until the parent's deadline has passed.
    // Handed a copy of the parent's timeout, they would start a fresh one on dequeue and succeed.
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();
    dbos.registerQueue("childQ", new QueueOptions());
    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.pause();

    var parentId = "wf-short-parent";
    try (var o = new WorkflowOptions(parentId).withTimeout(Duration.ofSeconds(3)).setContext()) {
      assertEquals("QueuedChildren", simpleService.syncWithQueued());
    }
    var deadline = DBUtils.getWorkflowRow(dataSource, parentId).deadlineEpochMs();
    Thread.sleep(Math.max(0, deadline - System.currentTimeMillis()) + 200);
    queueService.unpause();

    for (var childId : List.of("child0", "child1", "child2")) {
      assertThrows(
          DBOSAwaitedWorkflowCancelledException.class,
          () -> dbos.retrieveWorkflow(childId).getResult(),
          childId);
    }
  }

  @Test
  public void aResumedChildThatInheritedOnlyADeadlineRunsUnbounded() throws Exception {
    // Resume clears a deadline and keeps a timeout, as in Python and TypeScript. A child that
    // inherited only its parent's deadline has no timeout to keep.
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();
    dbos.registerQueue("childQ", new QueueOptions());
    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.pause();

    try (var o =
        new WorkflowOptions("wf-resume-parent").withTimeout(Duration.ofMinutes(5)).setContext()) {
      assertEquals("QueuedChildren", simpleService.syncWithQueued());
    }
    assertNotNull(DBUtils.getWorkflowRow(dataSource, "child0").deadlineEpochMs());

    dbos.cancelWorkflow("child0");
    var handle = dbos.resumeWorkflow("child0");
    queueService.unpause();
    handle.getResult();

    var child = DBUtils.getWorkflowRow(dataSource, "child0");
    assertEquals("SUCCESS", child.status());
    assertNull(child.timeoutMs());
    assertNull(child.deadlineEpochMs());
  }

  @Test
  public void aResumedWorkflowKeepsItsTimeoutAndGetsAFreshDeadline() throws Exception {
    // The other half of the resume rule: the old deadline is cleared, and the kept timeout sets a
    // new one when the workflow is dequeued again.
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();
    dbos.registerQueue("childQ", new QueueOptions());
    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.pause();

    var wfid = "wf-resume-timeout";
    var options =
        new StartWorkflowOptions(wfid).withQueue("childQ").withTimeout(Duration.ofMinutes(5));
    dbos.startWorkflow(() -> simpleService.childWorkflow("resumed"), options);
    dbos.cancelWorkflow(wfid);
    // A deadline long past, as if the workflow had run out of time before it was canceled.
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET workflow_deadline_epoch_ms = ?"
                    + " WHERE workflow_uuid = ?")) {
      stmt.setLong(1, System.currentTimeMillis() - 10_000);
      stmt.setString(2, wfid);
      assertEquals(1, stmt.executeUpdate());
    }

    var resumedAt = System.currentTimeMillis();
    var handle = dbos.<String, RuntimeException>resumeWorkflow(wfid);
    queueService.unpause();
    assertEquals("resumed", handle.getResult());

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(Duration.ofMinutes(5).toMillis(), row.timeoutMs());
    assertTrue(row.deadlineEpochMs() >= resumedAt + Duration.ofMinutes(5).toMillis());
  }

  @Test
  @SuppressWarnings("removal") // exercises the deprecated deadline option
  public void anInnerWorkflowOptionsBoundReplacesAnOuterDeadline() throws Exception {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    var simpleService = dbos.registerProxy(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    var farDeadline = Instant.now().plus(Duration.ofDays(1));
    try (var outer = new WorkflowOptions().withDeadline(farDeadline).setContext()) {
      try (var inner =
          new WorkflowOptions("wf-inner-timeout").withTimeout(Duration.ofMinutes(5)).setContext()) {
        simpleService.workWithString("timeout");
      }
      try (var inner = new WorkflowOptions("wf-inner-none").withNoTimeout().setContext()) {
        simpleService.workWithString("none");
      }
      // Closing the inner blocks restores the outer deadline.
      try (var id = new WorkflowOptions("wf-outer-deadline").setContext()) {
        simpleService.workWithString("deadline");
      }
    }

    var timed = DBUtils.getWorkflowRow(dataSource, "wf-inner-timeout");
    assertEquals(Duration.ofMinutes(5).toMillis(), timed.timeoutMs());
    assertTrue(timed.deadlineEpochMs() < farDeadline.toEpochMilli());
    var unbounded = DBUtils.getWorkflowRow(dataSource, "wf-inner-none");
    assertNull(unbounded.timeoutMs());
    assertNull(unbounded.deadlineEpochMs());
    var outer = DBUtils.getWorkflowRow(dataSource, "wf-outer-deadline");
    assertNull(outer.timeoutMs());
    assertEquals(farDeadline.toEpochMilli(), outer.deadlineEpochMs());
  }

  private void setWorkflowDeadlinePassed(DataSource ds, String workflowId) throws SQLException {

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
