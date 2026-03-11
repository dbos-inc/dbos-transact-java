package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.client.ClientService;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface DisruptiveService {

  String dbLossBetweenSteps();

  String runChildWf();

  String wfPart1();

  String wfPart2(String id1);
}

class DisruptiveServiceImpl implements DisruptiveService {

  private DisruptiveService self;
  private final DataSource dataSource;
  private final DBOS.Instance dbos;

  public DisruptiveServiceImpl(DBOS.Instance dbos, DataSource dataSource) {
    this.dbos = dbos;
    this.dataSource = dataSource;
  }

  public void setSelf(DisruptiveService service) {
    this.self = service;
  }

  @Override
  @Workflow()
  public String dbLossBetweenSteps() {
    dbos.runStep(() -> "A", "A");
    dbos.runStep(() -> "B", "B");
    causeChaos(dataSource);
    dbos.runStep(() -> "C", "C");
    dbos.runStep(() -> "D", "D");
    return "Hehehe";
  }

  @Override
  @Workflow()
  public String runChildWf() {
    causeChaos(dataSource);
    var wfh = dbos.startWorkflow(() -> self.dbLossBetweenSteps());
    causeChaos(dataSource);
    return wfh.getResult();
  }

  @Override
  @Workflow()
  public String wfPart1() {
    causeChaos(dataSource);
    var r = (String) dbos.recv("topic", Duration.ofSeconds(5));
    causeChaos(dataSource);
    dbos.setEvent("key", "v1");
    causeChaos(dataSource);
    return "Part1" + r;
  }

  @Override
  @Workflow()
  public String wfPart2(String id1) {
    causeChaos(dataSource);
    dbos.send(id1, "hello1", "topic");
    causeChaos(dataSource);
    var v1 = (String) dbos.getEvent(id1, "key", Duration.ofSeconds(5));
    causeChaos(dataSource);
    return "Part2" + v1;
  }

  static void causeChaos(DataSource ds) {
    try (var conn = ds.getConnection();
        var st = conn.createStatement()) {

      st.execute(
          """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND datname = current_database();
        """);
    } catch (SQLException e) {
      throw new RuntimeException("Could not cause chaos, credentials insufficient?", e);
    }
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SystemDatabaseTest {

  final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  SystemDatabase sysdb;

  DBOS.Instance dbos;
  HikariDataSource dataSource;
  ClientService service;

  @BeforeEach
  void beforeEach() {
    pgContainer.start();

    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @AfterEach
  void afterEach() {
    dataSource.close();
    sysdb.close();
    pgContainer.stop();
  }

  @Test
  public void testDeleteWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(5, rows.size());

    sysdb.deleteWorkflows("wfid-1", "wfid-3");

    rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(3, rows.size());

    assertTrue(rows.stream().noneMatch(r -> r.workflowId().equals("wfid-1")));
    assertTrue(rows.stream().noneMatch(r -> r.workflowId().equals("wfid-3")));

    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-0")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-2")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-4")));
  }

  @Test
  public void testGetChildWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    for (var i = 0; i < 5; i++) {
      var parentWfId = "wfid-2";
      var wfid = "childwfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
      sysdb.recordChildWorkflow(
          parentWfId, wfid, i, "step-%d".formatted(i), System.currentTimeMillis());
    }

    for (var i = 0; i < 5; i++) {
      var parentWfId = "childwfid-%d".formatted(i);
      var wfid = "grandchildwfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
      sysdb.recordChildWorkflow(
          parentWfId, wfid, i, "step-%d".formatted(i), System.currentTimeMillis());
    }

    var children = sysdb.getWorkflowChildrenInternal("wfid-2");
    assertEquals(10, children.size());

    for (var i = 0; i < 5; i++) {
      var child = "childwfid-%d".formatted(i);
      var grandchild = "grandchildwfid-%d".formatted(i);
      assertTrue(children.stream().anyMatch(r -> r.equals(child)));
      assertTrue(children.stream().anyMatch(r -> r.equals(grandchild)));
    }
  }

  @Test
  public void testRetries() throws Exception {
    var wfid = "wfid-1";
    var status =
        WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING)
            .name("wf-name")
            .inputs("wf-inputs")
            .build();

    for (var i = 1; i <= 6; i++) {
      var result1 = sysdb.initWorkflowStatus(status, 5, true, false);
      assertEquals(WorkflowState.PENDING.toString(), result1.status());
      assertEquals(wfid, result1.workflowId());
      assertEquals(0, result1.deadlineEpochMS());

      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals("PENDING", row.status());
      assertEquals(i, row.recoveryAttempts());
    }

    assertThrows(
        DBOSMaxRecoveryAttemptsExceededException.class,
        () -> sysdb.initWorkflowStatus(status, 5, true, false));
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertNotNull(row);
    assertEquals("MAX_RECOVERY_ATTEMPTS_EXCEEDED", row.status());
    assertEquals(7, row.recoveryAttempts());
  }

  @Test
  public void testDedupeId() throws Exception {
    var wfid = "wfid-1";
    var builder =
        WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING)
            .name("wf-name")
            .inputs("wf-inputs")
            .queueName("queue-name")
            .deduplicationId("dedupe-id");

    var result1 = sysdb.initWorkflowStatus(builder.build(), 5, false, false);
    assertEquals(WorkflowState.PENDING.toString(), result1.status());
    assertEquals(wfid, result1.workflowId());
    assertEquals(0, result1.deadlineEpochMS());

    var before = DBUtils.getWorkflowRow(dataSource, wfid);
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () -> sysdb.initWorkflowStatus(builder.workflowId("wfid-2").build(), 5, false, false));
    var after = DBUtils.getWorkflowRow(dataSource, wfid);

    assertTrue(before.equals(after));
  }

  void logWorkflowDetails(String wfid, String name) throws Exception {
    var wfstat = DBOS.getWorkflowStatus(wfid);
    System.out.println(
        String.format("Workflow (%s) ID: %s. Status %s", name, wfid, wfstat.status()));
    var steps = DBOS.listWorkflowSteps(wfid);
    for (var step : steps) {
      System.out.println(
          String.format("  - # %d %s %s", step.functionId(), step.functionName(), step.output()));
    }
    var events = DBUtils.getWorkflowEvents(dataSource, wfid);
    for (var event : events) {
      System.out.println(String.format("  $ %s", event));
    }
  }

  // TODO: fix this test
  // @RepeatedTest(100)
  public void testSysDbWfDisruption() throws Exception {
    var dbos = new DBOS.Instance(dbosConfig);

    var impl = new DisruptiveServiceImpl(dbos, dataSource);
    var proxy = dbos.registerWorkflows(DisruptiveService.class, impl, UUID.randomUUID().toString());
    impl.setSelf(proxy);

    dbos.launch();
    DBOSTestAccess.getSystemDatabase(dbos).speedUpPollingForTest();
    try {
      assertEquals("Hehehe", proxy.dbLossBetweenSteps());

      assertEquals("Hehehe", proxy.runChildWf());

      var h1 = dbos.startWorkflow(() -> proxy.wfPart1());
      var h2 = dbos.startWorkflow(() -> proxy.wfPart2(h1.workflowId()));

      if (!"Part1hello1".equals(h1.getResult()) || !"Part2v1".equals(h2.getResult())) {
        logWorkflowDetails(h1.workflowId(), "Part 1 Details");
        logWorkflowDetails(h2.workflowId(), "Part 2 Details");
      }

      assertEquals("Part1hello1", h1.getResult());
      assertEquals("Part2v1", h2.getResult());
    } finally {
      dbos.shutdown();
    }
  }
}
