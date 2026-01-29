package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SystemDatabaseTest {
  private static DBOSConfig config;
  private SystemDatabase sysdb;
  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    config =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(config);
    MigrationManager.runMigrations(config);
    sysdb = SystemDatabase.create(config);
    dataSource =
        SystemDatabase.createDataSource(config.databaseUrl(), config.dbUser(), config.dbPassword());
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    sysdb.close();
  }

  private static final Logger logger = LoggerFactory.getLogger(SystemDatabaseTest.class);

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

  // @RepeatedTest(100)
  public void testSysDbWfDisruption() throws Exception {
    var dsvci = new DisruptiveServiceImpl();
    dsvci.setDS(dataSource);
    var dsvc = DBOS.registerWorkflows(DisruptiveService.class, dsvci, UUID.randomUUID().toString());
    dsvci.setSelf(dsvc);
    DBOS.launch();
    DBOSTestAccess.getSystemDatabase().speedUpPollingForTest();
    try {
      assertEquals("Hehehe", dsvc.dbLossBetweenSteps());

      assertEquals("Hehehe", dsvc.runChildWf());

      var h1 = DBOS.startWorkflow(() -> dsvc.wfPart1());
      var h2 = DBOS.startWorkflow(() -> dsvc.wfPart2(h1.workflowId()));

      if (!"Part1hello1".equals(h1.getResult()) || !"Part2v1".equals(h2.getResult())) {
        logWorkflowDetails(h1.workflowId(), "Part 1 Details");
        logWorkflowDetails(h2.workflowId(), "Part 2 Details");
      }

      assertEquals("Part1hello1", h1.getResult());
      assertEquals("Part2v1", h2.getResult());
    } finally {
      DBOS.shutdown();
    }
  }
}
