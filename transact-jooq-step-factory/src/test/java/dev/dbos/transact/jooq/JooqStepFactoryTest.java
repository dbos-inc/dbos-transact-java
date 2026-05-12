package dev.dbos.transact.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.sql.SQLException;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface FactoryTestService {
  record TestResult(String user, int greetCount) {}

  TestResult insertWorkflow(String user);

  TestResult errorWorkflow(String user);

  TestResult readWorkflow(String user);

  TestResult insertThenReadWorkflow(String user);
}

class FactoryTestServiceImpl implements FactoryTestService {

  private final JooqStepFactory stepFactory;

  public FactoryTestServiceImpl(JooqStepFactory stepFactory) {
    this.stepFactory = stepFactory;
  }

  FactoryTestService.TestResult insertGreeting(DSLContext ctx, String user) {
    var sql =
        """
        INSERT INTO greetings(name, greet_count)
        VALUES (?, 1)
        ON CONFLICT(name)
        DO UPDATE SET greet_count = greetings.greet_count + 1
        RETURNING greet_count
        """;
    var record = ctx.fetchOne(sql, Objects.requireNonNull(user));
    int greetCount = record != null ? record.get("greet_count", Integer.class) : 0;
    return new FactoryTestService.TestResult(user, greetCount);
  }

  FactoryTestService.TestResult errorGreeting(DSLContext ctx, String user) {
    insertGreeting(ctx, user);
    throw new RuntimeException("Test Exception %d".formatted(System.currentTimeMillis()));
  }

  FactoryTestService.TestResult readGreeting(DSLContext ctx, String user) {
    var sql = "SELECT greet_count FROM greetings WHERE name = ?";
    var record = ctx.fetchOne(sql, Objects.requireNonNull(user));
    int greetCount = record != null ? record.get("greet_count", Integer.class) : 0;
    return new FactoryTestService.TestResult(user, greetCount);
  }

  @Override
  @Workflow
  public FactoryTestService.TestResult insertWorkflow(String user) {
    return stepFactory.txStepResult(ctx -> insertGreeting(ctx.dsl(), user), "insertGreeting");
  }

  @Override
  @Workflow
  public FactoryTestService.TestResult errorWorkflow(String user) {
    return stepFactory.txStepResult(ctx -> errorGreeting(ctx.dsl(), user), "errorGreeting");
  }

  @Override
  @Workflow
  public FactoryTestService.TestResult readWorkflow(String user) {
    return stepFactory.txStepResult(ctx -> readGreeting(ctx.dsl(), user), "readGreeting");
  }

  @Override
  @Workflow
  public FactoryTestService.TestResult insertThenReadWorkflow(String user) {
    stepFactory.txStep(ctx -> insertGreeting(ctx.dsl(), user), "insertGreeting");
    return stepFactory.txStepResult(ctx -> readGreeting(ctx.dsl(), user), "readGreeting");
  }
}

public class JooqStepFactoryTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  JooqStepFactory stepFactory;
  FactoryTestService proxy;
  FactoryTestServiceImpl impl;

  @BeforeEach
  void beforeEach() throws SQLException {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE greetings(name text NOT NULL, greet_count integer DEFAULT 0, PRIMARY KEY(name))");
    }

    dbos = new DBOS(dbosConfig);
    DSLContext dsl = DSL.using(dataSource, SQLDialect.POSTGRES);
    stepFactory = new JooqStepFactory(dbos, dsl);

    impl = new FactoryTestServiceImpl(stepFactory);
    proxy = dbos.registerProxy(FactoryTestService.class, impl);

    dbos.launch();
  }

  private int getGreetCount(String user) throws SQLException {
    var sql = "SELECT greet_count FROM greetings WHERE name = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, user);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getInt("greet_count") : 0;
      }
    }
  }

  @Test
  public void testInsert() throws Exception {
    var wfid = "wf1";
    var user = "testUser";
    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }

    var rows = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(wfid, row.workflowId());
    assertEquals(0, row.stepId());
    assertNotNull(row.output());
    assertNull(row.error());
    assertEquals(SerializationUtil.NATIVE, row.serialization());
    var output = SerializationUtil.deserializeValue(row.output(), row.serialization(), null);
    assertEquals(new FactoryTestService.TestResult(user, 1), output);

    assertEquals(1, getGreetCount(user));
  }

  @Test
  public void testError() throws Exception {
    var wfid = "wf1";
    var user = "testUser";
    try (var _o = new WorkflowOptions(wfid).setContext()) {
      assertThrows(RuntimeException.class, () -> proxy.errorWorkflow(user));
    }

    // Transaction rolled back — no greeting inserted
    var rows = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(wfid, row.workflowId());
    assertEquals(0, row.stepId());
    assertNull(row.output());
    assertNotNull(row.error());

    assertEquals(0, getGreetCount(user));
  }

  @Test
  public void testRead() throws Exception {
    var insertWfid = "wf1";
    var readWfid = "wf2";
    var user = "testUser";

    try (var _o = new WorkflowOptions(insertWfid).setContext()) {
      proxy.insertWorkflow(user);
    }

    try (var _o = new WorkflowOptions(readWfid).setContext()) {
      var result = proxy.readWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }

    var rows = DBUtils.getTxStepRows(dataSource, readWfid);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(readWfid, row.workflowId());
    assertEquals(0, row.stepId());
    assertNotNull(row.output());
    assertNull(row.error());
    assertEquals(SerializationUtil.NATIVE, row.serialization());
    var output = SerializationUtil.deserializeValue(row.output(), row.serialization(), null);
    assertEquals(new FactoryTestService.TestResult(user, 1), output);

    assertEquals(1, getGreetCount(user));
  }

  @Test
  public void testIdempotency() throws Exception {
    var wfid = "wf1";
    var user = "testUser";

    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }

    // Second call with same wfid — txStep output is cached, insert not re-executed
    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }

    assertEquals(1, getGreetCount(user));
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid).size());
  }

  @Test
  public void testRetryError() throws Exception {
    var wfid = "wf1";
    var user = "testUser";

    try (var _o = new WorkflowOptions(wfid).setContext()) {
      assertThrows(RuntimeException.class, () -> proxy.errorWorkflow(user));
    }
    assertEquals(0, getGreetCount(user));
    dbos.close();

    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement("DELETE FROM dbos.operation_outputs WHERE workflow_uuid = ?")) {
      stmt.setString(1, wfid);
      stmt.executeUpdate();
    }
    DBUtils.setWorkflowState(dataSource, wfid, "PENDING");

    assertEquals(0, DBUtils.getStepRows(dataSource, wfid).size());
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid).size());

    dbos.launch();
    WorkflowHandle<FactoryTestService.TestResult, RuntimeException> handle =
        dbos.retrieveWorkflow(wfid);
    assertThrows(RuntimeException.class, handle::getResult);

    // Cached error replayed — insert still not committed
    assertEquals(0, getGreetCount(user));
    var txSteps = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(1, txSteps.size());
    assertNull(txSteps.get(0).output());
    assertNotNull(txSteps.get(0).error());
  }

  @Test
  public void testMultipleTxSteps() throws Exception {
    var wfid = "wf1";
    var user = "testUser";

    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertThenReadWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }

    assertEquals(1, getGreetCount(user));

    var rows = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(2, rows.size());
    assertEquals(0, rows.get(0).stepId());
    assertNotNull(rows.get(0).output());
    assertNull(rows.get(0).error());
    assertEquals(1, rows.get(1).stepId());
    assertNotNull(rows.get(1).output());
    assertNull(rows.get(1).error());
  }

  @Test
  public void testDistinctWorkflows() throws Exception {
    var wfid1 = "wf1";
    var wfid2 = "wf2";
    var user = "testUser";

    try (var _o = new WorkflowOptions(wfid1).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(1, result.greetCount());
    }

    try (var _o = new WorkflowOptions(wfid2).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(2, result.greetCount());
    }

    assertEquals(2, getGreetCount(user));
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid1).size());
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid2).size());
  }

  @Test
  public void testRetryPartialMultipleSteps() throws Exception {
    var wfid = "wf1";
    var user = "testUser";

    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertThenReadWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }
    assertEquals(1, getGreetCount(user));
    dbos.close();

    // Simulate crash after step 0 wrote tx_step_outputs but before step 1 ran:
    // both operation_outputs rows are gone, and step 1 has no tx_step_outputs entry
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement("DELETE FROM dbos.operation_outputs WHERE workflow_uuid = ?")) {
      stmt.setString(1, wfid);
      stmt.executeUpdate();
    }
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "DELETE FROM dbos.tx_step_outputs WHERE workflow_id = ? AND step_id = 1")) {
      stmt.setString(1, wfid);
      stmt.executeUpdate();
    }
    DBUtils.setWorkflowState(dataSource, wfid, "PENDING");

    assertEquals(0, DBUtils.getStepRows(dataSource, wfid).size());
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid).size());

    var relaunchTimestamp = System.currentTimeMillis();
    dbos.launch();
    WorkflowHandle<FactoryTestService.TestResult, RuntimeException> handle =
        dbos.retrieveWorkflow(wfid);
    var result = (FactoryTestService.TestResult) handle.getResult();
    assertEquals(1, result.greetCount());
    assertEquals(user, result.user());

    // Step 0 cache hit — insert not re-executed
    assertEquals(1, getGreetCount(user));

    var txSteps = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(2, txSteps.size());
    assertTrue(txSteps.get(0).createdAt() < relaunchTimestamp); // step 0: original run
    assertTrue(txSteps.get(1).createdAt() >= relaunchTimestamp); // step 1: re-executed on retry
  }

  @Test
  public void testRetryInsert() throws Exception {
    var timestamp = System.currentTimeMillis();

    var wfid = "wf1";
    var user = "testUser";
    try (var _o = new WorkflowOptions(wfid).setContext()) {
      var result = proxy.insertWorkflow(user);
      assertEquals(1, result.greetCount());
      assertEquals(user, result.user());
    }
    dbos.close();

    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement("DELETE FROM dbos.operation_outputs WHERE workflow_uuid = ?")) {
      stmt.setString(1, wfid);
      stmt.executeUpdate();
    }
    DBUtils.setWorkflowState(dataSource, wfid, "PENDING");

    assertEquals(0, DBUtils.getStepRows(dataSource, wfid).size());
    assertEquals(1, DBUtils.getTxStepRows(dataSource, wfid).size());

    var relaunchTimestamp = System.currentTimeMillis();
    dbos.launch();
    var handle = dbos.retrieveWorkflow(wfid);
    var result = (FactoryTestService.TestResult) handle.getResult();
    assertEquals(1, result.greetCount());
    assertEquals(user, result.user());

    var steps = DBUtils.getStepRows(dataSource, wfid);
    var txSteps = DBUtils.getTxStepRows(dataSource, wfid);
    assertEquals(1, steps.size());
    assertEquals(1, txSteps.size());

    var step = steps.get(0);
    var txStep = txSteps.get(0);
    assertEquals(step.output(), txStep.output());
    assertEquals(step.error(), txStep.error());

    assertTrue(txStep.createdAt() < step.startedAt());
    assertTrue(timestamp < txStep.createdAt());
    assertTrue(txStep.createdAt() < relaunchTimestamp);
    assertTrue(relaunchTimestamp < step.startedAt());

    // Retry reads from tx_step_outputs cache — insert not re-executed
    assertEquals(1, getGreetCount(user));
  }
}
