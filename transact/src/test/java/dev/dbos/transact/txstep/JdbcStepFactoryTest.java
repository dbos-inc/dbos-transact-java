package dev.dbos.transact.txstep;

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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface FactoryTestService {
  record TestResult(String user, int greetCount) {}

  TestResult insertWorkflow(String user) throws SQLException;

  TestResult errorWorkflow(String user) throws SQLException;

  TestResult readWorkflow(String user) throws SQLException;

  TestResult insertThenReadWorkflow(String user) throws SQLException;
}

class FactoryTestServiceImpl implements FactoryTestService {

  private final JdbcStepFactory stepFactory;

  public FactoryTestServiceImpl(JdbcStepFactory stepFactory) {
    this.stepFactory = stepFactory;
  }

  TestResult insertGreeting(Connection conn, String user) throws SQLException {
    var sql =
        """
        INSERT INTO greetings(name, greet_count)
        VALUES (?, 1)
        ON CONFLICT(name)
        DO UPDATE SET greet_count = greetings.greet_count + 1
        RETURNING greet_count
        """;

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(user));
      try (var rs = stmt.executeQuery()) {
        var greetCount = rs.next() ? rs.getInt("greet_count") : 0;
        return new TestResult(user, greetCount);
      }
    }
  }

  TestResult errorGreeting(Connection conn, String user) throws SQLException {
    insertGreeting(conn, user);
    throw new RuntimeException("Test Exception %d".formatted(System.currentTimeMillis()));
  }

  TestResult readGreeting(Connection conn, String user) throws SQLException {
    var sql =
        """
        SELECT greet_count
        FROM greetings
        WHERE name = ?
        """;
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(user));
      try (var rs = stmt.executeQuery()) {
        var greetCount = rs.next() ? rs.getInt("greet_count") : 0;
        return new TestResult(user, greetCount);
      }
    }
  }

  @Override
  @Workflow
  public TestResult insertWorkflow(String user) throws SQLException {
    return stepFactory.txStep((Connection c) -> insertGreeting(c, user), "insertGreeting");
  }

  @Override
  @Workflow
  public TestResult errorWorkflow(String user) throws SQLException {
    return stepFactory.txStep((Connection c) -> errorGreeting(c, user), "errorGreeting");
  }

  @Override
  @Workflow
  public TestResult readWorkflow(String user) throws SQLException {
    return stepFactory.txStep((Connection c) -> readGreeting(c, user), "readGreeting");
  }

  @Override
  @Workflow
  public TestResult insertThenReadWorkflow(String user) throws SQLException {
    stepFactory.txStep((Connection c) -> insertGreeting(c, user), "insertGreeting");
    return stepFactory.txStep((Connection c) -> readGreeting(c, user), "readGreeting");
  }
}

public class JdbcStepFactoryTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  JdbcStepFactory stepFactory;
  FactoryTestService proxy;
  FactoryTestServiceImpl impl;

  @BeforeEach
  void beforeEach() throws SQLException {

    pgContainer.createDatabase();

    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS greetings");
      stmt.execute("DROP TABLE IF EXISTS dbos.tx_step_outputs");
      stmt.execute(
          "CREATE TABLE greetings(name text NOT NULL, greet_count integer DEFAULT 0, PRIMARY KEY(name))");
    }

    dbos = new DBOS(dbosConfig);
    stepFactory = new JdbcStepFactory(dbos, dataSource);

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
