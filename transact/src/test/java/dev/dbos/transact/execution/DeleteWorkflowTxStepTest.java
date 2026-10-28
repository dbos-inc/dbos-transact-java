package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.txstep.JdbcStepFactory;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface DeleteTxStepService {
  int greet(String user) throws Exception;

  int greetThenWait(String user) throws Exception;
}

class DeleteTxStepServiceImpl implements DeleteTxStepService {
  private final DBOS dbos;
  private final JdbcStepFactory stepFactory;
  final CountDownLatch greeted = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);

  DeleteTxStepServiceImpl(DBOS dbos, JdbcStepFactory stepFactory) {
    this.dbos = dbos;
    this.stepFactory = stepFactory;
  }

  private static int insertGreeting(Connection conn, String user) throws SQLException {
    var sql =
        """
        INSERT INTO greetings(name, greet_count) VALUES (?, 1)
        ON CONFLICT(name) DO UPDATE SET greet_count = greetings.greet_count + 1
        RETURNING greet_count
        """;
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, user);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  @Override
  @Workflow
  public int greet(String user) throws Exception {
    return stepFactory.txStep((Connection c) -> insertGreeting(c, user), "insertGreeting");
  }

  @Override
  @Workflow
  public int greetThenWait(String user) throws Exception {
    int count = stepFactory.txStep((Connection c) -> insertGreeting(c, user), "insertGreeting");
    greeted.countDown();
    release.await();
    dbos.runStep(() -> 0, "afterWait");
    return count;
  }
}

// Unlike JdbcStepFactoryTest, finished workflows clear their own checkpoints here, as they do in
// production, so the only checkpoints left are those of a workflow cancelled after a
// transactional step committed.
class DeleteWorkflowTxStepTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  DeleteTxStepService proxy;
  DeleteTxStepServiceImpl impl;

  @BeforeEach
  void beforeEach() throws SQLException {
    pgContainer.createDatabase();
    dataSource = pgContainer.dataSource();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS greetings");
      stmt.execute("DROP TABLE IF EXISTS dbos.tx_step_outputs");
      stmt.execute("CREATE TABLE greetings(name text PRIMARY KEY, greet_count integer DEFAULT 0)");
    }
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new DeleteTxStepServiceImpl(dbos, new JdbcStepFactory(dbos, dataSource));
    proxy = dbos.registerProxy(DeleteTxStepService.class, impl);
    dbos.launch();
  }

  // Commits one transactional step, then cancels the workflow before it finishes.
  private void cancelAfterTxStep(String workflowId) throws Exception {
    var handle =
        dbos.startWorkflow(
            () -> proxy.greetThenWait("testUser"), new StartWorkflowOptions(workflowId));
    impl.greeted.await();
    dbos.cancelWorkflow(workflowId);
    impl.release.countDown();
    assertThrows(Exception.class, handle::getResult);
    assertEquals(1, DBUtils.getTxStepRows(dataSource, workflowId).size());
  }

  private int rerun(String workflowId) throws Exception {
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      return proxy.greet("testUser");
    }
  }

  @Test
  void deletedCancelledWorkflowRunsTxStepAgain() throws Exception {
    cancelAfterTxStep("wf1");

    dbos.deleteWorkflow("wf1");
    assertEquals(0, DBUtils.getTxStepRows(dataSource, "wf1").size());
    assertEquals(2, rerun("wf1"));
  }
}
