package dev.dbos.transact.jdbi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.SQLException;
import java.util.List;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface NoAutoCommitService {
  void insertWorkflow(String name);

  void serializableInsertWorkflow(String name);

  void failingWorkflow(String name);
}

class NoAutoCommitServiceImpl implements NoAutoCommitService {
  private final JdbiStepFactory stepFactory;

  NoAutoCommitServiceImpl(JdbiStepFactory stepFactory) {
    this.stepFactory = stepFactory;
  }

  @Override
  @Workflow
  public void insertWorkflow(String name) {
    stepFactory.useStep(
        h -> h.createUpdate("INSERT INTO names(name) VALUES (?)").bind(0, name).execute(),
        "insert");
  }

  @Override
  @Workflow
  public void serializableInsertWorkflow(String name) {
    stepFactory.useStep(
        h -> h.createUpdate("INSERT INTO names(name) VALUES (?)").bind(0, name).execute(),
        new JdbiStepOptions("insert", TransactionIsolationLevel.SERIALIZABLE));
  }

  @Override
  @Workflow
  public void failingWorkflow(String name) {
    stepFactory.useStep(
        h -> {
          h.createUpdate("INSERT INTO names(name) VALUES (?)").bind(0, name).execute();
          throw new IllegalStateException("step failed");
        },
        "fail");
  }
}

/**
 * JdbiStepFactory on a pool configured without autocommit. A step or delete that did not commit
 * explicitly would be rolled back when its connection went back to the pool.
 */
public class JdbiStepFactoryNoAutoCommitTest {
  private static final String SCHEMA = "jdbi_no_autocommit";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  @AutoClose HikariDataSource noAutoCommitDataSource;
  NoAutoCommitService proxy;

  @BeforeEach
  void beforeEach() throws SQLException {
    dataSource = pgContainer.dataSource();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE names(name text PRIMARY KEY)");
    }
    var cfg = new HikariConfig();
    cfg.setJdbcUrl(pgContainer.jdbcUrl());
    cfg.setUsername(pgContainer.username());
    cfg.setPassword(pgContainer.password());
    cfg.setAutoCommit(false);
    noAutoCommitDataSource = new HikariDataSource(cfg);

    dbos = new DBOS(pgContainer.dbosConfig());
    var factory = new JdbiStepFactory(dbos, Jdbi.create(noAutoCommitDataSource), SCHEMA);
    proxy = dbos.registerProxy(NoAutoCommitService.class, new NoAutoCommitServiceImpl(factory));
    dbos.launch();
  }

  private void insertCheckpoints(String workflowId, int... stepIds) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "INSERT INTO %s.tx_step_outputs (workflow_id, step_id) VALUES (?, ?)"
                    .formatted(SCHEMA))) {
      for (var stepId : stepIds) {
        stmt.setString(1, workflowId);
        stmt.setInt(2, stepId);
        stmt.executeUpdate();
      }
    }
  }

  private int checkpoints(String workflowId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM %s.tx_step_outputs WHERE workflow_id = ?"
                    .formatted(SCHEMA))) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  @Test
  public void deletesCommit() throws Exception {
    insertCheckpoints("wf-one", 0, 1, 2);
    insertCheckpoints("wf-a", 0);
    insertCheckpoints("wf-b", 0);
    insertCheckpoints("wf-keep", 0);
    var executor = DBOSTestAccess.getDbosExecutor(dbos);

    executor.deleteStepCheckpoints("wf-one", 1);
    assertEquals(1, checkpoints("wf-one"));

    executor.deleteStepCheckpoints(List.of("wf-a", "wf-b"));
    assertEquals(0, checkpoints("wf-a"));
    assertEquals(0, checkpoints("wf-b"));
    assertEquals(1, checkpoints("wf-keep"));
  }

  private String recordedError(String workflowId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT error FROM %s.tx_step_outputs WHERE workflow_id = ?".formatted(SCHEMA))) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  private boolean nameExists(String name) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement("SELECT 1 FROM names WHERE name = ?")) {
      stmt.setString(1, name);
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Test
  public void stepCommits() throws Exception {
    try (var _o = new WorkflowOptions("wf-insert").setContext()) {
      proxy.insertWorkflow("alice");
    }
    assertTrue(nameExists("alice"));
    assertEquals(1, checkpoints("wf-insert"));
    assertNull(recordedError("wf-insert"));
  }

  @Test
  public void stepWithIsolationLevelCommits() throws Exception {
    try (var _o = new WorkflowOptions("wf-serializable").setContext()) {
      proxy.serializableInsertWorkflow("bob");
    }
    assertTrue(nameExists("bob"));
    assertEquals(1, checkpoints("wf-serializable"));
  }

  @Test
  public void recordedErrorCommits() throws Exception {
    try (var _o = new WorkflowOptions("wf-fail").setContext()) {
      assertThrows(IllegalStateException.class, () -> proxy.failingWorkflow("carol"));
    }
    assertFalse(nameExists("carol"));
    assertEquals(1, checkpoints("wf-fail"));
    assertNotNull(recordedError("wf-fail"));
  }
}
