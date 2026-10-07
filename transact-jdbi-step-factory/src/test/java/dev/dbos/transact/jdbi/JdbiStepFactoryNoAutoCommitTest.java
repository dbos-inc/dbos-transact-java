package dev.dbos.transact.jdbi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.util.List;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JdbiStepFactory on a pool configured without autocommit. A delete that did not commit explicitly
 * would be rolled back when its connection went back to the pool.
 */
public class JdbiStepFactoryNoAutoCommitTest {
  private static final String SCHEMA = "jdbi_no_autocommit";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  @AutoClose HikariDataSource noAutoCommitDataSource;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    var cfg = new HikariConfig();
    cfg.setJdbcUrl(pgContainer.jdbcUrl());
    cfg.setUsername(pgContainer.username());
    cfg.setPassword(pgContainer.password());
    cfg.setAutoCommit(false);
    noAutoCommitDataSource = new HikariDataSource(cfg);

    dbos = new DBOS(pgContainer.dbosConfig());
    new JdbiStepFactory(dbos, Jdbi.create(noAutoCommitDataSource), SCHEMA);
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
}
